/* oxlint-disable typescript/await-thenable -- Bun Promise rejection matchers execute asynchronously despite their void declarations. */
/* oxlint-disable effecttsgo/async-function -- External filesystem conformance and just-bash Promise harness. */
import { expect, test } from "bun:test";
import { runFileSystemConformance, runWritableFileSystemConformance } from "@better-fs-tools/fs";
import { createAgentFileSystem } from "../../agent/index.ts";
import { agentHost } from "./host.ts";

const encoded = (text: string) => new TextEncoder().encode(text);

const create = (host: ReturnType<typeof agentHost>, id: string) =>
  host.fetch(
    new Request("http://localhost/api/folders", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ folderId: id, title: id }),
    }),
  );

test("Bun HTTP adapter passes both Better FS Tools conformance suites", async () => {
  const host = agentHost();

  try {
    expect((await create(host, "daily/demo")).status).toBe(200);

    const { core, tools } = await createAgentFileSystem({
      server: "http://localhost",
      fetch: host.fetch,
      folders: ["daily/demo"],
      writable: ["daily/demo"],
    });

    await core.mutate("/daily/demo/read.txt", encoded("fixture\n"));
    await core.mkdir("/daily/demo/scratch");

    const read = await runFileSystemConformance(tools, {
      existingFile: { path: "/daily/demo/read.txt", bytes: encoded("fixture\n") },
      missingPath: "/daily/demo/missing.txt",
      directoryPath: "/daily/demo",
      listDirectory: "/daily/demo",
      refusedPath: "/.folder-sync/state",
      mutate: async () => {
        await core.mutate("/daily/demo/read.txt", encoded("changed\n"));
      },
    });

    expect(read.checks.filter((check) => !check.ok)).toEqual([]);
    expect(read.passed).toBe(true);

    const write = await runWritableFileSystemConformance(tools, {
      scratchDirectory: "/daily/demo/scratch",
      directoryPath: "/daily/demo/scratch",
    });

    expect(write.checks.filter((check) => !check.ok)).toEqual([]);
    expect(write.passed).toBe(true);
    expect(tools.writeCapabilities).toEqual({
      atomic: true,
      compareAndSwap: false,
      preserveMode: false,
    });
  } finally {
    await host.stop();
  }
});

test("scripted just-bash session lists, reads, candidate greps, writes and survives errors", async () => {
  const host = agentHost();
  const requests: Request[] = [];

  const transport = (request: Request) => {
    requests.push(request.clone());

    return host.fetch(request);
  };

  try {
    await create(host, "docs/root");
    await create(host, "reference");

    const { core, bash } = await createAgentFileSystem({
      server: "http://localhost",
      fetch: transport,
      folders: ["docs/root", "reference"],
      writable: ["docs/root"],
    });

    await core.mutate("/docs/root/a.txt", encoded("needle one\nother\n"));
    await core.mutate("/docs/root/b.txt", encoded("different\n"));

    // The index is intentionally asynchronous. Wait for its own response, not a sleep.
    for (let attempt = 0; attempt < 100 && (await core.search("needle")).length === 0; attempt++)
      await core.list("/docs/root");
    requests.length = 0;
    expect((await bash.exec("ls /docs/root")).stdout).toContain("a.txt");
    expect((await bash.exec("cat /docs/root/a.txt")).stdout).toBe("needle one\nother\n");
    requests.length = 0;
    const grep = await bash.exec("grep --search needle -n needle /docs/root");
    expect(grep.exitCode).toBe(0);
    expect(grep.stdout).toContain("1:needle one");

    const reads = requests.filter(
      (request) => request.method === "GET" && new URL(request.url).pathname.includes("/files/"),
    );

    expect(reads.map((request) => new URL(request.url).pathname)).toEqual([
      "/api/folders/docs%2Froot/files/a.txt",
    ]);
    expect(requests.some((request) => new URL(request.url).pathname === "/api/search")).toBe(true);
    expect((await bash.exec("grep --search needle absent /docs/root")).exitCode).toBe(1);
    expect((await bash.exec("grep --search '\"unterminated' needle /docs/root")).exitCode).toBe(2);
    const save = await bash.exec("echo 'agent wrote' > /docs/root/a.txt; cat /docs/root/a.txt");
    expect(save.exitCode).toBe(0);
    expect(save.stdout).toBe("agent wrote\n");
    const again = await bash.exec("echo 'second save' > /docs/root/a.txt; cat /docs/root/a.txt");
    expect(again.stdout).toBe("second save\n");

    const secondReader = await host.fetch(
      new Request("http://localhost/api/folders/docs%2Froot/files/a.txt"),
    );

    expect(await secondReader.text()).toBe("second save\n");

    const history = await host.fetch(
      new Request("http://localhost/api/folders/docs%2Froot/history/a.txt"),
    );

    const rows = await history.json();
    expect(rows.at(-1).operation.base).toBe(rows.at(-2).version);
    expect(rows.at(-1).operation.overwrote).toBeUndefined();
    await expect(bash.exec("echo denied > /reference/no.txt")).rejects.toMatchObject({
      code: "EROFS",
    });
    expect((await bash.exec("cat /docs/root/missing; cat /docs/root/a.txt")).stdout).toBe(
      "second save\n",
    );
  } finally {
    await host.stop();
  }
});

test("listing observes external edits and deletes while a session write waits for the manifest", async () => {
  const host = agentHost();

  try {
    await create(host, "docs");

    const session = await createAgentFileSystem({
      server: "http://localhost",
      fetch: host.fetch,
      folders: ["docs"],
      writable: ["docs"],
    });

    await session.core.mutate("/docs/a", encoded("original"));

    const deletion = await host.fetch(
      new Request("http://localhost/api/folders/docs/files/a", {
        method: "DELETE",
        headers: {
          "x-file-op-id": "external-delete",
          "x-file-author": "reader",
          "x-file-origin": "reader",
        },
      }),
    );

    expect(deletion.status).toBe(200);
    expect(await session.core.stat("/docs/a")).toBeUndefined();
    expect(await session.core.list("/docs")).toEqual([]);
    const recreate = await session.core.mutate("/docs/a", encoded("recreated"));
    expect(recreate.version).not.toBeNull();
    const history = await host.fetch(new Request("http://localhost/api/folders/docs/history/a"));
    const rows = await history.json();
    expect(rows.at(-1).operation.base).toBe(rows.at(-2).version);
  } finally {
    await host.stop();
  }
});
