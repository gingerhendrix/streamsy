/* oxlint-disable effecttsgo/async-function -- External filesystem conformance and just-bash Promise harness. */
import { expect, test } from "vitest";
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

    for (const command of [
      "ls /docs/root | grep a",
      "cat /docs/root/a.txt | grep needle",
      "grep needle /docs/root/a.txt",
      "grep -rn needle /docs/root",
      "find /docs/root -name '*.txt' | xargs grep -l needle",
      "grep --search needle -r needle /docs/root",
    ]) {
      const ordinary = await bash.exec(command);
      expect(ordinary.exitCode).toBe(0);
      expect(ordinary.stderr).toBe("");
      expect(ordinary.stdout).not.toBe("");
    }

    const discardedError = await bash.exec("cat /docs/root/missing 2>/dev/null; echo status $?");
    expect(discardedError.stdout).toBe("status 1\n");
    expect(discardedError.stderr).toBe("");
    expect((await bash.exec("ls /docs/root > /dev/null && echo ok")).stdout).toBe("ok\n");
    expect((await bash.exec("cat /dev/null")).stdout).toBe("");
    expect((await bash.exec("echo scratch > /tmp/x; cat /tmp/x")).stdout).toBe("scratch\n");
    expect((await bash.exec("printf '%s\\n' /tmp/* /docs/root/*.txt /dev/std*")).stdout).toBe(
      "/tmp/x\n/docs/root/a.txt\n/docs/root/b.txt\n/dev/stderr\n/dev/stdin\n/dev/stdout\n",
    );
    expect((await bash.exec("cat /dev/stdin")).exitCode).toBe(0);
    expect((await bash.exec("touch /docs/root/touched.txt")).exitCode).toBe(0);
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
    expect(rows.at(-1).operation.base).toBe(rows[0].version);
    expect(rows.at(-1).operation.overwrote).toBe(rows.at(-2).version);
  } finally {
    await host.stop();
  }
});

test("session receipts and reads expose unseen external edits in overwrite history", async () => {
  const host = agentHost();

  try {
    await create(host, "docs");

    const session = await createAgentFileSystem({
      server: "http://localhost",
      fetch: host.fetch,
      folders: ["docs"],
      writable: ["docs"],
    });

    await session.bash.exec("echo agent-one > /docs/a.txt");

    const initial = await (
      await host.fetch(new Request("http://localhost/api/folders/docs/history/a.txt"))
    ).json();

    const initialVersion = initial.at(-1).version;

    const external = await host.fetch(
      new Request("http://localhost/api/folders/docs/files/a.txt", {
        method: "PUT",
        headers: {
          "x-file-op-id": "human-one",
          "x-file-author": "human",
          "x-file-origin": "human",
          "x-file-base": "1",
        },
        body: "human edit",
      }),
    );

    const human = await external.json();
    // Fresh metadata and version precondition checks do not advance the observation.
    const current = await session.tools.stat("/docs/a.txt", {});
    expect(current.ok && current.stat.exists && current.stat.version).toBe(String(human.version));
    await session.fs.writeFile("/docs/a.txt", "agent-two\n");

    const history = await host.fetch(
      new Request("http://localhost/api/folders/docs/history/a.txt"),
    );

    const rows = await history.json();
    expect(rows.at(-1).operation.base).toBe(initialVersion);
    expect(rows.at(-1).operation.overwrote).toBe(human.version);
    await session.core.read("/docs/a.txt");
    const readVersion = rows.at(-1).version;
    await session.core.mutate("/docs/a.txt", encoded("after read"));

    const after = await (
      await host.fetch(new Request("http://localhost/api/folders/docs/history/a.txt"))
    ).json();

    expect(after.at(-1).operation.base).toBe(readVersion);
    expect(after.at(-1).operation.overwrote).toBeUndefined();
    await session.core.mutate("/docs/a.txt", null);
    await session.core.mutate("/docs/a.txt", encoded("after tombstone"));

    const recreated = await (
      await host.fetch(new Request("http://localhost/api/folders/docs/history/a.txt"))
    ).json();

    expect(recreated.at(-1).operation.base).toBe(recreated.at(-2).version);
    expect(recreated.at(-1).operation.overwrote).toBeUndefined();
  } finally {
    await host.stop();
  }
});

test("append and same-path copy retain the version of their read across metadata races", async () => {
  for (const operation of ["append", "copy"]) {
    const host = agentHost();
    let inject = false;
    let readReturned = false;

    const transport = async (request: Request) => {
      if (inject && readReturned && new URL(request.url).pathname.includes("/history/")) {
        inject = false;
        await host.fetch(
          new Request("http://localhost/api/folders/docs/files/a", {
            method: "PUT",
            headers: {
              "x-file-op-id": "between-read-and-write",
              "x-file-author": "human",
              "x-file-origin": "human",
            },
            body: "unseen",
          }),
        );
      }

      const response = await host.fetch(request);

      if (inject && request.method === "GET" && new URL(request.url).pathname.includes("/files/"))
        readReturned = true;

      return response;
    };

    try {
      await create(host, "docs");

      const session = await createAgentFileSystem({
        server: "http://localhost",
        fetch: transport,
        folders: ["docs"],
        writable: ["docs"],
      });

      await session.core.mutate("/docs/a", encoded("read bytes"));
      inject = true;

      if (operation === "append") await session.fs.appendFile("/docs/a", " appended");
      else await session.fs.cp("/docs/a", "/docs/a");

      const history = await (
        await host.fetch(new Request("http://localhost/api/folders/docs/history/a"))
      ).json();

      expect(history.at(-1).operation.base).toBe(history[0].version);
      expect(history.at(-1).operation.overwrote).toBe(history.at(-2).version);
      expect(new TextDecoder().decode((await session.core.read("/docs/a")).bytes)).toBe(
        operation === "append" ? "read bytes appended" : "read bytes",
      );
    } finally {
      await host.stop();
    }
  }
});

test("shell mounts discard null writes and isolate scratch across sessions", async () => {
  const host = agentHost();

  try {
    const options = { server: "http://localhost", fetch: host.fetch, folders: [] };
    const first = await createAgentFileSystem(options);
    await first.fs.writeFile("/dev/null", "discard");
    await first.fs.appendFile("/dev/null", "discard too");
    expect(await first.fs.readFile("/dev/null")).toBe("");
    expect(await first.fs.readdir("/dev")).toEqual([
      "fd",
      "null",
      "stderr",
      "stdin",
      "stdout",
      "zero",
    ]);
    await first.fs.writeFile("/tmp/x", "scratch");
    expect(first.fs.getAllPaths()).toContain("/tmp/x");
    expect(first.fs.getAllPaths()).toContain("/dev/null");
    expect(first.fs.resolvePath("/tmp", "../dev/null")).toBe("/dev/null");
    const second = await createAgentFileSystem(options);
    expect(await second.fs.exists("/tmp/x")).toBe(false);
    expect((await second.bash.exec("ls /dev > /dev/null; echo ok")).stdout).toBe("ok\n");
  } finally {
    await host.stop();
  }
});
