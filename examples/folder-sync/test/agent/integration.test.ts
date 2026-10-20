/* oxlint-disable effecttsgo/async-function -- External filesystem conformance and just-bash Promise harness. */
import { expect, test } from "vitest";
import { runFileSystemConformance, runWritableFileSystemConformance } from "@better-fs-tools/fs";
import { createAgentFileSystem } from "../../agent/index.ts";
import { protocolHost, writerToken } from "../protocol-host.ts";

const encoded = (text: string) => new TextEncoder().encode(text);

const create = (host: ReturnType<typeof protocolHost>, id: string) =>
  host.fetch(
    new Request("http://localhost/api/folders", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${writerToken}` },
      body: JSON.stringify({ txid: `create:${id}`, folderId: id, title: id }),
    }),
  );

const options = (host: ReturnType<typeof protocolHost>) => ({
  server: "http://localhost",
  token: writerToken,
  fetch: host.fetch,
  folders: ["docs"],
  writable: ["docs"],
});

test("Bun agent passes Better FS Tools read and write conformance with atomic CAS", async () => {
  const host = protocolHost();

  try {
    expect((await create(host, "docs")).status).toBe(200);
    const { core, tools } = await createAgentFileSystem(options(host));
    await core.mutate("/docs/read.txt", encoded("fixture\n"));
    await core.mkdir("/docs/scratch");

    const read = await runFileSystemConformance(tools, {
      existingFile: { path: "/docs/read.txt", bytes: encoded("fixture\n") },
      missingPath: "/docs/missing.txt",
      directoryPath: "/docs",
      listDirectory: "/docs",
      refusedPath: "/.folder-sync/state",
      mutate: async () => {
        await core.mutate("/docs/read.txt", encoded("changed\n"));
      },
    });

    expect(read.checks.filter((check) => !check.ok)).toEqual([]);

    const write = await runWritableFileSystemConformance(tools, {
      scratchDirectory: "/docs/scratch",
      directoryPath: "/docs/scratch",
    });

    expect(write.checks.filter((check) => !check.ok)).toEqual([]);
    expect(tools.writeCapabilities).toEqual({
      atomic: true,
      compareAndSwap: true,
      preserveMode: false,
    });
  } finally {
    await host.stop();
  }
});

test("stale reads and commit races return ESTALE, while successful edits use If-Match", async () => {
  const host = protocolHost();
  const requests: Request[] = [];

  try {
    await create(host, "docs");
    const a = await createAgentFileSystem(options(host));
    await a.core.mutate("/docs/a", encoded("one"));

    const b = await createAgentFileSystem({
      ...options(host),
      fetch: (request) => {
        requests.push(request.clone());

        return host.fetch(request);
      },
    });

    await expect(b.core.mutate("/docs/a", encoded("blind"))).rejects.toMatchObject({
      code: "ESTALE",
    });
    const blind = requests.filter((request) => request.method === "PUT").at(-1);
    expect(blind?.headers.get("if-none-match")).toBe("*");
    expect(blind?.headers.get("if-match")).toBeNull();
    await b.core.read("/docs/a");
    await a.core.mutate("/docs/a", encoded("two"));
    await expect(b.core.mutate("/docs/a", encoded("stale"))).rejects.toMatchObject({
      code: "ESTALE",
    });
    const stale = requests.filter((request) => request.method === "PUT").at(-1);
    expect(stale?.headers.get("if-match")).toBe('"1"');
    expect(new TextDecoder().decode((await b.core.read("/docs/a")).bytes)).toBe("two");
    await b.core.mutate("/docs/a", encoded("three"));
    expect(
      requests
        .filter((request) => request.method === "PUT")
        .at(-1)
        ?.headers.get("if-match"),
    ).toBe('"2"');
    expect(
      requests.some(
        (request) =>
          new URL(request.url).pathname === "/api/ls" || request.url.includes("/history/"),
      ),
    ).toBe(false);
    await b.core.mutate("/docs/a", null);
    expect(await a.core.stat("/docs/a")).toBeUndefined();
    await a.core.read("/docs/a").catch(() => undefined);
    await a.core.mutate("/docs/a", encoded("recreated"), { precondition: { kind: "absent" } });
    expect(new TextDecoder().decode((await b.core.read("/docs/a")).bytes)).toBe("recreated");
  } finally {
    await host.stop();
  }
});

test("agent blob uploads precede reference commits and session shell commands stay usable", async () => {
  const host = protocolHost();
  const requests: Request[] = [];

  try {
    await create(host, "docs");

    const { core, bash } = await createAgentFileSystem({
      ...options(host),
      fetch: (request) => {
        requests.push(request.clone());

        return host.fetch(request);
      },
    });

    const large = encoded("x".repeat(65537));
    await core.mutate("/docs/large.txt", large);
    expect((await core.read("/docs/large.txt")).bytes).toEqual(large);
    await core.mutate("/docs/nul", new Uint8Array([0, 1, 2]));
    expect((await core.read("/docs/nul")).bytes).toEqual(new Uint8Array([0, 1, 2]));
    const writes = requests.filter((request) => request.method === "PUT");
    expect(new URL(writes[0]?.url ?? "http://localhost").pathname).toMatch(/^\/api\/blobs\//);
    expect(writes[1]?.headers.get("if-none-match")).toBe("*");
    const reference = await writes[1]?.json();
    expect(reference.text).toBeUndefined();
    expect(reference.size).toBe(65537);
    expect((await bash.exec("echo hello > /docs/hello.txt; cat /docs/hello.txt")).stdout).toBe(
      "hello\n",
    );
    expect((await bash.exec("ls /docs")).stdout).toContain("hello.txt");
    expect((await bash.exec("grep hello /docs/hello.txt")).stdout).toContain("hello");
    expect((await bash.exec("cat /docs/missing; cat /docs/hello.txt")).stdout).toBe("hello\n");
    expect((await bash.exec("cat /dev/null")).stdout).toBe("");
  } finally {
    await host.stop();
  }
});

test("listed unread files can be deleted with If-Match and stale listings return ESTALE", async () => {
  const host = protocolHost();
  const requests: Request[] = [];

  try {
    await create(host, "docs");
    const writer = await createAgentFileSystem(options(host));
    await writer.core.mutate("/docs/delete.txt", encoded("delete me"));
    await writer.core.mutate("/docs/stale.txt", encoded("keep the edit"));

    const agent = await createAgentFileSystem({
      ...options(host),
      fetch: (request) => {
        requests.push(request.clone());

        return host.fetch(request);
      },
    });

    expect(await agent.core.list("/docs")).toEqual([
      { name: "delete.txt", kind: "file" },
      { name: "stale.txt", kind: "file" },
    ]);
    await agent.core.mutate("/docs/delete.txt", null);
    const removed = requests.filter((request) => request.method === "DELETE").at(-1);
    expect(removed?.headers.get("if-match")).toBe('"1"');
    expect(removed?.headers.get("if-none-match")).toBeNull();
    expect(await writer.core.stat("/docs/delete.txt")).toBeUndefined();

    await writer.core.mutate("/docs/stale.txt", encoded("newer edit"));
    await expect(agent.core.mutate("/docs/stale.txt", null)).rejects.toMatchObject({
      code: "ESTALE",
    });
    const stale = requests.filter((request) => request.method === "DELETE").at(-1);
    expect(stale?.headers.get("if-match")).toBe('"2"');
    expect(new TextDecoder().decode((await writer.core.read("/docs/stale.txt")).bytes)).toBe(
      "newer edit",
    );
    expect(requests.some((request) => request.url.includes("/api/blobs/"))).toBe(false);
  } finally {
    await host.stop();
  }
});
