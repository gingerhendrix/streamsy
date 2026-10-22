/* oxlint-disable effecttsgo/async-function -- External filesystem conformance and just-bash Promise harness. */
import { Array } from "effect";
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

test("Bun agent passes Better FS Tools read and write conformance without server CAS", async () => {
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

    expect(Array.filter(read.checks, (check) => !check.ok)).toEqual([]);

    const write = await runWritableFileSystemConformance(tools, {
      scratchDirectory: "/docs/scratch",
      directoryPath: "/docs/scratch",
    });

    expect(Array.filter(write.checks, (check) => !check.ok)).toEqual([]);
    expect(tools.writeCapabilities).toEqual({
      atomic: true,
      compareAndSwap: false,
      preserveMode: false,
    });
  } finally {
    await host.stop();
  }
});

/** The JSON bodies of the file writes that a session sent, oldest first. */
const writeBodies = (requests: ReadonlyArray<Request>, method: "PUT" | "DELETE") =>
  Promise.all(
    Array.map(
      Array.filter(
        requests,
        (request) => request.method === method && request.url.includes("/files/"),
      ),
      (request) => request.clone().json(),
    ),
  );

test("blind and stale writes land with their parents; explicit preconditions are checked first", async () => {
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

    // A write to a path that the session never read has parent null. It lands.
    expect((await b.core.mutate("/docs/a", encoded("blind"))).version).toBe("2");
    expect(
      Array.every(
        requests,
        (request) => !request.headers.has("if-match") && !request.headers.has("if-none-match"),
      ),
    ).toBe(true);
    await b.core.read("/docs/a");
    await a.core.mutate("/docs/a", encoded("two"));
    // A write from an older read lands too, and becomes the head.
    expect((await b.core.mutate("/docs/a", encoded("stale"))).version).toBe("4");
    expect(new TextDecoder().decode((await a.core.read("/docs/a")).bytes)).toBe("stale");
    await b.core.mutate("/docs/a", encoded("three"));
    expect(Array.map(await writeBodies(requests, "PUT"), (body) => body.parent)).toEqual([
      null,
      2,
      4,
    ]);
    // An explicit precondition is checked against a fresh snapshot, before any request.
    const sent = requests.length;
    await expect(
      b.core.mutate("/docs/a", encoded("guarded"), {
        precondition: { kind: "version", version: "1" },
      }),
    ).rejects.toMatchObject({ code: "ESTALE" });
    await expect(
      b.core.mutate("/docs/a", encoded("guarded"), { precondition: { kind: "absent" } }),
    ).rejects.toMatchObject({ code: "EEXIST" });
    expect(Array.filter(requests.slice(sent), (request) => request.method !== "GET")).toHaveLength(
      0,
    );
    expect(
      Array.some(
        requests,
        (request) =>
          new URL(request.url).pathname === "/api/ls" || request.url.includes("/history/"),
      ),
    ).toBe(false);
    await b.core.mutate("/docs/a", null);
    expect(await writeBodies(requests, "DELETE")).toMatchObject([{ parent: 5 }]);
    expect(await a.core.stat("/docs/a")).toBeUndefined();
    await a.core.read("/docs/a").catch(() => undefined);
    await a.core.mutate("/docs/a", encoded("recreated"), { precondition: { kind: "absent" } });
    // The session that deleted the path names its tombstone, 6, when it writes the path again.
    // Session a made version 7 from no file, so history shows two creates after one delete.
    await b.core.mutate("/docs/a", encoded("again"));
    expect((await writeBodies(requests, "PUT")).at(-1)).toMatchObject({ parent: 6 });
    expect(new TextDecoder().decode((await a.core.read("/docs/a")).bytes)).toBe("again");
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
    const writes = Array.filter(requests, (request) => request.method === "PUT");
    expect(new URL(writes[0]?.url ?? "http://localhost").pathname).toMatch(/^\/api\/blobs\//);
    expect(writes[1]?.headers.has("if-none-match")).toBe(false);
    const reference = await writes[1]?.json();
    expect(reference.parent).toBeNull();
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

test("a listed unread file can be deleted; a delete from a stale listing lands and forks", async () => {
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
    expect(await writeBodies(requests, "DELETE")).toMatchObject([{ parent: 1 }]);
    expect(await writer.core.stat("/docs/delete.txt")).toBeUndefined();

    // The writer edits version 2 to version 4. The agent still names version 2 from its listing.
    await writer.core.mutate("/docs/stale.txt", encoded("newer edit"));
    await agent.core.mutate("/docs/stale.txt", null);
    expect((await writeBodies(requests, "DELETE")).at(-1)).toMatchObject({ parent: 2 });
    expect(await writer.core.stat("/docs/stale.txt")).toBeUndefined();
    expect(
      Array.every(
        requests,
        (request) => !request.headers.has("if-match") && !request.headers.has("if-none-match"),
      ),
    ).toBe(true);
    expect(Array.some(requests, (request) => request.url.includes("/api/blobs/"))).toBe(false);
  } finally {
    await host.stop();
  }
});

test("a shell write into a read-only folder fails one command, and the script goes on", async () => {
  const host = protocolHost();

  try {
    await create(host, "docs");
    await create(host, "ro");

    const writer = await createAgentFileSystem({
      ...options(host),
      folders: ["ro"],
      writable: ["ro"],
    });

    await writer.core.mutate("/ro/kept.txt", encoded("kept\n"));

    const { bash } = await createAgentFileSystem({ ...options(host), folders: ["docs", "ro"] });

    const result = await bash.exec(
      'echo before; echo x > /ro/new.txt; echo "after rc=$?"; ' +
        "echo x >> /ro/kept.txt || echo handled; " +
        'for i in 1 2; do echo $i > /ro/kept.txt; echo "loop $i"; done; ' +
        "echo ok > /docs/ok.txt; cat /docs/ok.txt",
    );

    expect(result.stdout).toBe("before\nafter rc=1\nhandled\nloop 1\nloop 2\nok\n");
    expect(result.stderr).toBe(
      "bash: /ro/new.txt: Read-only file system\n" +
        "bash: /ro/kept.txt: Read-only file system\n".repeat(3),
    );
    expect(result.exitCode).toBe(0);
    // The refused writes change nothing in the read-only folder.
    expect(await writer.core.stat("/ro/new.txt")).toBeUndefined();
    expect(new TextDecoder().decode((await writer.core.read("/ro/kept.txt")).bytes)).toBe("kept\n");
  } finally {
    await host.stop();
  }
});
