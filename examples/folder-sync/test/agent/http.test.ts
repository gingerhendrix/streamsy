/* oxlint-disable effecttsgo/async-function -- Promise filesystem HTTP boundary tests. */
import { expect, test } from "vitest";
import { betterFileSystem, FileSystemError, FolderHttpFileSystem } from "../../agent/index.ts";

const core = (transport: (request: Request) => Promise<Response>) =>
  FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    writable: ["docs"],
    fetch: transport,
  });

const empty = () => new Response("[]", { headers: { "content-type": "application/json" } });

test("HTTP status errors map to errno and tool reasons without ending a session", async () => {
  for (const [status, code, reason] of [
    [400, "EINVAL", "io"],
    [404, "ENOENT", "not-found"],
    [413, "EFBIG", "too-large"],
    [401, "EACCES", "permission-denied"],
    [403, "EACCES", "permission-denied"],
    [500, "EIO", "io"],
  ] as const) {
    let calls = 0;
    let failed = true;

    const fs = await core(async (request) => {
      if (new URL(request.url).pathname === "/api/ls") return empty();
      calls++;

      return failed
        ? new Response("failure", { status })
        : new Response("ok", { headers: { "x-file-version": "1" } });
    });

    await expect(fs.read("/docs/a")).rejects.toMatchObject({ code });
    const tools = betterFileSystem(fs);
    const outcome = await tools.open("/docs/a", {});
    expect(outcome.ok).toBe(false);

    if (!outcome.ok) expect(outcome.error.reason).toBe(reason);
    // ENOENT checks the type through history once as well. No status is retried here.
    expect(calls).toBe(status === 404 ? 4 : 2);
    failed = false;
    expect(new TextDecoder().decode((await fs.read("/docs/a")).bytes)).toBe("ok");
  }
});

test("index reads retry 503 and honour Retry-After: 1", async () => {
  let calls = 0;
  const started = performance.now();
  await core(async () =>
    ++calls === 1
      ? new Response("index unavailable", { status: 503, headers: { "retry-after": "1" } })
      : empty(),
  );
  expect(calls).toBe(2);
  expect(performance.now() - started).toBeGreaterThanOrEqual(950);
});

test("408, 409 and 429 retry with bounded backoff; exhausted 503 returns EIO", async () => {
  for (const status of [408, 409, 429, 503]) {
    let calls = 0;

    const fs = await core(async (request) => {
      if (new URL(request.url).pathname === "/api/ls") return empty();
      calls++;

      return new Response("unavailable", { status, headers: { "retry-after": "0" } });
    });

    await expect(fs.read("/docs/a")).rejects.toMatchObject({ code: "EIO" });
    expect(calls).toBe(4);
  }
});

test("folder length and mapping policy reject before any file request", async () => {
  for (const id of ["a".repeat(101), "雪".repeat(84)]) {
    await expect(
      FolderHttpFileSystem.create({
        server: "http://localhost",
        folders: [id],
        fetch: async () => {
          throw new Error("must not fetch");
        },
      }),
    ).rejects.toMatchObject({ code: "ENAMETOOLONG" });
  }

  for (const mapping of ["/outside", "../outside", ".folder-sync/a"]) {
    await expect(
      FolderHttpFileSystem.create({
        server: "http://localhost",
        folders: ["a"],
        folderDirectory: () => mapping,
        fetch: async () => empty(),
      }),
    ).rejects.toMatchObject({ code: "EINVAL" });
  }

  await expect(
    FolderHttpFileSystem.create({
      server: "http://localhost",
      folders: ["daily", "daily/demo"],
      fetch: async () => empty(),
    }),
  ).rejects.toMatchObject({ code: "EINVAL" });
});

test("read-only folders and synthetic paths give EROFS and read-only, with no write request", async () => {
  let writes = 0;

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["daily/demo"],
    fetch: async (request) => {
      if (request.method !== "GET") writes++;

      return empty();
    },
  });

  const tools = betterFileSystem(fs);

  for (const path of ["/daily/demo/a", "/daily/demo", "/daily", "/", "/unselected/a"]) {
    await expect(fs.mutate(path, new Uint8Array())).rejects.toBeInstanceOf(FileSystemError);
    await expect(fs.mutate(path, new Uint8Array())).rejects.toMatchObject({ code: "EROFS" });

    const outcome = await tools.write(path, new Uint8Array(), {
      precondition: { kind: "any" },
      createParents: true,
    });

    expect(outcome).toMatchObject({ ok: false, error: { reason: "read-only" } });
  }

  expect(writes).toBe(0);
});

test("mutation retries keep token, opId and base; receipt becomes the next base", async () => {
  const writes: Request[] = [];
  let version = 1;
  let attempt = 0;

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    writable: ["docs"],
    token: "test-token",
    fetch: async (request) => {
      const route = new URL(request.url).pathname;

      if (route === "/api/ls") return empty();

      if (route.includes("/history/"))
        return Response.json([{ version, operation: { type: "put", size: 3 } }]);
      writes.push(request.clone());

      if (++attempt === 1) return new Response("busy", { status: 409 });
      version++;

      return Response.json({ version });
    },
  });

  await fs.mutate("/docs/a", new TextEncoder().encode("one"));
  await fs.mutate("/docs/a", new TextEncoder().encode("two"));
  expect(writes).toHaveLength(3);
  expect(writes.map((request) => request.headers.get("x-file-base"))).toEqual(["1", "1", "2"]);
  expect(writes.map((request) => request.headers.get("authorization"))).toEqual([
    "Bearer test-token",
    "Bearer test-token",
    "Bearer test-token",
  ]);
  expect(writes[0]?.headers.get("x-file-op-id")).toBe(writes[1]?.headers.get("x-file-op-id"));
  expect(await writes[0]?.text()).toBe("one");
  expect(await writes[1]?.text()).toBe("one");
});

test("empty folders and mapped opaque ids retain synthetic parents", async () => {
  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["opaque/id"],
    writable: ["opaque/id"],
    folderDirectory: () => "mapped/notes",
    fetch: async () => empty(),
  });

  expect(await fs.list("/")).toEqual([{ name: "mapped", kind: "directory" }]);
  expect(await fs.list("/mapped")).toEqual([{ name: "notes", kind: "directory" }]);
  expect(await fs.list("/mapped/notes")).toEqual([]);
});

test("explicit selection excludes unselected writable folders and discovery uses ls", async () => {
  const excluded = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["selected"],
    writable: ["other"],
    fetch: async () => empty(),
  });

  expect(await excluded.list("/")).toEqual([{ name: "selected", kind: "directory" }]);
  await expect(excluded.mutate("/other/a", new Uint8Array())).rejects.toMatchObject({
    code: "EROFS",
  });

  const discovered = await FolderHttpFileSystem.create({
    server: "http://localhost",
    fetch: async () =>
      Response.json([
        {
          id: '["docs","a"]',
          folderId: "docs",
          path: "a",
          version: 1,
          hash: "0".repeat(64),
          size: 1,
          updatedAt: "2026-10-13T00:00:00Z",
          author: "agent",
        },
      ]),
  });

  expect(await discovered.list("/")).toEqual([{ name: "docs", kind: "directory" }]);
  expect(await discovered.list("/docs")).toEqual([{ name: "a", kind: "file" }]);
});

test("cancellation stops a pending Retry-After backoff", async () => {
  let attempts = 0;

  const fs = await core(async (request) => {
    if (new URL(request.url).pathname === "/api/ls") return empty();
    attempts++;

    return new Response("busy", { status: 503, headers: { "retry-after": "5" } });
  });

  const controller = new AbortController();
  const pending = fs.read("/docs/a", controller.signal);
  controller.abort();
  await expect(pending).rejects.toMatchObject({ code: "ECANCELED" });
  expect(attempts).toBeLessThanOrEqual(1);
});

const manifest = (folderId: string, path: string) => ({
  id: JSON.stringify([folderId, path]),
  folderId,
  path,
  version: 1,
  hash: "0".repeat(64),
  size: 1,
  updatedAt: "2026-10-13T00:00:00Z",
  author: "agent",
});

test("discovered overlaps warn once and leave listing, metadata, reads and search usable", async () => {
  const warnings: string[] = [];
  let overlap = false;

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    onWarning: (message) => warnings.push(message),
    fetch: async (request) => {
      const route = new URL(request.url).pathname;

      if (route === "/api/ls")
        return Response.json([
          manifest("docs", "a"),
          ...(overlap ? [manifest("docs/root", "b")] : []),
        ]);

      if (route === "/api/search")
        return Response.json([
          { folderId: "docs/root", path: "b", version: 1, snippet: [] },
          { folderId: "docs", path: "a", version: 1, snippet: [] },
        ]);

      if (route.includes("/history/"))
        return Response.json([{ version: 1, operation: { type: "put", size: 1 } }]);

      return new Response("x", { headers: { "x-file-version": "1" } });
    },
  });

  overlap = true;
  expect(await fs.list("/docs")).toEqual([{ name: "a", kind: "file" }]);
  expect((await fs.stat("/docs/a"))?.kind).toBe("file");
  expect(new TextDecoder().decode((await fs.read("/docs/a")).bytes)).toBe("x");
  expect((await fs.search("x")).map((hit) => hit.path)).toEqual(["/docs/a"]);
  expect(warnings).toHaveLength(1);
  expect(warnings[0]).toContain("Skipping discovered folder docs/root");
  const initialWarnings: string[] = [];

  const initial = await FolderHttpFileSystem.create({
    server: "http://localhost",
    onWarning: (message) => initialWarnings.push(message),
    fetch: async () => Response.json([manifest("docs", "a"), manifest("docs/root", "b")]),
  });

  expect(await initial.list("/docs")).toEqual([{ name: "a", kind: "file" }]);
  expect(initialWarnings).toHaveLength(1);
});

test("Retry-After above five seconds fails immediately for seconds and HTTP dates", async () => {
  // oxlint-disable-next-line effecttsgo/global-date -- Exercise HTTP-date parsing against the real adapter clock.
  for (const value of ["6", new Date(Date.now() + 60000).toUTCString()]) {
    let attempts = 0;
    const started = performance.now();

    const fs = await core(async (request) => {
      if (new URL(request.url).pathname === "/api/ls") return empty();
      attempts++;

      return new Response("busy", { status: 503, headers: { "retry-after": value } });
    });

    await expect(fs.read("/docs/a")).rejects.toMatchObject({ code: "EIO" });
    expect(attempts).toBe(1);
    expect(performance.now() - started).toBeLessThan(1000);
  }
});
