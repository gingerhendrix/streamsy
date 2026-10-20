/* oxlint-disable effecttsgo/async-function -- Native Promise filesystem transport boundary. */
import { expect, test } from "vitest";
import { FolderHttpFileSystem } from "../../agent/http.ts";

const response = (rows: object[], cursor = "0000000000000001_0000000000000000") =>
  new Response(JSON.stringify(rows), {
    headers: {
      "content-type": "application/json",
      "stream-next-offset": cursor,
      "stream-up-to-date": "true",
    },
  });

const empty = () =>
  new Response("[]", {
    headers: {
      "content-type": "application/json",
      "stream-next-offset": "0000000000000000_0000000000000000",
      "stream-up-to-date": "true",
    },
  });

test("folder length, mapping, overlap and read-only policies reject unsafe targets", async () => {
  for (const id of ["a".repeat(101), "雪".repeat(84)])
    await expect(
      FolderHttpFileSystem.create({
        server: "http://localhost",
        folders: [id],
        fetch: async () => {
          throw new Error("must not fetch");
        },
      }),
    ).rejects.toMatchObject({ code: "ENAMETOOLONG" });

  for (const mapped of ["/outside", "../outside", ".folder-sync/a"])
    await expect(
      FolderHttpFileSystem.create({
        server: "http://localhost",
        folders: ["docs"],
        folderDirectory: () => mapped,
        fetch: async () => empty(),
      }),
    ).rejects.toMatchObject({ code: "EINVAL" });
  await expect(
    FolderHttpFileSystem.create({
      server: "http://localhost",
      folders: ["docs", "docs/nested"],
      fetch: async () => empty(),
    }),
  ).rejects.toMatchObject({ code: "EINVAL" });

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["opaque/id"],
    folderDirectory: () => "mapped/notes",
    fetch: async () => empty(),
  });

  expect(await fs.list("/")).toEqual([{ name: "mapped", kind: "directory" }]);
  await expect(fs.mutate("/mapped/notes/a", new Uint8Array())).rejects.toMatchObject({
    code: "EROFS",
  });
});

test("bounded stream retries honour Retry-After and cancellation", async () => {
  let calls = 0;
  const start = performance.now();

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    fetch: async () =>
      ++calls === 1
        ? new Response("busy", { status: 503, headers: { "retry-after": "1" } })
        : empty(),
  });

  expect(calls).toBe(2);
  expect(performance.now() - start).toBeGreaterThanOrEqual(950);
  const controller = new AbortController();
  controller.abort();
  await expect(fs.read("/docs/a", controller.signal)).rejects.toMatchObject({ code: "ECANCELED" });
});

for (const status of [408, 409, 429]) {
  test(`stream reads retry ${status} and then recover`, async () => {
    let calls = 0;
    await FolderHttpFileSystem.create({
      server: "http://localhost",
      folders: ["docs"],
      fetch: async () => (++calls < 3 ? new Response("retry", { status }) : empty()),
    });
    expect(calls).toBe(3);
  });
}

test("exhausted 503 retries become EIO", async () => {
  let calls = 0;
  await expect(
    FolderHttpFileSystem.create({
      server: "http://localhost",
      folders: ["docs"],
      fetch: async () => {
        calls++;

        return new Response("busy", { status: 503 });
      },
    }),
  ).rejects.toMatchObject({ code: "EIO" });
  expect(calls).toBe(4);
});

test("Retry-After above five seconds fails without waiting or retrying", async () => {
  for (const retryAfter of ["6", "Thu, 01 Jan 2099 00:00:00 GMT"]) {
    let calls = 0;
    const start = performance.now();
    await expect(
      FolderHttpFileSystem.create({
        server: "http://localhost",
        folders: ["docs"],
        fetch: async () => {
          calls++;

          return new Response("busy", { status: 429, headers: { "retry-after": retryAfter } });
        },
      }),
    ).rejects.toMatchObject({ code: "EIO" });
    expect(calls).toBe(1);
    expect(performance.now() - start).toBeLessThan(1000);
  }
});

test("anonymous blob uploads and commits omit Authorization", async () => {
  const requests: Request[] = [];

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    writable: ["docs"],
    fetch: async (request) => {
      requests.push(request);

      if (request.method === "PUT")
        return Response.json(
          request.url.includes("/api/blobs/") ? {} : { version: 1, txid: "one" },
        );

      return empty();
    },
  });

  await fs.mutate("/docs/binary", new Uint8Array([0, 1]));
  const writes = requests.filter((request) => request.method === "PUT");
  expect(writes).toHaveLength(2);
  expect(writes.every((request) => !request.headers.has("authorization"))).toBe(true);
});

const fileChange = (path: string, version = 1) => ({
  type: "file",
  key: path,
  value: { path, version, hash: "a".repeat(64), size: 1, author: "test", text: path },
  headers: { operation: "upsert", txid: `file:${path}:${version}` },
});

const reset = { headers: { control: "reset" } };

const firstCursor = "0000000000000001_0000000000000000";

const nextCursor = "0000000000000002_0000000000000000";

test("a stored file reset clears earlier changes and advances to the following page", async () => {
  const offsets: string[] = [];

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    fetch: async (request) => {
      const offset = new URL(request.url).searchParams.get("offset") ?? "";
      offsets.push(offset);

      if (offset === "-1") return response([fileChange("old")]);

      if (offset === firstCursor)
        return response([fileChange("discard"), reset, fileChange("kept", 2)], nextCursor);

      return response([fileChange("later", 3)], "0000000000000003_0000000000000000");
    },
  });

  expect(await fs.list("/docs")).toEqual([{ name: "kept", kind: "file" }]);
  expect(await fs.list("/docs")).toEqual([
    { name: "kept", kind: "file" },
    { name: "later", kind: "file" },
  ]);
  expect(offsets).toEqual(["-1", firstCursor, nextCursor]);
});

test("410 clears the file map and reads a replacement from -1", async () => {
  const offsets: string[] = [];

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    fetch: async (request) => {
      offsets.push(new URL(request.url).searchParams.get("offset") ?? "");

      if (offsets.length === 1) return response([fileChange("old")]);

      if (offsets.length === 2) return new Response(null, { status: 410 });

      return response([reset, fileChange("replacement", 2)], nextCursor);
    },
  });

  expect(await fs.list("/docs")).toEqual([{ name: "replacement", kind: "file" }]);
  expect(offsets).toEqual(["-1", firstCursor, "-1"]);
});

const folder = (folderId: string) => ({
  type: "folder",
  key: folderId,
  value: { folderId, title: folderId },
  headers: { operation: "upsert", txid: folderId },
});

for (const expired of [false, true]) {
  test(`registry ${expired ? "410 replays" : "stored reset advances"} and clears old folders`, async () => {
    const offsets: string[] = [];

    const fs = await FolderHttpFileSystem.create({
      server: "http://localhost",
      fetch: async (request) => {
        const url = new URL(request.url);

        if (url.pathname !== "/streams/folders") return empty();
        offsets.push(url.searchParams.get("offset") ?? "");

        if (offsets.length === 1) return response([folder("old")]);

        if (expired && offsets.length === 2) return new Response(null, { status: 410 });

        return response([folder("discard"), reset, folder("new")], nextCursor);
      },
    });

    expect(await fs.list("/")).toEqual([{ name: "new", kind: "directory" }]);
    expect(await fs.list("/")).toEqual([{ name: "new", kind: "directory" }]);
    expect(offsets).toEqual(
      expired ? ["-1", firstCursor, "-1", nextCursor] : ["-1", firstCursor, nextCursor],
    );
  });
}

test("410 at the bootstrap offset fails instead of looping", async () => {
  for (const folders of [undefined, ["docs"]]) {
    let calls = 0;
    await expect(
      FolderHttpFileSystem.create({
        server: "http://localhost",
        folders,
        fetch: async () => {
          calls++;

          return new Response(null, { status: 410 });
        },
      }),
    ).rejects.toMatchObject({ code: "EIO" });
    expect(calls).toBe(1);
  }
});
