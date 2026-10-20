/* oxlint-disable effecttsgo/async-function -- Native Promise filesystem transport boundary. */
import { expect, test } from "vitest";
import { FolderHttpFileSystem } from "../../agent/http.ts";

const response = (rows: object[]) =>
  new Response(JSON.stringify(rows), {
    headers: {
      "content-type": "application/json",
      "stream-next-offset": "0000000000000001_0000000000000000",
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

test("stream reset and 410 clear the local folder map and replay from -1", async () => {
  let stage = 0;
  const offsets: string[] = [];
  const hash = "a".repeat(64);

  const fs = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    fetch: async (request) => {
      offsets.push(new URL(request.url).searchParams.get("offset") ?? "");

      if (stage++ === 0)
        return response([
          {
            type: "file",
            key: "a",
            value: { path: "a", version: 1, hash, size: 1, author: "test", text: "a" },
            headers: { operation: "upsert", txid: "one" },
          },
        ]);

      if (stage === 2) return new Response(null, { status: 410 });

      if (stage === 3) return response([{ headers: { control: "reset" } }]);

      return response([]);
    },
  });

  expect(await fs.list("/docs")).toEqual([]);
  expect(offsets.slice(1, 4)).toEqual(["0000000000000001_0000000000000000", "-1", "-1"]);
});
