/* oxlint-disable effecttsgo/async-function -- Native HTTP protocol boundary tests. */
import { Schema } from "effect";
import { test, expect } from "vitest";
import { FILE_LIMIT } from "../server/domain.ts";
import { protocolHost, writerToken } from "./protocol-host.ts";

const digest = (text: string) => new Bun.CryptoHasher("sha256").update(text).digest("hex");

const call = (
  host: ReturnType<typeof protocolHost>,
  path: string,
  method = "GET",
  body?: Schema.Json,
  headers: Record<string, string> = {},
) => {
  const init: RequestInit = {
    method,
    headers: {
      authorization: `Bearer ${writerToken}`,
      "content-type": "application/json",
      ...headers,
    },
  };

  if (body !== undefined) init.body = Schema.is(Schema.String)(body) ? body : JSON.stringify(body);

  return host.fetch(new Request(`http://localhost${path}`, init));
};

/** A commit body names its content by hash and size. The content is a blob. */
const commit = (text: string, txid: string, parent: number | null = null) => ({
  txid,
  parent,
  author: "Zoë",
  hash: digest(text),
  size: new TextEncoder().encode(text).length,
});

/** Upload each text as a blob, so that a later commit can name it. */
const upload = async (host: ReturnType<typeof protocolHost>, ...texts: Array<string>) => {
  for (const text of texts)
    expect((await call(host, `/api/blobs/${digest(text)}`, "PUT", text)).status).toBe(200);
};

test("a storage failure answers 500 and preserves the previous head and stream", async () => {
  let failStorage = false;
  const host = protocolHost({ failStorage: () => failStorage });

  try {
    await call(host, "/api/folders", "POST", { txid: "folder", folderId: "docs", title: "Docs" });
    await upload(host, "one", "two");
    const file = "/api/folders/docs/files/a";
    await call(host, file, "PUT", commit("one", "first"));
    const stream = "/streams/folders/646f6373/files?offset=-1";
    const before = await (await call(host, stream)).json();
    failStorage = true;
    expect((await call(host, file, "PUT", commit("two", "failed", 1))).status).toBe(500);
    expect((await call(host, file)).status).toBe(500);
    failStorage = false;
    const read = await call(host, file);
    expect(await read.text()).toBe("one");
    expect(read.headers.get("etag")).toBe('"1"');
    expect(await (await call(host, stream)).json()).toEqual(before);
    const next = await call(host, file, "PUT", commit("two", "second", 1));
    expect(next.status).toBe(200);
    expect(next.headers.get("etag")).toBe('"2"');
  } finally {
    await host.stop();
  }
});

test("JSON writes, ETags, parents, retries, tombstones and State stream reads", async () => {
  const host = protocolHost();

  try {
    const created = { txid: "create", folderId: "docs/雪", title: "Docs" };
    expect((await call(host, "/api/folders", "POST", created)).status).toBe(200);
    expect((await call(host, "/api/folders", "POST", created)).status).toBe(200);
    expect(
      (await call(host, "/api/folders", "POST", { ...created, title: "Different" })).status,
    ).toBe(409);
    await upload(host, "one", "two", "three");
    const url = "/api/folders/docs%2F%E9%9B%AA/files/a";
    const first = await call(host, url, "PUT", commit("one", "first"));
    expect(first.status).toBe(200);
    expect(first.headers.get("etag")).toBe('"1"');
    const firstHead = await first.json();
    expect(firstHead).toEqual({ version: 1, parent: null, hash: digest("one"), txid: "first" });
    const noop = await call(host, url, "PUT", commit("one", "other"));
    expect(noop.status).toBe(200);
    expect(await noop.json()).toEqual(firstHead);

    // A second write that saw no file lands too. The last write is the head.
    const fork = await call(host, url, "PUT", commit("two", "second"));
    expect(fork.status).toBe(200);
    expect(await fork.json()).toEqual({
      version: 2,
      parent: null,
      hash: digest("two"),
      txid: "second",
    });
    // A late retry of the first write gets its first answer and does not hide the second.
    const retry = await call(host, url, "PUT", commit("one", "first"));
    expect(retry.status).toBe(200);
    expect(await retry.json()).toEqual(firstHead);
    const read = await call(host, url);
    expect(await read.text()).toBe("two");
    expect(read.headers.get("etag")).toBe('"2"');

    // Preconditions are not part of the protocol.
    for (const [method, body, headers] of [
      ["PUT", commit("three", "match", 2), { "if-match": '"2"' }],
      ["PUT", commit("three", "absent"), { "if-none-match": "*" }],
      ["DELETE", { txid: "bad", author: "test", parent: 2 }, { "if-match": '"2"' }],
    ] as const) {
      const refused = await call(host, url, method, body, headers);
      expect(refused.status).toBe(400);
      expect(await refused.json()).toEqual({
        error: "Preconditions are not supported. Send the parent version in the body.",
      });
    }

    const deleted = await call(host, url, "DELETE", { txid: "delete", author: "test", parent: 2 });
    expect(deleted.status).toBe(200);
    expect(await deleted.json()).toEqual({ version: 3, parent: 2, txid: "delete" });
    const gone = await call(host, url);
    expect(gone.status).toBe(404);
    expect(await gone.json()).toEqual({ error: "File not found", deleted: 3 });

    const noopDelete = await call(host, url, "DELETE", {
      txid: "noop-delete",
      author: "test",
      parent: 3,
    });

    expect(noopDelete.status).toBe(200);
    expect(await noopDelete.json()).toEqual({});
    const third = await call(host, url, "PUT", commit("three", "third", 3));
    expect(third.headers.get("etag")).toBe('"4"');
    // A parent must name a version of this folder.
    expect((await call(host, url, "PUT", commit("one", "future", 99))).status).toBe(400);
    const stream = "/streams/folders/646f63732fe99baa/files";
    const changes = await (await call(host, `${stream}?offset=-1`)).json();
    expect(changes.map((row: { headers: { operation: string } }) => row.headers.operation)).toEqual(
      ["upsert", "upsert", "upsert", "upsert"],
    );
    expect(
      changes.map((row: { value: { version: number; parent: number | null } }) => [
        row.value.version,
        row.value.parent,
      ]),
    ).toEqual([
      [1, null],
      [2, null],
      [3, 2],
      [4, 3],
    ]);
    expect(changes[2].value).toEqual({
      path: "a",
      version: 3,
      parent: 2,
      author: "test",
      deleted: true,
    });
    expect(changes[0].headers.txid).toBe("first");

    for (const target of ["/streams/folders", stream])
      for (const method of ["PUT", "POST", "DELETE"])
        expect((await call(host, target, method, "[]")).status).toBe(405);
    expect((await call(host, "/api/folders/docs/history/a")).status).toBe(404);
    expect((await call(host, url, "PUT", { ...commit("one", "bad"), hash: "wrong" })).status).toBe(
      400,
    );
    expect((await call(host, url, "PUT", { ...commit("one", "inline"), text: "one" })).status).toBe(
      200,
    );
    expect(
      (await (await call(host, `${stream}?offset=-1`)).json()).some(
        (row: { value?: object }) => row.value !== undefined && "text" in row.value,
      ),
    ).toBe(false);
    expect(
      (await call(host, url, "PUT", { ...commit("x", "oversize"), size: FILE_LIMIT + 1 })).status,
    ).toBe(413);
  } finally {
    await host.stop();
  }
});

test("blob routes verify hashes, sizes and existence; bytes use immutable reads", async () => {
  const host = protocolHost();

  try {
    await call(host, "/api/folders", "POST", { txid: "folder", folderId: "docs", title: "Docs" });
    const hash = digest("binary\0");
    const blob = `/api/blobs/${hash}`;
    const reference = { txid: "blob", author: "test", hash, size: 7, parent: null };
    const file = "/api/folders/docs/files/binary";
    const missing = await call(host, file, "PUT", reference);
    expect(missing.status).toBe(409);
    expect(await missing.json()).toEqual({ error: "blob-missing", hash });
    expect((await call(host, blob, "HEAD")).status).toBe(404);
    expect((await call(host, blob, "PUT", "wrong")).status).toBe(400);
    expect((await call(host, blob, "HEAD")).status).toBe(404);
    expect((await call(host, blob, "PUT", "binary\0")).status).toBe(200);
    const head = await call(host, blob, "HEAD");
    expect(head.status).toBe(200);
    expect(head.headers.get("content-length")).toBe("7");
    expect((await call(host, blob, "PUT", "ignored repeat")).status).toBe(200);
    const bytes = await call(host, blob);
    expect(await bytes.text()).toBe("binary\0");
    expect(bytes.headers.get("cache-control")).toContain("immutable");
    expect((await call(host, file, "PUT", { ...reference, size: 8 })).status).toBe(409);
    expect((await call(host, file, "PUT", reference)).status).toBe(200);
    const content = await call(host, file);
    expect(await content.text()).toBe("binary\0");
    expect(content.headers.get("etag")).toBe('"1"');
    expect(
      (
        await call(host, `/api/blobs/${digest("new")}`, "PUT", "", {
          "content-length": String(FILE_LIMIT + 1),
        })
      ).status,
    ).toBe(413);

    for (const size of [0, 5, 64 * 1024 + 1]) {
      const text = "y".repeat(size);
      const smallHash = digest(text);
      const small = `/api/folders/docs/files/small-${size}`;
      expect((await call(host, `/api/blobs/${smallHash}`, "PUT", text)).status).toBe(200);
      expect(
        (
          await call(host, small, "PUT", {
            hash: smallHash,
            size,
            txid: `small-blob-${size}`,
            author: "test",
            parent: null,
          })
        ).status,
      ).toBe(200);
      expect(await (await call(host, small)).text()).toBe(text);
    }
  } finally {
    await host.stop();
  }
});

test("JSON writes have no body size limit", async () => {
  const host = protocolHost();

  try {
    await call(host, "/api/folders", "POST", { txid: "folder", folderId: "docs", title: "Docs" });
    await upload(host, "one");
    // Whitespace pads a valid commit body past the 512 KiB limit that the server once had.
    const padded = `${JSON.stringify(commit("one", "padded"))}${" ".repeat(1024 * 1024)}`;
    const response = await call(host, "/api/folders/docs/files/a", "PUT", padded);

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ version: 1 });
  } finally {
    await host.stop();
  }
});

test("every write requires the writer token; reads stay public", async () => {
  const host = protocolHost();

  try {
    for (const authorization of ["", "Bearer wrong"]) {
      const response = await call(
        host,
        "/api/folders",
        "POST",
        { txid: "auth", folderId: "docs", title: "Docs" },
        { authorization },
      );

      expect(response.status).toBe(401);
    }

    expect(
      (await call(host, "/streams/folders?offset=-1", "GET", undefined, { authorization: "" }))
        .status,
    ).toBe(200);
  } finally {
    await host.stop();
  }
});

test("every JSON write needs application/json", async () => {
  const host = protocolHost();

  try {
    await call(host, "/api/folders", "POST", { txid: "folder", folderId: "docs", title: "Docs" });
    await upload(host, "one");
    const file = "/api/folders/docs/files/a";
    expect((await call(host, file, "PUT", commit("one", "first"))).status).toBe(200);

    for (const [path, method, body] of [
      ["/api/folders", "POST", { txid: "again", folderId: "docs", title: "Docs" }],
      [file, "PUT", commit("two", "second")],
      [file, "DELETE", { txid: "delete", author: "Zoë", parent: 1 }],
    ] as const) {
      const response = await call(host, path, method, body, { "content-type": "text/plain" });

      expect(response.status).toBe(415);
      expect(await response.json()).toEqual({ error: "Expected application/json" });
    }

    expect(await (await call(host, file)).text()).toBe("one");
  } finally {
    await host.stop();
  }
});
