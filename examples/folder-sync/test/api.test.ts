/* oxlint-disable effecttsgo/async-function -- Native HTTP protocol boundary tests. */
import { Schema } from "effect";
import { test, expect } from "vitest";
import { FILE_LIMIT, INLINE_LIMIT } from "../server/domain.ts";
import { JSON_BODY_LIMIT } from "../server/api.ts";
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

const commit = (text: string, txid: string) => ({
  txid,
  author: "Zoë",
  hash: digest(text),
  size: new TextEncoder().encode(text).length,
  text,
});

test("a storage failure answers 500 and preserves the previous head and stream", async () => {
  let failStorage = false;
  const host = protocolHost({ failStorage: () => failStorage });

  try {
    await call(host, "/api/folders", "POST", { txid: "folder", folderId: "docs", title: "Docs" });
    const file = "/api/folders/docs/files/a";
    await call(host, file, "PUT", commit("one", "first"));
    const stream = "/streams/folders/646f6373/files?offset=-1";
    const before = await (await call(host, stream)).json();
    failStorage = true;
    expect(
      (await call(host, file, "PUT", commit("two", "failed"), { "if-match": '"1"' })).status,
    ).toBe(500);
    expect((await call(host, file)).status).toBe(500);
    failStorage = false;
    const read = await call(host, file);
    expect(await read.text()).toBe("one");
    expect(read.headers.get("etag")).toBe('"1"');
    expect(await (await call(host, stream)).json()).toEqual(before);
    const next = await call(host, file, "PUT", commit("two", "second"), { "if-match": '"1"' });
    expect(next.status).toBe(200);
    expect(next.headers.get("etag")).toBe('"2"');
  } finally {
    await host.stop();
  }
});

test("JSON writes, ETags, preconditions, no-op txids and State stream reads", async () => {
  const host = protocolHost();

  try {
    const created = { txid: "create", folderId: "docs/雪", title: "Docs" };
    expect((await call(host, "/api/folders", "POST", created)).status).toBe(200);
    expect((await call(host, "/api/folders", "POST", created)).status).toBe(200);
    expect(
      (await call(host, "/api/folders", "POST", { ...created, title: "Different" })).status,
    ).toBe(409);
    const url = "/api/folders/docs%2F%E9%9B%AA/files/a";
    const first = await call(host, url, "PUT", commit("one", "first"), { "if-none-match": "*" });
    expect(first.status).toBe(200);
    expect(first.headers.get("etag")).toBe('"1"');
    const firstHead = await first.json();
    const noop = await call(host, url, "PUT", commit("one", "other"), { "if-none-match": "*" });
    expect(noop.status).toBe(200);
    expect(await noop.json()).toEqual(firstHead);

    const staleCreate = await call(host, url, "PUT", commit("two", "second"), {
      "if-none-match": "*",
    });

    expect(staleCreate.status).toBe(412);
    expect(await staleCreate.json()).toEqual(firstHead);
    expect(
      (await call(host, url, "PUT", commit("two", "second"), { "if-match": '"1"' })).status,
    ).toBe(200);
    expect(
      (await call(host, url, "PUT", commit("one", "first"), { "if-match": '"1"' })).status,
    ).toBe(412);
    const read = await call(host, url);
    expect(await read.text()).toBe("two");
    expect(read.headers.get("etag")).toBe('"2"');
    expect(
      (await call(host, url, "DELETE", { txid: "bad", author: "test" }, { "if-none-match": "*" }))
        .status,
    ).toBe(400);
    expect(
      (await call(host, url, "DELETE", { txid: "delete", author: "test" }, { "if-match": '"2"' }))
        .status,
    ).toBe(200);
    expect((await call(host, url)).status).toBe(404);
    const noopDelete = await call(host, url, "DELETE", { txid: "noop-delete", author: "test" });
    expect(noopDelete.status).toBe(200);
    expect(await noopDelete.json()).toEqual({});
    const absent = await call(host, url, "PUT", commit("three", "third"), { "if-match": '"2"' });
    expect(absent.status).toBe(412);
    expect(await absent.json()).toEqual({ head: null });
    expect(
      (
        await call(host, url, "PUT", commit("three", "third"), { "if-none-match": "*" })
      ).headers.get("etag"),
    ).toBe('"3"');
    expect(
      (await call(host, url, "PUT", commit("one", "first"), { "if-match": '"1"' })).status,
    ).toBe(412);
    const stream = "/streams/folders/646f63732fe99baa/files";
    const changes = await (await call(host, `${stream}?offset=-1`)).json();
    expect(changes.map((row: { headers: { operation: string } }) => row.headers.operation)).toEqual(
      ["upsert", "upsert", "delete", "upsert"],
    );
    expect(changes.map((row: { value?: { version: number } }) => row.value?.version)).toEqual([
      1,
      2,
      undefined,
      3,
    ]);
    expect(changes[0].headers.txid).toBe("first");

    for (const target of ["/streams/folders", stream])
      for (const method of ["PUT", "POST", "DELETE"])
        expect((await call(host, target, method, "[]")).status).toBe(405);
    expect((await call(host, "/api/folders/docs/history/a")).status).toBe(404);
    expect(
      (await call(host, url, "PUT", { ...commit("bad", "bad"), hash: digest("wrong") })).status,
    ).toBe(400);
    expect((await call(host, url, "PUT", commit("null\0", "bad"))).status).toBe(400);
    expect(
      (await call(host, url, "PUT", commit("x".repeat(INLINE_LIMIT + 1), "large-inline"))).status,
    ).toBe(400);
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
    const reference = { txid: "blob", author: "test", hash, size: 7 };
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
    expect((await call(host, file, "PUT", commit("x".repeat(INLINE_LIMIT), "exact"))).status).toBe(
      200,
    );

    for (const size of [0, INLINE_LIMIT]) {
      const text = "x".repeat(size);
      const inlineHash = digest(text);
      expect((await call(host, `/api/blobs/${inlineHash}`, "PUT", text)).status).toBe(200);
      expect(
        (
          await call(host, file, "PUT", {
            hash: inlineHash,
            size,
            txid: "must-inline",
            author: "test",
          })
        ).status,
      ).toBe(400);
    }

    const largeText = "x".repeat(INLINE_LIMIT + 1);
    const largeHash = digest(largeText);
    await call(host, `/api/blobs/${largeHash}`, "PUT", largeText);
    expect(
      (
        await call(host, file, "PUT", {
          hash: largeHash,
          size: largeText.length,
          txid: "large-text",
          author: "test",
        })
      ).status,
    ).toBe(200);
  } finally {
    await host.stop();
  }
});

test("JSON writes reject oversized declared and streamed bodies before parsing", async () => {
  const host = protocolHost();

  try {
    await call(host, "/api/folders", "POST", { txid: "folder", folderId: "docs", title: "Docs" });

    for (const [path, method] of [
      ["/api/folders", "POST"],
      ["/api/folders/docs/files/a", "PUT"],
      ["/api/folders/docs/files/a", "DELETE"],
    ]) {
      const headers = {
        authorization: `Bearer ${writerToken}`,
        "content-type": "application/json",
      };

      const declared = await host.fetch(
        new Request(`http://localhost${path}`, {
          method,
          headers: { ...headers, "content-length": String(JSON_BODY_LIMIT + 1) },
          body: "not JSON",
        }),
      );

      expect(declared.status).toBe(413);
      let pulls = 0;

      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          pulls++;
          controller.enqueue(new Uint8Array(65536).fill(32));

          if (pulls === 32) controller.close();
        },
      });

      const streamed = await host.fetch(
        new Request(`http://localhost${path}`, { method, headers, body }),
      );

      expect(streamed.status).toBe(413);
      expect(pulls).toBeLessThan(32);
    }

    // Six-byte JSON escapes still fit for a full 64 KiB of inline UTF-8.
    const escaped = commit("\u0001".repeat(INLINE_LIMIT), "escaped");
    expect((await call(host, "/api/folders/docs/files/a", "PUT", escaped)).status).toBe(200);
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
