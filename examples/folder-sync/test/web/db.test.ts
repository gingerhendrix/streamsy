/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise -- These browser integration tests exercise native StreamDB promises and an in-process HTTP fetch boundary. */
import { expect, test } from "vitest";
import { createFileDb, createFolderDb, editFile } from "../../src/db.ts";
import { fileUrl, folderStreamUrl, history, readFile, textContent } from "../../src/client.ts";
import { protocolHost, writerToken } from "../protocol-host.ts";

const origin = "http://browser.test";

const authorization = { authorization: `Bearer ${writerToken}` };

const fixture = () => {
  const host = protocolHost();
  const calls: Request[] = [];

  const fetcher = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      calls.push(request.clone());

      return host.fetch(request);
    },
    { preconnect: globalThis.fetch.preconnect },
  );

  const options = { origin, fetch: fetcher, writerHeaders: () => authorization };

  const create = async (folderId: string, text: string) => {
    const folder = await fetcher(`${origin}/api/folders`, {
      method: "POST",
      headers: { ...authorization, "content-type": "application/json" },
      body: JSON.stringify({ folderId, title: "Docs", txid: "folder-created" }),
    });

    expect(folder.status).toBe(200);
    const content = await textContent(text);

    const file = await fetcher(new URL(fileUrl({ folderId, path: "guide/a.md" }), origin), {
      method: "PUT",
      headers: { ...authorization, "content-type": "application/json", "if-none-match": "*" },
      body: JSON.stringify({ txid: "first-write", author: "seed", ...content }),
    });

    expect(file.status).toBe(200);
  };

  return { host, calls, options, fetcher, create };
};

test("registry and file StreamDBs use opaque folder IDs; optimistic edits await the accepted txid", async () => {
  const f = fixture();
  const folderId = "docs/雪%";
  await f.create(folderId, "first");
  const registry = createFolderDb(f.options);
  let release!: () => void;
  let posted!: () => void;

  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });

  const started = new Promise<void>((resolve) => {
    posted = resolve;
  });

  const delayed = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);

      if (request.method === "PUT" && request.url.includes("/files/")) {
        posted();
        await gate;
      }

      return f.fetcher(request);
    },
    { preconnect: globalThis.fetch.preconnect },
  );

  const db = createFileDb(folderId, { ...f.options, fetch: delayed });

  try {
    await registry.preload();
    await db.preload();
    expect(registry.collections.folders.get(folderId)?.title).toBe("Docs");
    const before = db.collections.files.get("guide/a.md")!;
    expect(before.version).toBe(1);
    const content = await textContent("edited");

    const transaction = db.actions.edit({
      file: { ...before, ...content, author: "browser" },
      base: before.version,
      txid: "browser-edit",
      bytes: new TextEncoder().encode("edited"),
    });

    expect(db.collections.files.get(before.path)?.text).toBe("edited");
    expect(db.collections.files.get(before.path)?.version).toBe(1);
    await started;
    release();
    await transaction.isPersisted.promise;
    expect(db.collections.files.get(before.path)?.version).toBe(2);
    expect(db.collections.files.get(before.path)?.text).toBe("edited");

    const put = f.calls.find(
      (request) => request.method === "PUT" && request.headers.get("if-match") === '"1"',
    );

    expect(await put?.json()).toMatchObject({ txid: "browser-edit", text: "edited" });
    expect(f.calls.some((request) => request.url.includes(folderStreamUrl(folderId)))).toBe(true);
  } finally {
    release();
    registry.close();
    db.close();
    await f.host.stop();
  }
});

test("same-hash no-op waits for the response's original txid without appending", async () => {
  const f = fixture();
  await f.create("noop", "first");
  const db = createFileDb("noop", f.options);

  try {
    await db.preload();
    const before = db.collections.files.get("guide/a.md")!;
    await db.actions.edit({
      file: { ...before, author: "browser" },
      base: before.version,
      txid: "not-appended",
      bytes: new TextEncoder().encode("first"),
    }).isPersisted.promise;
    expect(db.collections.files.get(before.path)?.author).toBe("seed");
    expect(
      await history({ folderId: "noop", path: before.path }, undefined, f.options),
    ).toHaveLength(1);
  } finally {
    db.close();
    await f.host.stop();
  }
});

test("412 rolls back the optimistic row while preserving the newer server version", async () => {
  const f = fixture();
  await f.create("conflict", "first");
  const db = createFileDb("conflict", f.options);

  try {
    await db.preload();
    const before = db.collections.files.get("guide/a.md")!;
    const content = await textContent("other writer");

    const response = await f.fetcher(
      new URL(fileUrl({ folderId: "conflict", path: before.path }), origin),
      {
        method: "PUT",
        headers: { ...authorization, "content-type": "application/json", "if-match": '"1"' },
        body: JSON.stringify({ txid: "other-writer", author: "other", ...content }),
      },
    );

    expect(response.status).toBe(200);
    await db.utils.awaitTxId("other-writer");
    const edited = await textContent("my draft");

    const transaction = db.actions.edit({
      file: { ...before, ...edited, author: "browser" },
      base: 1,
      txid: "stale-edit",
      bytes: new TextEncoder().encode("my draft"),
    });

    expect(db.collections.files.get(before.path)?.text).toBe("my draft");
    await expect(transaction.isPersisted.promise).rejects.toThrow("The file changed on the server");
    expect(db.collections.files.get(before.path)?.text).toBe("other writer");
    expect(db.collections.files.get(before.path)?.version).toBe(2);
  } finally {
    db.close();
    await f.host.stop();
  }
});

test("the editor retains its exact accepted version when another write arrives before its response", async () => {
  const f = fixture();
  await f.create("racing", "first");
  let db: ReturnType<typeof createFileDb>;

  const raced = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const request = new Request(input, init);
      const response = await f.fetcher(request);

      if (request.method === "PUT" && request.url.includes("/files/")) {
        const content = await textContent("later writer");

        const later = await f.fetcher(request.url, {
          method: "PUT",
          headers: { ...authorization, "content-type": "application/json", "if-match": '"2"' },
          body: JSON.stringify({ txid: "later-writer", author: "other", ...content }),
        });

        expect(later.status).toBe(200);
        await db.utils.awaitTxId("later-writer");
      }

      return response;
    },
    { preconnect: globalThis.fetch.preconnect },
  );

  db = createFileDb("racing", { ...f.options, fetch: raced });

  try {
    await db.preload();
    const before = db.collections.files.get("guide/a.md")!;
    const accepted = await editFile(db, before, "my save", "browser", "my-save");
    expect(accepted.version).toBe(2);
    expect(db.collections.files.get(before.path)?.version).toBe(3);
    expect(db.collections.files.get(before.path)?.text).toBe("later writer");
  } finally {
    db.close();
    await f.host.stop();
  }
});

test("large browser edits upload blobs before JSON commits and read history from State", async () => {
  const f = fixture();
  await f.create("blob", "original");
  const db = createFileDb("blob", f.options);

  try {
    await db.preload();
    const before = db.collections.files.get("guide/a.md")!;
    const text = "x".repeat(64 * 1024 + 1);
    const content = await textContent(text);
    await db.actions.edit({
      file: { path: before.path, version: before.version, ...content, author: "browser" },
      base: 1,
      txid: "large",
      bytes: new TextEncoder().encode(text),
    }).isPersisted.promise;

    const uploadIndex = f.calls.findIndex(
      (request) => request.method === "PUT" && request.url.includes("/api/blobs/"),
    );

    const commitIndex = f.calls.findIndex((request) => request.headers.get("if-match") === '"1"');
    expect(uploadIndex).toBeGreaterThan(-1);
    expect(commitIndex).toBeGreaterThan(uploadIndex);
    expect(await f.calls[commitIndex]?.json()).not.toHaveProperty("text");
    const current = db.collections.files.get(before.path)!;
    expect(current).not.toHaveProperty("text");
    expect(
      (
        await readFile(
          { folderId: "blob", path: before.path },
          current,
          new AbortController().signal,
          f.options,
        )
      ).text,
    ).toBe(text);

    const historical = await readFile(
      { folderId: "blob", path: before.path, at: 1 },
      current,
      new AbortController().signal,
      f.options,
    );

    expect(historical.text).toBe("original");
    expect(
      f.calls.every(
        (request) => !request.url.includes("/history/") && !request.url.includes("?at="),
      ),
    ).toBe(true);
  } finally {
    db.close();
    await f.host.stop();
  }
});

test("history paginates, filters paths, and leaves deletes unversioned", async () => {
  const f = fixture();
  await f.create("history", "first");

  try {
    const deleted = await f.fetcher(
      `${origin}${fileUrl({ folderId: "history", path: "guide/a.md" })}`,
      {
        method: "DELETE",
        headers: { ...authorization, "content-type": "application/json", "if-match": '"1"' },
        body: JSON.stringify({ txid: "deleted", author: "browser" }),
      },
    );

    expect(deleted.status).toBe(200);
    const rows = await history({ folderId: "history", path: "guide/a.md" }, undefined, f.options);
    expect(rows).toHaveLength(2);
    expect(rows[1]).not.toHaveProperty("value");
    expect(rows[1]).not.toHaveProperty("version");
    expect(await history({ folderId: "history", path: "elsewhere" }, undefined, f.options)).toEqual(
      [],
    );
  } finally {
    await f.host.stop();
  }

  const offsets: string[] = [];

  const fetcher = Object.assign(
    async (input: RequestInfo | URL) => {
      const url = new URL(input instanceof Request ? input.url : input.toString());
      const offset = url.searchParams.get("offset")!;
      offsets.push(offset);

      return new Response(
        JSON.stringify(
          offset === "-1"
            ? [
                {
                  type: "file",
                  key: "a",
                  value: {
                    path: "a",
                    version: 7,
                    hash: "0".repeat(64),
                    size: 0,
                    author: "writer",
                    text: "",
                  },
                  headers: { operation: "upsert", txid: "write" },
                },
              ]
            : [{ type: "file", key: "a", headers: { operation: "delete", txid: "delete" } }],
        ),
        {
          headers: {
            "stream-next-offset": "cursor",
            "stream-up-to-date": offset === "-1" ? "false" : "true",
          },
        },
      );
    },
    { preconnect: globalThis.fetch.preconnect },
  );

  const rows = await history({ folderId: "slash/id", path: "a" }, undefined, {
    origin,
    fetch: fetcher,
    writerHeaders: () => ({}),
  });

  expect(offsets).toEqual(["-1", "cursor"]);
  expect(rows).toHaveLength(2);
});
