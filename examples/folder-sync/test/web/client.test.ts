/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise -- Exercise the default native browser transport against the local HTTP host. */
import { Array } from "effect";
import { expect, test, vi } from "vitest";
import {
  browserTransport,
  history,
  historyReader,
  readFile,
  request,
  saveFile,
  search,
  textContent,
} from "../../src/client.ts";
import type { History } from "../../src/client.ts";
import { createFileDb, editFile } from "../../src/db.ts";
import { protocolHost, writerToken } from "../protocol-host.ts";

test("default browser fetch keeps its global receiver for saves, history, search and blobs", async () => {
  const host = protocolHost();
  const calls: string[] = [];
  const preconnect = globalThis.fetch.preconnect;

  const nativeFetch = Object.assign(
    function (this: typeof globalThis, input: RequestInfo | URL, init?: RequestInit) {
      if (this !== globalThis) throw new TypeError("Illegal invocation");
      const sent = new Request(input, init);
      calls.push(`${sent.method} ${new URL(sent.url).pathname}`);

      return host.fetch(sent);
    },
    { preconnect },
  );

  vi.stubGlobal("window", { location: { origin: "http://browser.test" } });
  vi.stubGlobal("localStorage", { getItem: () => writerToken });
  vi.stubGlobal("fetch", nativeFetch);

  try {
    const transport = browserTransport();
    expect(transport.fetch.preconnect).toBe(preconnect);
    const headers = new Headers(transport.writerHeaders());
    headers.set("content-type", "application/json");
    await request("/api/folders", {
      method: "POST",
      headers,
      body: JSON.stringify({ folderId: "docs", title: "Docs", txid: "folder" }),
    });
    const one = await textContent("one");
    await request(`/api/blobs/${one.hash}`, {
      method: "PUT",
      headers: transport.writerHeaders(),
      body: "one",
    });
    await request("/api/folders/docs/files/a", {
      method: "PUT",
      headers,
      body: JSON.stringify({ ...one, txid: "first", author: "test", parent: null }),
    });
    const bytes = new Uint8Array([0, 255, 1]);
    const hash = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    const file = { path: "a", version: 1, parent: 1, hash, size: bytes.length, author: "browser" };
    const saved = await saveFile("docs", file, 1, "second", transport, bytes);
    expect(saved).toEqual({ version: 2, parent: 1, txid: "second", hash });
    expect(await history({ folderId: "docs", path: "a" })).toHaveLength(2);
    expect(
      await readFile({ folderId: "docs", path: "a" }, file, new AbortController().signal),
    ).toMatchObject({ hash, size: bytes.length, text: null });
    expect(await search("one", new AbortController().signal)).toBeInstanceOf(globalThis.Array);
    expect(calls).toContain(`PUT /api/blobs/${hash}`);
    expect(calls).toContain(`GET /api/blobs/${hash}`);
    expect(calls).toContain("GET /api/search");
  } finally {
    vi.unstubAllGlobals();
    await host.stop();
  }
});

test("history revalidates its bootstrap and later pages", async () => {
  const modes: RequestCache[] = [];
  const offsets: Array<string | null> = [];

  const fetcher = Object.assign(
    (input: RequestInfo | URL, init?: RequestInit) => {
      const sent = new Request(input, init);
      modes.push(sent.cache);
      offsets.push(new URL(sent.url).searchParams.get("offset"));

      return Promise.resolve(
        Response.json([], {
          headers: {
            "stream-next-offset": modes.length === 1 ? "page-two" : "tail",
            "stream-up-to-date": String(modes.length === 2),
          },
        }),
      );
    },
    { preconnect: globalThis.fetch.preconnect },
  );

  await history({ folderId: "docs", path: "a" }, undefined, {
    origin: "http://browser.test",
    fetch: fetcher,
    writerHeaders: () => ({}),
  });
  expect(offsets).toEqual(["-1", "page-two"]);
  expect(modes).toEqual(["no-cache", "no-cache"]);
});

test("the panel history reader refreshes accepted saves and a later StreamDB row update", async () => {
  const host = protocolHost();
  const origin = "http://browser.test";
  const authorization = { authorization: `Bearer ${writerToken}` };
  const cache = new Map<string, Response>();

  const fetcher = Object.assign(
    async (input: RequestInfo | URL, init?: RequestInit) => {
      const sent = new Request(input, init);
      const url = new URL(sent.url);
      const bootstrap = sent.method === "GET" && url.searchParams.get("offset") === "-1";
      const cached = cache.get(sent.url);

      // Model the browser cache that caused F-B2. Bun fetch has no HTTP cache itself.
      if (bootstrap && cached && sent.cache !== "no-cache") return cached.clone();
      const response = await host.fetch(sent);

      if (bootstrap) cache.set(sent.url, response.clone());

      return response;
    },
    { preconnect: globalThis.fetch.preconnect },
  );

  const options = { origin, fetch: fetcher, writerHeaders: () => authorization };
  const selection = { folderId: "docs", path: "a" };
  let rows: History = [];

  const panel = historyReader(
    (next) => {
      rows = next;
    },
    (selected, signal) => history(selected, signal, options),
  );

  const db = createFileDb("docs", options);

  try {
    await request(
      "/api/folders",
      {
        method: "POST",
        headers: { ...authorization, "content-type": "application/json" },
        body: JSON.stringify({ folderId: "docs", title: "Docs", txid: "folder" }),
      },
      options,
    );
    const one = await textContent("one");
    await request(
      `/api/blobs/${one.hash}`,
      { method: "PUT", headers: authorization, body: "one" },
      options,
    );
    await request(
      "/api/folders/docs/files/a",
      {
        method: "PUT",
        headers: { ...authorization, "content-type": "application/json" },
        body: JSON.stringify({ ...one, txid: "first", author: "test", parent: null }),
      },
      options,
    );
    await db.preload();
    await panel.refresh(selection);
    expect(rows).toHaveLength(1);

    for (const text of ["two", "three"]) {
      await editFile(db, db.collections.files.get("a")!, text, "browser", text);
      // editFile resolves after the response txid is observed, as FilePanel requires.
      await panel.refresh(selection);
    }

    expect(Array.map(rows, (row) => ("value" in row ? row.value.version : null))).toEqual([
      1, 2, 3,
    ]);
    await saveFile(
      "docs",
      { ...db.collections.files.get("a")!, ...(await textContent("other")) },
      3,
      "other-writer",
      options,
      new TextEncoder().encode("other"),
    );
    await db.utils.awaitTxId("other-writer");
    expect(db.collections.files.get("a")?.version).toBe(4);
    await panel.refresh(selection);
    expect(rows).toHaveLength(4);
  } finally {
    panel.cancel();
    db.close();
    await host.stop();
  }
});

test("a superseded history refresh cannot replace newer panel rows", async () => {
  let release!: (rows: History) => void;
  let calls = 0;
  let applied = 0;

  const reader = historyReader(
    () => applied++,
    () => {
      calls++;

      // oxlint-disable-next-line effecttsgo/new-promise -- Hold the old request after cancellation to check late-response ownership.
      return calls === 1
        ? new Promise<History>((resolve) => {
            release = resolve;
          })
        : Promise.resolve([]);
    },
  );

  const old = reader.refresh({ folderId: "docs", path: "a" });
  await reader.refresh({ folderId: "docs", path: "a" });
  release([]);
  await old;
  expect(applied).toBe(1);
  reader.cancel();
});
