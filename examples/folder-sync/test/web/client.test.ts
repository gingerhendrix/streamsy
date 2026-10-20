/* oxlint-disable effecttsgo/async-function -- Exercise the default native browser transport against the local HTTP host. */
import { expect, test, vi } from "vitest";
import {
  browserTransport,
  history,
  readFile,
  request,
  saveFile,
  search,
  textContent,
} from "../../src/client.ts";
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
    await request("/api/folders/docs/files/a", {
      method: "PUT",
      headers,
      body: JSON.stringify({ ...(await textContent("one")), txid: "first", author: "test" }),
    });
    const bytes = new Uint8Array([0, 255, 1]);
    const hash = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    const file = { path: "a", version: 1, hash, size: bytes.length, author: "browser" };
    const saved = await saveFile("docs", file, 1, "second", transport, bytes);
    expect(saved).toMatchObject({ version: 2, txid: "second", hash });
    expect(await history({ folderId: "docs", path: "a" })).toHaveLength(2);
    expect(
      await readFile({ folderId: "docs", path: "a" }, file, new AbortController().signal),
    ).toMatchObject({ hash, size: bytes.length, text: null });
    expect(await search("one", new AbortController().signal)).toBeInstanceOf(Array);
    expect(calls).toContain(`PUT /api/blobs/${hash}`);
    expect(calls).toContain(`GET /api/blobs/${hash}`);
    expect(calls).toContain("GET /api/search");
  } finally {
    vi.unstubAllGlobals();
    await host.stop();
  }
});
