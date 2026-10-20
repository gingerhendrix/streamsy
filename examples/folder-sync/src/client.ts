import { Schema } from "effect";
import { decodeText, hex } from "../server/bytes.ts";
import { CommitFile, File, Head, inlineText } from "../server/domain.ts";
import { SearchResult } from "../server/index-rows.ts";
import { FileMessage, isReset, isUpsert, type FileChange } from "../server/messages.ts";
import { blobUrl, fileUrl, folderStreamUrl, streamRead } from "../server/routes.ts";

export interface Selection {
  folderId: string;
  path: string;
  at?: number;
}

export interface Transport {
  origin: string;
  fetch: typeof globalThis.fetch;
  writerHeaders: () => HeadersInit;
}

/** Browser defaults are resolved only when needed, so tests can supply a native host. */
export const browserTransport = (options: Partial<Transport> = {}): Transport => ({
  origin: options.origin ?? window.location.origin,
  fetch:
    options.fetch ??
    Object.assign(globalThis.fetch.bind(globalThis), { preconnect: globalThis.fetch.preconnect }),
  writerHeaders: options.writerHeaders ?? writerHeaders,
});

export type History = ReadonlyArray<FileChange>;

/** Native browser transport keeps AbortSignal and raw byte semantics at this boundary. */
export const request = (
  url: string,
  init?: RequestInit,
  transport = browserTransport(),
): Promise<Response> =>
  transport.fetch(new URL(url, transport.origin), init).then((response) => {
    if (response.ok) return response;

    if (response.status === 412)
      throw new Error(
        "The file changed on the server. Keep your draft and open the current version before saving again.",
      );

    return response.text().then((body) => {
      let message = `Request failed with status ${response.status}`;

      try {
        const decoded = Schema.decodeUnknownSync(Schema.Struct({ error: Schema.String }))(
          JSON.parse(body),
        );

        message = decoded.error;
      } catch {
        // A proxy may return an HTML error page instead of the API error shape.
      }

      throw new Error(message);
    });
  });

/** History is a normal, paginated replay of the folder State stream. */
// oxlint-disable-next-line effecttsgo/async-function -- Browser history consumes native fetch pages with navigation cancellation.
export async function history(
  { folderId, path }: Selection,
  signal?: AbortSignal,
  transport = browserTransport(),
): Promise<History> {
  let offset = "-1";
  let rows: Array<FileChange> = [];

  while (true) {
    const response = await transport.fetch(
      new URL(streamRead(folderStreamUrl(folderId), offset), transport.origin),
      // Even the last page at a fixed offset can grow as new changes are appended.
      { signal, cache: "no-cache" },
    );

    if (response.status === 410) {
      if (offset === "-1") throw new Error("History bootstrap offset is gone");
      rows = [];
      offset = "-1";
      continue;
    }

    if (!response.ok) throw new Error(`History read failed with status ${response.status}`);

    const messages =
      response.status === 204
        ? []
        : Schema.decodeUnknownSync(Schema.Array(FileMessage))(await response.json());

    for (const change of messages) {
      if (isReset(change)) rows = [];
      else if (change.key === path) rows.push(change);
    }

    if (response.headers.get("stream-up-to-date") === "true") return rows;
    const next = response.headers.get("stream-next-offset");

    if (next === null || next === offset)
      throw new Error("History read did not advance its cursor");
    offset = next;
  }
}

/** The panel's latest refresh owns the rows, including when a save races a row update. */
export const historyReader = (apply: (rows: History) => void, load: typeof history = history) => {
  let controller: AbortController | undefined;

  return {
    refresh: (selection: Selection) => {
      controller?.abort();
      const pending = new AbortController();
      controller = pending;

      return load(selection, pending.signal)
        .then((rows) => {
          if (!pending.signal.aborted) apply(rows);
        })
        .catch((error: Error) => {
          if (!pending.signal.aborted) throw error;
        });
    },
    cancel: () => controller?.abort(),
  };
};

export interface OpenFile {
  version: number;
  hash: string;
  size: number;
  text: string | null;
  inline: boolean;
}

// oxlint-disable-next-line effecttsgo/async-function -- Browser file reads use native blob responses and AbortSignal cancellation.
export async function readFile(
  selection: Selection,
  current: File | undefined,
  signal: AbortSignal,
  transport = browserTransport(),
): Promise<OpenFile> {
  let file = current;

  if (selection.at !== undefined) {
    const change = (await history(selection, signal, transport)).find(
      (row) => isUpsert(row) && row.value.version === selection.at,
    );

    file = change !== undefined && isUpsert(change) ? change.value : undefined;
  }

  if (file === undefined) throw new Error("File not found");

  const text =
    file.text !== undefined
      ? file.text
      : (decodeText(
          new Uint8Array(
            await (await request(blobUrl(file.hash), { signal }, transport)).arrayBuffer(),
          ),
        ) ?? null);

  return {
    version: file.version,
    hash: file.hash,
    size: file.size,
    text,
    inline: file.text !== undefined,
  };
}

export const search = (q: string, signal: AbortSignal) =>
  request(`/api/search?q=${encodeURIComponent(q)}`, { signal })
    .then((response) => response.json())
    .then(Schema.decodeUnknownSync(Schema.Array(SearchResult)));

/** Hashing uses Web Crypto, shared by browsers and Bun's browser-client tests. */
export const textContent = (text: string) => {
  const bytes = new TextEncoder().encode(text);

  return crypto.subtle.digest("SHA-256", bytes).then((digest) => {
    const hash = hex(new Uint8Array(digest));
    const inline = inlineText(bytes);

    let content: Pick<CommitFile, "hash" | "size" | "text"> = { hash, size: bytes.length };

    if (inline !== undefined) content = { ...content, text: inline };

    return content;
  });
};

// oxlint-disable-next-line effecttsgo/async-function -- The optimistic mutation awaits native upload and commit responses.
export async function saveFile(
  folderId: string,
  file: File,
  base: number,
  txid: string,
  transport: Transport,
  bytes: Uint8Array,
) {
  const upload = () =>
    request(
      blobUrl(file.hash),
      {
        method: "PUT",
        headers: transport.writerHeaders(),
        body: new Uint8Array(bytes),
      },
      transport,
    );

  if (file.text === undefined) await upload();
  const headers = new Headers(transport.writerHeaders());
  headers.set("content-type", "application/json");
  headers.set("if-match", `"${base}"`);
  let input: CommitFile = { txid, author: file.author, hash: file.hash, size: file.size };

  if (file.text !== undefined) input = { ...input, text: file.text };
  const body = JSON.stringify(input);

  const response = await request(
    fileUrl(folderId, file.path),
    {
      method: "PUT",
      headers,
      body,
    },
    transport,
  );

  return Schema.decodeUnknownSync(Head)(await response.json());
}

/** Browser-only writer setting. Reads stay public. */
export const writerHeaders = (): Record<string, string> => {
  const token = localStorage.getItem("folder-sync-writer-token");

  return token ? { authorization: `Bearer ${token}` } : {};
};
