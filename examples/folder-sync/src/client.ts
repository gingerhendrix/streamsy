import { Schema } from "effect";
import { CommitFile, File, Head, inlineText } from "../server/domain.ts";
import { FolderKey } from "../server/streams.ts";
import { SearchResult } from "../server/search-result.ts";
import { encodeFilePath } from "../sync/decisions.ts";

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

export const fileUrl = ({ folderId, path }: Selection) =>
  `/api/folders/${encodeURIComponent(folderId)}/files/${encodeFilePath(path)}`;

export const folderStreamUrl = (folderId: string) =>
  `/streams/folders/${Schema.encodeSync(FolderKey)(folderId)}/files`;

const ChangeHeaders = Schema.Struct({
  txid: Schema.String,
  timestamp: Schema.optionalKey(Schema.String),
});

const FileChange = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("file"),
    key: Schema.String,
    value: File,
    headers: Schema.Struct({ ...ChangeHeaders.fields, operation: Schema.Literal("upsert") }),
  }),
  Schema.Struct({
    type: Schema.Literal("file"),
    key: Schema.String,
    headers: Schema.Struct({ ...ChangeHeaders.fields, operation: Schema.Literal("delete") }),
  }),
]);

const HistoryMessage = Schema.Union([
  FileChange,
  Schema.Struct({ headers: Schema.Struct({ control: Schema.Literal("reset") }) }),
]);

export type History = ReadonlyArray<typeof FileChange.Type>;

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

export const readableText = (bytes: Uint8Array): string | null => {
  try {
    const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);

    return text.includes("\0") ? null : text;
  } catch {
    return null;
  }
};

/** History is a normal, paginated replay of the folder State stream. */
// oxlint-disable-next-line effecttsgo/async-function -- Browser history consumes native fetch pages with navigation cancellation.
export async function history(
  { folderId, path }: Selection,
  signal?: AbortSignal,
  transport = browserTransport(),
): Promise<History> {
  let offset = "-1";
  let rows: Array<typeof FileChange.Type> = [];

  while (true) {
    const response = await transport.fetch(
      new URL(
        `${folderStreamUrl(folderId)}?offset=${encodeURIComponent(offset)}`,
        transport.origin,
      ),
      { signal },
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
        : Schema.decodeUnknownSync(Schema.Array(HistoryMessage))(await response.json());

    for (const change of messages) {
      if (!("key" in change)) rows = [];
      else if (change.key === path) rows.push(change);
    }

    if (response.headers.get("stream-up-to-date") === "true") return rows;
    const next = response.headers.get("stream-next-offset");

    if (next === null || next === offset)
      throw new Error("History read did not advance its cursor");
    offset = next;
  }
}

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
      (row) => "value" in row && row.value.version === selection.at,
    );

    file = change !== undefined && "value" in change ? change.value : undefined;
  }

  if (file === undefined) throw new Error("File not found");

  const text =
    file.text !== undefined
      ? file.text
      : readableText(
          new Uint8Array(
            await (await request(`/api/blobs/${file.hash}`, { signal }, transport)).arrayBuffer(),
          ),
        );

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
    const hash = [...new Uint8Array(digest)]
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");

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
      `/api/blobs/${file.hash}`,
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
    fileUrl({ folderId, path: file.path }),
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
