import { Schema } from "effect";
import { FileOp, Version, VersionFromString } from "../server/domain.ts";
import { SearchResult } from "../server/search-result.ts";
import { encodeFilePath } from "../sync/decisions.ts";

export interface Selection {
  folderId: string;
  path: string;
  at?: number;
}

export const fileUrl = ({ folderId, path, at }: Selection) =>
  `/api/folders/${encodeURIComponent(folderId)}/files/${encodeFilePath(path)}${at === undefined ? "" : `?at=${encodeURIComponent(at)}`}`;

export const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

export type History = typeof History.Type;

export const Receipt = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

export type Receipt = typeof Receipt.Type;

const ErrorBody = Schema.Struct({ error: Schema.String });

/** Native browser transport keeps AbortSignal and raw byte semantics at this boundary. */
export const request = (url: string, init?: RequestInit): Promise<Response> => {
  // oxlint-disable-next-line effecttsgo/global-fetch -- Native browser API adapter, including cancellation on navigation.
  return fetch(url, init).then((response) => {
    if (response.ok) return response;

    return response.json().then((body) => {
      const decoded = Schema.decodeUnknownSync(ErrorBody)(body);
      throw new Error(decoded.error);
    });
  });
};

export const readableText = (bytes: Uint8Array): string | null => {
  try {
    const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);

    return text.includes("\0") ? null : text;
  } catch {
    return null;
  }
};

export const readFile = (selection: Selection, signal: AbortSignal) =>
  request(fileUrl(selection), { signal }).then((response) => {
    const version = Schema.decodeUnknownSync(VersionFromString)(
      response.headers.get("x-file-version"),
    );

    return response.arrayBuffer().then((buffer) => ({
      version,
      text: readableText(new Uint8Array(buffer)),
      size: buffer.byteLength,
    }));
  });

export type OpenFile = Awaited<ReturnType<typeof readFile>>;

export const history = ({ folderId, path }: Selection, signal?: AbortSignal) =>
  request(`/api/folders/${encodeURIComponent(folderId)}/history/${encodeFilePath(path)}`, {
    signal,
  })
    .then((response) => response.json())
    .then(Schema.decodeUnknownSync(History));

export const search = (q: string, signal: AbortSignal) =>
  request(`/api/search?q=${encodeURIComponent(q)}`, { signal })
    .then((response) => response.json())
    .then(Schema.decodeUnknownSync(Schema.Array(SearchResult)));

export const saveFile = (
  selection: Selection,
  text: string,
  base: number,
  author: string,
  opId: string,
) =>
  request(fileUrl({ folderId: selection.folderId, path: selection.path }), {
    method: "PUT",
    headers: {
      ...writerHeaders(),
      "x-file-base": String(base),
      "x-file-op-id": encodeURIComponent(opId),
      "x-file-author": encodeURIComponent(author.trim() || "anonymous"),
      "x-file-origin": encodeURIComponent("browser"),
    },
    body: new TextEncoder().encode(text),
  })
    .then((response) => response.json())
    .then(Schema.decodeUnknownSync(Receipt));

/** Browser-only writer setting. Reads stay public. */
export const writerHeaders = (): Record<string, string> => {
  const token = localStorage.getItem("folder-sync-writer-token");

  return token ? { authorization: `Bearer ${token}` } : {};
};
