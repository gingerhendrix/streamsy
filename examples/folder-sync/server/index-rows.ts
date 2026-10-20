import { Schema } from "effect";
import { File, FolderId, Version } from "./domain.ts";

/** One `/api/ls` row. Its shape is public and does not follow the source streams. */
export const ListedFile = Schema.Struct({
  id: Schema.String,
  folderId: FolderId,
  ...File.fields,
  updatedAt: Schema.String,
});

export type ListedFile = typeof ListedFile.Type;

/** The index row key of one file. */
export const fileKey = (folderId: string, path: string) => JSON.stringify([folderId, path]);

export const SnippetPart = Schema.Struct({ text: Schema.String, match: Schema.Boolean });

export const SearchResult = Schema.Struct({
  folderId: Schema.String,
  path: Schema.String,
  version: Version,
  snippet: Schema.Array(SnippetPart),
});

export type SearchResult = typeof SearchResult.Type;

/** SQLite delimiters become flags. Every byte of file content remains plain text. */
export const snippetParts = (snippet: string): (typeof SnippetPart.Type)[] => {
  const parts: (typeof SnippetPart.Type)[] = [];
  let match = false;

  // oxlint-disable-next-line eslint/no-control-regex -- FTS5 uses these control bytes as match delimiters.
  for (const text of snippet.split(/([\u0001\u0002])/)) {
    if (text === "\u0001") match = true;
    else if (text === "\u0002") match = false;
    else if (text !== "") parts.push({ text, match });
  }

  return parts;
};
