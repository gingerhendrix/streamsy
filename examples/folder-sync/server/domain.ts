import { Crypto, Effect, Schema } from "effect";

export const INLINE_LIMIT = 64 * 1024;

export const FILE_LIMIT = 64 * 1024 * 1024;

/** Folder sequence carried in the value. Offsets are read cursors only. */
export const Version = Schema.Int.check(Schema.isGreaterThan(0));

export const VersionFromString = Schema.String.check(Schema.isPattern(/^[1-9]\d*$/)).pipe(
  Schema.decodeTo(Schema.FiniteFromString),
  Schema.decodeTo(Version),
);

export const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

export const Identifier = Schema.String.check(
  Schema.isMinLength(1),
  // oxlint-disable-next-line eslint/no-control-regex -- Reject control characters at the input boundary.
  Schema.isPattern(/^[^\x00-\x1f\x7f]+$/),
  Schema.makeFilter((value) => value.isWellFormed()),
);

export const FolderId = Identifier.check(
  Schema.makeFilter((value) =>
    value.length <= 100 && new TextEncoder().encode(value).length <= 249
      ? undefined
      : "Folder IDs must be at most 100 decoded UTF-16 code units and 249 UTF-8 bytes",
  ),
);

export const CreateFolder = Schema.Struct({
  txid: Identifier,
  folderId: FolderId,
  title: Identifier,
});

export class InvalidFile extends Schema.TaggedError<InvalidFile>()("InvalidFile", {
  message: Schema.String,
}) {}

export class FileTooLarge extends Schema.TaggedError<FileTooLarge>()("FileTooLarge", {
  size: Schema.Int,
}) {}

export class MissingFile extends Schema.TaggedError<MissingFile>()("MissingFile", {
  message: Schema.String,
}) {}

export class FolderConflict extends Schema.TaggedError<FolderConflict>()("FolderConflict", {
  message: Schema.String,
}) {}

export class BlobMissing extends Schema.TaggedError<BlobMissing>()("BlobMissing", { hash: Hash }) {}

const canonicalPath = (path: string): string | undefined => {
  const parts = path.split("/");

  if (
    !path ||
    !path.isWellFormed() ||
    path.startsWith("/") ||
    path.endsWith("/") ||
    // oxlint-disable-next-line eslint/no-control-regex -- Reject filesystem control characters.
    /[\\\x00-\x1f\x7f]/.test(path) ||
    parts.includes("..")
  )
    return undefined;
  const normalized = parts.filter((part) => part !== "" && part !== ".").join("/");

  return normalized === "" ? undefined : normalized;
};

export const Path = Identifier.check(Schema.makeFilter((path) => canonicalPath(path) === path));

export const normalisePath = (path: string) => {
  const normalized = canonicalPath(path);

  return normalized === undefined
    ? Effect.fail(
        new InvalidFile({
          message: "Expected a relative file path without traversal or control characters",
        }),
      )
    : Effect.succeed(normalized);
};

const contentFields = {
  hash: Hash,
  size: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  author: Identifier,
  text: Schema.optionalKey(Schema.String),
};

export const CommitFile = Schema.Struct({ txid: Identifier, ...contentFields });

export type CommitFile = typeof CommitFile.Type;

export const DeleteFile = Schema.Struct({ txid: Identifier, author: Identifier });

export type DeleteFile = typeof DeleteFile.Type;

export const File = Schema.Struct({ path: Path, version: Version, ...contentFields });

export type File = typeof File.Type;

export const Head = Schema.Struct({ version: Version, hash: Hash, txid: Identifier });

export type Head = typeof Head.Type;

export class PreconditionFailed extends Schema.TaggedError<PreconditionFailed>()(
  "PreconditionFailed",
  { head: Schema.NullOr(Head) },
) {}

export type Precondition = { readonly version: number } | { readonly absent: true } | undefined;

export const sha256 = Effect.fn("File.sha256")(function* (bytes: Uint8Array) {
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", bytes);

  return [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("");
});

export const inlineText = (bytes: Uint8Array): string | undefined => {
  if (bytes.length > INLINE_LIMIT) return undefined;

  try {
    const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);

    return text.includes("\0") ? undefined : text;
  } catch {
    return undefined;
  }
};

export const prepareContent = Effect.fn("File.prepareContent")(function* (bytes: Uint8Array) {
  if (bytes.length > FILE_LIMIT) return yield* new FileTooLarge({ size: bytes.length });
  const hash = yield* sha256(bytes);
  const text = inlineText(bytes);

  return text === undefined ? { hash, size: bytes.length } : { hash, size: bytes.length, text };
});
