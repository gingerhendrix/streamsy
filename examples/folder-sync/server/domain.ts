import { Crypto, Effect, Schema } from "effect";

export const INLINE_LIMIT = 64 * 1024;

export const FILE_LIMIT = 8 * 1024 * 1024;

export const Version = Schema.String.check(Schema.isPattern(/^\d{16}_\d{16}$/));

export const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

export const Identifier = Schema.String.check(
  Schema.isMinLength(1),
  // oxlint-disable-next-line eslint/no-control-regex -- Reject control characters at the input boundary.
  Schema.isPattern(/^[^\x00-\x1f\x7f]+$/),
  Schema.makeFilter((value) => value.isWellFormed()),
);

export class InvalidFile extends Schema.TaggedError<InvalidFile>()("InvalidFile", {
  message: Schema.String,
}) {}

export class FileTooLarge extends Schema.TaggedError<FileTooLarge>()("FileTooLarge", {
  size: Schema.Int,
}) {}

export class MissingFile extends Schema.TaggedError<MissingFile>()("MissingFile", {
  message: Schema.String,
}) {}

export class VersionUnavailable extends Schema.TaggedError<VersionUnavailable>()(
  "VersionUnavailable",
  { version: Version },
) {}

/** POSIX file names. Traversal is rejected before redundant separators and dots are removed. */
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

const fields = {
  path: Path,
  base: Schema.NullOr(Version),
  opId: Identifier,
  author: Identifier,
  origin: Identifier,
  overwrote: Schema.optionalKey(Version),
};

const putFields = {
  ...fields,
  type: Schema.Literal("put"),
  hash: Hash,
  size: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(FILE_LIMIT)),
};

export const PutFile = Schema.Union([
  Schema.Struct({ ...putFields, inline: Schema.String }),
  Schema.Struct({ ...putFields, blob: Hash }),
]).check(
  Schema.makeFilter((operation) =>
    "blob" in operation
      ? operation.hash === operation.blob
      : new TextEncoder().encode(operation.inline).length === operation.size &&
        operation.size <= INLINE_LIMIT &&
        !operation.inline.includes("\0"),
  ),
);

export type PutFile = typeof PutFile.Type;

export const DeleteFile = Schema.Struct({ ...fields, type: Schema.Literal("delete") });

export type DeleteFile = typeof DeleteFile.Type;

export const FileOp = Schema.Union([PutFile, DeleteFile]);

export type FileOp = typeof FileOp.Type;

export const WriteMetadata = Schema.Struct({
  base: fields.base,
  opId: Identifier,
  author: Identifier,
  origin: Identifier,
});

export type WriteMetadata = typeof WriteMetadata.Type;

export const Head = Schema.Struct({
  version: Version,
  hash: Schema.optionalKey(Hash),
  size: Schema.Int,
  deleted: Schema.Boolean,
  opId: Identifier,
  operation: FileOp,
});

export type Head = typeof Head.Type;

export const Heads = Schema.Record(Schema.String, Head);

export type Heads = typeof Heads.Type;

/** The caller supplies the exact log offset of the entry. Deletes retain tombstones. */
export const headAt = (heads: Heads, path: string): Head | undefined =>
  Object.hasOwn(heads, path) ? heads[path] : undefined;

export const foldHead = (heads: Heads, operation: FileOp, version: string): Heads => {
  const common = { version, opId: operation.opId, operation };

  const head: Head =
    operation.type === "put"
      ? { ...common, hash: operation.hash, size: operation.size, deleted: false }
      : { ...common, size: 0, deleted: true };

  return { ...heads, [operation.path]: head };
};

/** A null base means the author had no head. Comparing fixed-width offsets follows commit order. */
export const overwriteMark = (head: Head | undefined, base: string | null) =>
  head !== undefined && (base === null || head.version > base) ? { overwrote: head.version } : {};

export const sha256 = Effect.fn("File.sha256")(function* (bytes: Uint8Array) {
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", bytes);

  return [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("");
});

const inlineText = (bytes: Uint8Array): string | undefined => {
  if (bytes.length > INLINE_LIMIT) return undefined;

  try {
    const text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);

    return text.includes("\0") ? undefined : text;
  } catch {
    return undefined;
  }
};

/** Text is inline only when UTF-8 round trips without losing bytes. All other content is a blob. */
export const prepareContent = Effect.fn("File.prepareContent")(function* (bytes: Uint8Array) {
  if (bytes.length > FILE_LIMIT) return yield* new FileTooLarge({ size: bytes.length });
  const hash = yield* sha256(bytes);
  const inline = inlineText(bytes);

  return inline === undefined
    ? { hash, size: bytes.length, blob: hash }
    : { hash, size: bytes.length, inline };
});
