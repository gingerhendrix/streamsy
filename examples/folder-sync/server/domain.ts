import { Crypto, Effect, Schema } from "effect";

export const INLINE_LIMIT = 64 * 1024;

export const FILE_LIMIT = 8 * 1024 * 1024;

/** Folder sequence carried in the log entry. Stream offsets are only cursors. */
export const Version = Schema.Int.check(Schema.isGreaterThan(0));

/** Decimal representation at HTTP header and query boundaries. */
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

/** Router limits decoded UTF-16 units; the 512-character hex stream id allows 249 UTF-8 bytes. */
export const FolderId = Identifier.check(
  Schema.makeFilter((value) =>
    value.length <= 100 && new TextEncoder().encode(value).length <= 249
      ? undefined
      : "Folder IDs must be at most 100 decoded UTF-16 code units and 249 UTF-8 bytes",
  ),
);

export const CreateFolder = Schema.Struct({ folderId: FolderId, title: Identifier });

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
  version: Version,
  path: Path,
  base: Schema.NullOr(Version),
  opId: Identifier,
  author: Identifier,
  origin: Identifier,
  overwrote: Schema.optionalKey(Version),
  updatedAt: Schema.optionalKey(Schema.String),
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

export const OperationReceipt = Schema.Struct({
  path: Path,
  version: Version,
  overwrote: Schema.optionalKey(Version),
});

export type OperationReceipt = typeof OperationReceipt.Type;

export const Operations = Schema.Record(Schema.String, OperationReceipt);

export type Operations = typeof Operations.Type;

export const FolderState = Schema.Struct({
  seq: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  heads: Heads,
  ops: Operations,
});

export type FolderState = typeof FolderState.Type;

export const headAt = (heads: Heads, path: string): Head | undefined =>
  Object.hasOwn(heads, path) ? heads[path] : undefined;

export const operationAt = (ops: Operations, opId: string): OperationReceipt | undefined =>
  Object.hasOwn(ops, opId) ? ops[opId] : undefined;

/** Keep the first receipt when rebuilding duplicate entries. Versions come from the entry. */
export const foldHead = (state: FolderState, operation: FileOp): FolderState => {
  const { version } = operation;
  const common = { version, opId: operation.opId, operation };

  const head: Head =
    operation.type === "put"
      ? { ...common, hash: operation.hash, size: operation.size, deleted: false }
      : { ...common, size: 0, deleted: true };

  const receipt = { path: operation.path, version, ...overwriteMarkFromOp(operation) };

  const ops =
    operationAt(state.ops, operation.opId) === undefined
      ? { ...state.ops, [operation.opId]: receipt }
      : state.ops;

  return { seq: version, heads: { ...state.heads, [operation.path]: head }, ops };
};

export const overwriteMarkFromOp = (operation: FileOp | OperationReceipt) =>
  operation.overwrote === undefined ? {} : { overwrote: operation.overwrote };

/** A null base means the author had no head. Folder sequences follow commit order. */
export const overwriteMark = (head: Head | undefined, base: number | null) =>
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
