import { Array, Crypto, Effect, Schema } from "effect";
import { hex } from "./bytes.ts";

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

export class InvalidFile extends Schema.TaggedError<
  InvalidFile,
  { readonly brand: unique symbol }
>()("InvalidFile", {
  message: Schema.String,
}) {}

export class FileTooLarge extends Schema.TaggedError<
  FileTooLarge,
  { readonly brand: unique symbol }
>()("FileTooLarge", {
  size: Schema.Int,
}) {}

export class MissingFile extends Schema.TaggedError<
  MissingFile,
  { readonly brand: unique symbol }
>()("MissingFile", {
  message: Schema.String,
}) {}

export class FolderConflict extends Schema.TaggedError<
  FolderConflict,
  { readonly brand: unique symbol }
>()("FolderConflict", {
  message: Schema.String,
}) {}

/** A read of a path whose last change is a tombstone. */
export class DeletedFile extends Schema.TaggedError<
  DeletedFile,
  { readonly brand: unique symbol }
>()("DeletedFile", {
  version: Version,
}) {}

export class BlobMissing extends Schema.TaggedError<
  BlobMissing,
  { readonly brand: unique symbol }
>()("BlobMissing", { hash: Hash }) {}

/** Other writes to the folder kept landing first. The client may retry. */
export class WriteConflict extends Schema.TaggedError<
  WriteConflict,
  { readonly brand: unique symbol }
>()("WriteConflict", {
  folderId: Schema.String,
}) {}

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

  const normalized = Array.join(
    Array.filter(parts, (part) => part !== "" && part !== "."),
    "/",
  );

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

/** A file's content lives only in the blob store. A change names it by hash and size. */
const contentFields = {
  hash: Hash,
  size: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  author: Identifier,
};

/**
 * The version that a writer started from. `null` means that the writer saw no file
 * at the path. The server records it and does not check it against the head.
 */
export const Parent = Schema.NullOr(Version);

export const CommitFile = Schema.Struct({ txid: Identifier, parent: Parent, ...contentFields });

export type CommitFile = typeof CommitFile.Type;

export const DeleteFile = Schema.Struct({ txid: Identifier, author: Identifier, parent: Parent });

export type DeleteFile = typeof DeleteFile.Type;

/** A live file value: its content is in the blob store under `hash`. */
export const File = Schema.Struct({
  path: Path,
  version: Version,
  parent: Parent,
  ...contentFields,
});

export type File = typeof File.Type;

/**
 * A delete is an upsert of a tombstone. It takes a version and names its parent,
 * so every change of a path is part of one parent chain.
 */
export const Tombstone = Schema.Struct({
  path: Path,
  version: Version,
  parent: Parent,
  author: Identifier,
  deleted: Schema.Literal(true),
});

export type Tombstone = typeof Tombstone.Type;

/** The value of one change on a folder stream. */
export const FileValue = Schema.Union([File, Tombstone]);

export type FileValue = typeof FileValue.Type;

export const isTombstone = (value: FileValue): value is Tombstone => "deleted" in value;

/** The answer to a commit. */
export const Head = Schema.Struct({
  version: Version,
  parent: Parent,
  hash: Hash,
  txid: Identifier,
});

export type Head = typeof Head.Type;

/** The answer to a delete that appended a tombstone. */
export const Deleted = Schema.Struct({ version: Version, parent: Parent, txid: Identifier });

export type Deleted = typeof Deleted.Type;

export const sha256 = Effect.fn("File.sha256")(function* (bytes: Uint8Array) {
  const crypto = yield* Crypto.Crypto;

  return hex(yield* crypto.digest("SHA-256", bytes));
});

/** The hash and size that name `bytes` in the blob store. */
export const prepareContent = Effect.fn("File.prepareContent")(function* (bytes: Uint8Array) {
  if (bytes.length > FILE_LIMIT) return yield* new FileTooLarge({ size: bytes.length });

  return { hash: yield* sha256(bytes), size: bytes.length };
});
