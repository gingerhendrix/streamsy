import { CommitBoundary, DurableState } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import { Context, Crypto, DateTime, Effect, Layer, Option, Schema, Stream } from "effect";
import { BlobStore } from "./blob-store.ts";
import {
  BlobMissing,
  CommitFile,
  CreateFolder,
  DeleteFile,
  File,
  FileTooLarge,
  FolderConflict,
  FolderId,
  Head,
  INLINE_LIMIT,
  FILE_LIMIT,
  InvalidFile,
  MissingFile,
  PreconditionFailed,
  normalisePath,
  inlineText,
  sha256,
  type Precondition,
} from "./domain.ts";
import { Files, Folders } from "./streams.ts";

const StoredHead = Schema.Struct({ head: Head, value: File });

const StoredHeadJson = Schema.fromJsonString(StoredHead);

const CounterJson = Schema.fromJsonString(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)));

const key = (folderId: string, path?: string) =>
  JSON.stringify(["folder-sync.owner.v1", folderId, path ?? null]);

const invalid = () => new InvalidFile({ message: "Invalid request" });

const condition = (head: Head | undefined, precondition: Precondition) =>
  precondition === undefined ||
  ("absent" in precondition ? head === undefined : head?.version === precondition.version)
    ? Effect.void
    : Effect.fail(new PreconditionFailed({ head: head ?? null }));

const make = Effect.gen(function* () {
  const files = yield* Files;
  const folders = yield* Folders;
  const records = yield* OwnerRecords;
  const boundary = yield* CommitBoundary;
  const blobs = yield* BlobStore;
  const crypto = yield* Crypto.Crypto;

  const requireFolder = Effect.fn("Folder.require")(function* (folderId: string) {
    yield* Schema.decodeEffect(FolderId)(folderId).pipe(Effect.mapError(invalid));

    return yield* files
      .head({ folderId })
      .pipe(
        Effect.catchTag("StreamNotFound", () => new MissingFile({ message: "Unknown folder" })),
      );
  });

  const load = Effect.fn("Folder.head")(function* (folderId: string, path: string) {
    const encoded = yield* records.read(key(folderId, path));

    return Option.isNone(encoded)
      ? undefined
      : yield* Schema.decodeEffect(StoredHeadJson)(encoded.value);
  });

  const create = Effect.fn("Folder.create")(function* (
    folderId: string,
    title: string,
    txid: string,
  ) {
    yield* Schema.decodeEffect(CreateFolder)({ folderId, title, txid }).pipe(
      Effect.mapError(invalid),
    );

    return yield* boundary.withTransaction(
      Effect.gen(function* () {
        const state = yield* folders.fold(
          {},
          () => new Map<string, typeof Folders.collections.folder.schema.Type>(),
          (rows, change) => {
            if ("value" in change) rows.set(change.key, change.value);
            else rows.delete(change.key);

            return rows;
          },
        );

        const existing = state.state.get(folderId);

        if (existing !== undefined) {
          if (existing.title !== title)
            return yield* new FolderConflict({ message: "Folder title differs" });

          return existing;
        }

        const value = { folderId, title };
        yield* files.create({ folderId });

        const changes = DurableState.changes(Folders.at({}), { offset: state.offset }, [
          DurableState.upsert("folder", value, {
            headers: { txid, timestamp: DateTime.formatIso(yield* DateTime.now) },
          }),
        ]);

        yield* folders.append({}, changes, { expectedOffset: state.offset });
        yield* records.write(key(folderId), "0");

        return value;
      }),
    );
  });

  const commit = Effect.fn("Folder.commit")(function* (
    folderId: string,
    path: string,
    input: CommitFile,
    precondition?: Precondition,
  ) {
    const normalized = yield* normalisePath(path);
    const body = yield* Schema.decodeEffect(CommitFile)(input).pipe(Effect.mapError(invalid));

    if (body.size > FILE_LIMIT) return yield* new FileTooLarge({ size: body.size });
    yield* requireFolder(folderId);

    if (body.text !== undefined) {
      const bytes = new TextEncoder().encode(body.text);

      if (
        !body.text.isWellFormed() ||
        body.text.includes("\0") ||
        bytes.length > INLINE_LIMIT ||
        bytes.length !== body.size
      )
        return yield* new InvalidFile({ message: "Invalid inline text" });

      if ((yield* sha256(bytes).pipe(Effect.provideService(Crypto.Crypto, crypto))) !== body.hash)
        return yield* new InvalidFile({ message: "Text hash mismatch" });
    } else {
      if ((yield* blobs.head(body.hash)) !== body.size)
        return yield* new BlobMissing({ hash: body.hash });

      if (body.size <= INLINE_LIMIT) {
        const content = yield* blobs.get(body.hash);
        if (content === undefined) return yield* new BlobMissing({ hash: body.hash });
        const bytes = new Uint8Array(body.size);
        let offset = 0;
        yield* Stream.runForEach(content.body, (chunk) =>
          Effect.sync(() => {
            bytes.set(chunk, offset);
            offset += chunk.length;
          }),
        );
        if (inlineText(bytes) !== undefined)
          return yield* new InvalidFile({ message: "Small UTF-8 content must use inline text" });
      }
    }

    return yield* boundary.withTransaction(
      Effect.gen(function* () {
        const current = yield* load(folderId, normalized);

        if (current?.head.hash === body.hash) return current.head;
        yield* condition(current?.head, precondition);
        const tail = (yield* files.head({ folderId })).nextOffset;
        const encoded = yield* records.read(key(folderId));

        // The counter belongs to the owner, independently of retained stream history.
        const last = Option.isNone(encoded)
          ? 0
          : yield* Schema.decodeEffect(CounterJson)(encoded.value);

        const fields = {
          path: normalized,
          version: last + 1,
          hash: body.hash,
          size: body.size,
          author: body.author,
        };

        const value: File = body.text === undefined ? fields : { ...fields, text: body.text };

        const head: Head = { version: value.version, hash: value.hash, txid: body.txid };

        const changes = DurableState.changes(Files.at({ folderId }), { offset: tail }, [
          DurableState.upsert("file", value, {
            headers: { txid: body.txid, timestamp: DateTime.formatIso(yield* DateTime.now) },
          }),
        ]);

        yield* files.append({ folderId }, changes, { expectedOffset: tail });
        yield* records.write(
          key(folderId, normalized),
          yield* Schema.encodeEffect(StoredHeadJson)({ head, value }),
        );
        yield* records.write(key(folderId), String(value.version));

        return head;
      }),
    );
  });

  const remove = Effect.fn("Folder.delete")(function* (
    folderId: string,
    path: string,
    input: DeleteFile,
    precondition?: Precondition,
  ) {
    const normalized = yield* normalisePath(path);
    const body = yield* Schema.decodeEffect(DeleteFile)(input).pipe(Effect.mapError(invalid));

    if (precondition !== undefined && "absent" in precondition)
      return yield* new InvalidFile({ message: "Delete does not accept If-None-Match" });
    yield* requireFolder(folderId);

    return yield* boundary.withTransaction(
      Effect.gen(function* () {
        const current = yield* load(folderId, normalized);

        if (current === undefined) return {};
        yield* condition(current.head, precondition);
        const tail = (yield* files.head({ folderId })).nextOffset;

        const changes = DurableState.changes(Files.at({ folderId }), { offset: tail }, [
          DurableState.remove("file", normalized, {
            headers: { txid: body.txid, timestamp: DateTime.formatIso(yield* DateTime.now) },
          }),
        ]);

        yield* files.append({ folderId }, changes, { expectedOffset: tail });
        yield* records.remove(key(folderId, normalized));

        return { txid: body.txid };
      }),
    );
  });

  const read = Effect.fn("Folder.read")(function* (folderId: string, path: string) {
    const normalized = yield* normalisePath(path);
    yield* requireFolder(folderId);
    const stored = yield* load(folderId, normalized);

    if (stored === undefined) return yield* new MissingFile({ message: "File not found" });

    return stored;
  });

  return { create, commit, remove, read };
});

export class FolderStore extends Context.Service<FolderStore, Effect.Success<typeof make>>()(
  "FolderSync/FolderStore",
) {}

export const FolderStoreLive = Layer.effect(FolderStore, make);

export const Initialise = Layer.effectDiscard(
  Effect.gen(function* () {
    const folders = yield* Folders;
    yield* folders.create({});
  }),
);
