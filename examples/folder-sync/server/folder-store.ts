import { CommitBoundary, ZERO_OFFSET, type Decision, type AppendFailure } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { Context, Crypto, DateTime, Effect, Layer, Match, Schema, Stream } from "effect";
import type { SqlError } from "effect/sql/SqlError";
import { BlobStore } from "./blob-store.ts";
import {
  FileOp,
  Identifier,
  InvalidFile,
  MissingFile,
  OperationReceipt,
  VersionUnavailable,
  WriteMetadata,
  headAt,
  normalisePath,
  operationAt,
  overwriteMark,
  overwriteMarkFromOp,
  prepareContent,
} from "./domain.ts";
import { Files, Folders, folderHead } from "./streams.ts";

export type WriteHookError = InvalidFile | AppendFailure | SqlError;

export class WriteHookFailure extends Schema.TaggedError<WriteHookFailure>()("WriteHookFailure", {
  cause: Schema.Defect(),
}) {}

/** Runs after the append, on its owner fiber in the same transaction. A host can capture another stream here. */
export class WriteHook extends Context.Service<
  WriteHook,
  {
    readonly committed: (
      folderId: string,
      operation: FileOp,
      version: string,
    ) => Effect.Effect<void, WriteHookError>;
  }
>()("FolderSync/WriteHook") {}

export const NoWriteHook = Layer.succeed(WriteHook, { committed: () => Effect.void });

const validId = (folderId: string) =>
  Schema.decodeEffect(Identifier)(folderId).pipe(
    Effect.mapError(() => new InvalidFile({ message: "Invalid folder id" })),
  );

const requireFolder = Effect.fn("Folder.require")(function* (folderId: string) {
  yield* validId(folderId);
  const files = yield* Files;

  return yield* files
    .head({ folderId })
    .pipe(Effect.catchTag("StreamNotFound", () => new MissingFile({ message: "Unknown folder" })));
});

const create = Effect.fn("Folder.create")(function* (folderId: string, title: string) {
  yield* validId(folderId);
  yield* Schema.decodeEffect(Identifier)(title).pipe(
    Effect.mapError(() => new InvalidFile({ message: "Invalid folder title" })),
  );
  const folders = yield* Folders;
  const files = yield* Files;
  const boundary = yield* CommitBoundary;
  const created = DateTime.formatIso(yield* DateTime.now);

  return yield* boundary.withTransaction(
    Effect.gen(function* () {
      const existing = (yield* folders.collect({})).items.find(
        (item) => item.folderId === folderId,
      );

      if (existing !== undefined) return existing;
      const folder = { folderId, title, created };
      yield* files.create({ folderId });
      yield* folders.append({}, [folder]);

      return folder;
    }),
  );
});

type WriteDecision = { readonly replay: OperationReceipt } | { readonly operation: FileOp };

const write = Effect.fn("Folder.write")(function* (
  folderId: string,
  path: string,
  metadata: WriteMetadata,
  bytes?: Uint8Array,
) {
  const normalized = yield* normalisePath(path);

  const meta = yield* Schema.decodeEffect(WriteMetadata)(metadata).pipe(
    Effect.mapError(() => new InvalidFile({ message: "Invalid write metadata" })),
  );

  yield* requireFolder(folderId);
  // Immutable content is stored before the authoritative append. A failed write may leave an orphan blob.
  const content = bytes === undefined ? undefined : yield* prepareContent(bytes);

  if (content !== undefined && "blob" in content && bytes !== undefined) {
    const blobs = yield* BlobStore;
    yield* blobs.put(bytes);
  }

  const boundary = yield* CommitBoundary;
  const hook = yield* WriteHook;
  const writtenAt = yield* DateTime.now;

  return yield* boundary.withTransaction(
    Effect.gen(function* () {
      const result = yield* Projection.decide(folderHead.at({ folderId }), {
        input: "input",
        decide: ({
          state,
          offsets,
        }): Effect.Effect<Decision<WriteDecision, FileOp>, InvalidFile | MissingFile> => {
          const replay = operationAt(state.ops, meta.opId);

          if (replay !== undefined) {
            if (replay.path !== normalized)
              return Effect.fail(new InvalidFile({ message: "opId already used" }));

            return Effect.succeed({ value: { replay }, items: [] });
          }

          const head = headAt(state.heads, normalized);

          if (content === undefined && (head === undefined || head.deleted))
            return Effect.fail(new MissingFile({ message: "File not found" }));

          if (meta.base !== null && meta.base > (offsets.input ?? ZERO_OFFSET))
            return Effect.fail(
              new InvalidFile({ message: "Base version is ahead of the folder log" }),
            );

          const common = {
            ...meta,
            path: normalized,
            ...overwriteMark(head, meta.base),
            updatedAt: DateTime.formatIso(writtenAt),
          };

          const operation: FileOp =
            content === undefined
              ? { ...common, type: "delete" }
              : { ...common, type: "put", ...content };

          return Effect.succeed({ value: { operation }, items: [operation] });
        },
      });

      if ("replay" in result.value)
        return {
          version: result.value.replay.version,
          ...overwriteMarkFromOp(result.value.replay),
        };

      if (result.appended)
        yield* hook.committed(folderId, result.value.operation, result.offset).pipe(
          Effect.mapError((error) =>
            Match.value(error).pipe(
              Match.tag("InvalidFile", (invalid) => invalid),
              Match.orElse((cause) => new WriteHookFailure({ cause })),
            ),
          ),
        );

      return { version: result.offset, ...overwriteMarkFromOp(result.value.operation) };
    }),
  );
});

const history = Effect.fn("Folder.history")(function* (folderId: string, path: string) {
  const normalized = yield* normalisePath(path);
  yield* requireFolder(folderId);
  const files = yield* Files;

  return yield* files.read({ folderId }).pipe(
    Stream.runFold(
      () => new Array<{ version: string; operation: FileOp }>(),
      (rows, batch) => {
        // Storage readLimit: 1 preserves the exact offset of every entry.
        for (const operation of batch.items) {
          if (operation.path === normalized) rows.push({ version: batch.nextOffset, operation });
        }

        return rows;
      },
    ),
  );
});

const waitForVersion = Effect.fn("Folder.waitForVersion")(function* (
  folderId: string,
  version: string,
) {
  const files = yield* Files;
  let tail = (yield* requireFolder(folderId)).nextOffset;

  while (tail < version) {
    const page = yield* files.wire.readNext(Files.at({ folderId }).id, { offset: tail });
    tail = page.nextOffset;
  }
});

const read = Effect.fn("Folder.read")(function* (
  folderId: string,
  path: string,
  options: { at?: string; minVersion?: string } = {},
) {
  const normalized = yield* normalisePath(path);
  yield* requireFolder(folderId);

  if (options.minVersion !== undefined)
    yield* waitForVersion(folderId, options.minVersion).pipe(
      Effect.timeoutOrElse({
        duration: "10 seconds",
        orElse: () => new VersionUnavailable({ version: options.minVersion ?? ZERO_OFFSET }),
      }),
    );
  let selected: { version: string; operation: FileOp } | undefined;

  if (options.at !== undefined) {
    selected = (yield* history(folderId, normalized)).find((row) => row.version === options.at);
  } else {
    const boundary = yield* CommitBoundary;
    selected = yield* boundary.withTransaction(
      Effect.gen(function* () {
        const projection = folderHead.at({ folderId });
        yield* Projection.run(projection);

        return headAt((yield* Projection.read(projection)).state.heads, normalized);
      }),
    );
  }

  if (selected === undefined || selected.operation.type === "delete")
    return yield* new MissingFile({
      message:
        options.at !== undefined
          ? "File not found at the requested version"
          : selected?.operation.type === "delete"
            ? "File was deleted"
            : "File not found",
    });
  const operation = selected.operation;

  if ("inline" in operation)
    return {
      version: selected.version,
      operation,
      bytes: new TextEncoder().encode(operation.inline),
    };
  const blobs = yield* BlobStore;
  const bytes = yield* blobs.get(operation.blob);

  if (bytes === undefined) return yield* new MissingFile({ message: "Blob not found" });

  return { version: selected.version, operation, bytes };
});

const make = Effect.gen(function* () {
  const context = yield* Effect.context<
    Files | Folders | CheckpointStore | CommitBoundary | BlobStore | WriteHook | Crypto.Crypto
  >();

  return {
    create: (...args: Parameters<typeof create>) =>
      create(...args).pipe(Effect.provideContext(context)),
    write: (...args: Parameters<typeof write>) =>
      write(...args).pipe(Effect.provideContext(context)),
    read: (...args: Parameters<typeof read>) => read(...args).pipe(Effect.provideContext(context)),
    history: (...args: Parameters<typeof history>) =>
      history(...args).pipe(Effect.provideContext(context)),
  };
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
