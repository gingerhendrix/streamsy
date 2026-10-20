import { CommitBoundary, DurableState, StreamAddress } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { Context, Crypto, DateTime, Effect, Layer, Option, Schema, Semaphore } from "effect";
import { BlobStore } from "./blob-store.ts";
import { collectBytes } from "./bytes.ts";
import {
  BlobMissing,
  CommitFile,
  CreateFolder,
  DeleteFile,
  File,
  FileTooLarge,
  FolderId,
  Head,
  INLINE_LIMIT,
  FILE_LIMIT,
  InvalidFile,
  MissingFile,
  Precondition,
  PreconditionFailed,
  WriteConflict,
  normalisePath,
  inlineText,
  sha256,
} from "./domain.ts";
import { registerFolder } from "./folder-registry.ts";
import {
  fileOf,
  folderView,
  headOf,
  STREAM_START,
  type FolderView,
  type LiveFile,
} from "./folder-view.ts";
import { FileMessage, isUpsert } from "./messages.ts";
import { Files, Folders } from "./streams.ts";

/** Save the folder view after this many landed writes, and on the first write in a process. */
export const CHECKPOINT_EVERY = 32;

const invalid = () => new InvalidFile({ message: "Invalid request" });

const condition = (head: Head | undefined, precondition: Precondition) =>
  Precondition.$match(precondition, {
    Unconditional: () => true,
    IfMatch: ({ version }) => head?.version === version,
    IfAbsent: () => head === undefined,
  })
    ? Effect.void
    : Effect.fail(new PreconditionFailed({ head: head ?? null }));

const liveHead = (view: FolderView, path: string) => {
  const live = view.files.get(path);

  return live === undefined ? undefined : headOf(live);
};

/** Initialise only the folder owner's stream. Registry ownership may be remote. */
export const initialiseFolder = Effect.fn("Folder.initialise")(function* (folderId: string) {
  yield* Schema.decodeEffect(FolderId)(folderId).pipe(Effect.mapError(invalid));
  const files = yield* Files;
  // The view folds versions from the stream, so a repeated create cannot reset them.
  yield* files.create({ folderId });
});

const make = Effect.gen(function* () {
  const files = yield* Files;
  const folders = yield* Folders;
  const boundary = yield* CommitBoundary;
  const blobs = yield* BlobStore;
  const crypto = yield* Crypto.Crypto;
  // The view reads the Files stream and the checkpoint at each call.
  const context = yield* Effect.context<CheckpointStore | Files>();
  // One save at a time for each folder. A long save for one folder does not skip a save for another.
  const checkpointing = new Map<string, Semaphore.Semaphore>();
  // Writes since this process saved each folder's view. A missing folder has no save yet.
  const unsaved = new Map<string, number>();

  const requireFolder = Effect.fn("Folder.require")(function* (folderId: string) {
    yield* Schema.decodeEffect(FolderId)(folderId).pipe(Effect.mapError(invalid));

    return yield* files
      .head({ folderId })
      .pipe(
        Effect.catchTag("StreamNotFound", () => new MissingFile({ message: "Unknown folder" })),
      );
  });

  /**
   * Fold the view, decide, and append at the offset that was read. A lost race
   * folds the new tail and decides again. A precondition that held before can then fail.
   */
  const decide = <A, E>(
    folderId: string,
    decision: (view: FolderView) => Effect.Effect<{ value: A; items: Array<FileChangeItem> }, E>,
  ) =>
    Projection.decide(folderView.at({ folderId }), {
      input: "files",
      decide: ({ state, offsets }) =>
        decision(state).pipe(
          Effect.map(({ value, items }) => ({
            value,
            items:
              items.length === 0
                ? []
                : DurableState.changes(Files.at({ folderId }), { offset: offsets.files }, items),
          })),
        ),
    }).pipe(
      Effect.catchTag("OffsetMismatch", () => Effect.fail(new WriteConflict({ folderId }))),
      Effect.tap(({ appended }) => (appended ? checkpoint(folderId) : Effect.void)),
      Effect.map(({ value }) => value),
      Effect.provideContext(context),
    );

  /** A lagging checkpoint is safe, so a failed or skipped save only costs a longer fold. */
  const checkpoint = Effect.fn("Folder.checkpoint")(
    function* (folderId: string) {
      const count = (unsaved.get(folderId) ?? CHECKPOINT_EVERY - 1) + 1;
      unsaved.set(folderId, count);

      if (count < CHECKPOINT_EVERY) return;

      // One writer per checkpoint key. A save that is running already covers this write.
      // The count restarts before the save, so a save that fails runs again after
      // CHECKPOINT_EVERY more writes, not on each write.
      let gate = checkpointing.get(folderId);

      if (gate === undefined) {
        gate = Semaphore.makeUnsafe(1);
        checkpointing.set(folderId, gate);
      }

      yield* gate.withPermitsIfAvailable(1)(
        Effect.suspend(() => {
          unsaved.set(folderId, 0);

          return Projection.run(folderView.at({ folderId }));
        }),
      );
    },
    Effect.catch((error) => Effect.logWarning("Folder view checkpoint failed", error)),
  );

  const currentView = (folderId: string) =>
    Projection.current(folderView.at({ folderId })).pipe(
      Effect.map(({ state }) => state),
      Effect.provideContext(context),
    );

  /**
   * The inline text of a live file, read from the change that wrote it. The scan
   * starts at the change's own offset and decodes one message at a time, so it
   * usually decodes one change. A stream copied by replay keeps the offsets of
   * its source, so a scan that misses starts again at the start of the stream.
   */
  const inlineTextOf = Effect.fn("Folder.inlineText")(function* (
    folderId: string,
    path: string,
    live: LiveFile,
  ) {
    const address = StreamAddress.json(Files.at({ folderId }).id, { schema: FileMessage });
    const decodeChange = Schema.decodeEffect(address.codec);
    const decoder = new TextDecoder();

    const scan = Effect.fn("Folder.scanText")(function* (from: string) {
      let offset = from;

      while (true) {
        const page = yield* files.wire.read(address.id, { offset });

        for (const message of page.messages) {
          const change = yield* decodeChange(decoder.decode(message.data));

          if (isUpsert(change) && change.key === path && change.value.version === live.version)
            return Option.fromUndefinedOr(change.value.text);
        }

        if (page.upToDate || page.messages.length === 0) return Option.none<string>();
        offset = page.nextOffset;
      }
    });

    const hinted =
      live.offset === STREAM_START
        ? Option.none<string>()
        : yield* scan(live.offset).pipe(Effect.orElseSucceed(() => Option.none<string>()));

    const text = Option.isSome(hinted) ? hinted : yield* scan(STREAM_START);

    if (Option.isNone(text))
      return yield* Effect.die(new Error(`The Files stream has no text for ${path}`));

    return text.value;
  });

  const create = Effect.fn("Folder.create")(function* (
    folderId: string,
    title: string,
    txid: string,
  ) {
    const input = yield* Schema.decodeEffect(CreateFolder)({ folderId, title, txid }).pipe(
      Effect.mapError(invalid),
    );

    // One host owns the registry and the folder, so the stream and the registration commit together.
    return yield* registerFolder(input, initialiseFolder(folderId)).pipe(
      Effect.provideService(Files, files),
      Effect.provideService(Folders, folders),
      Effect.provideService(CommitBoundary, boundary),
    );
  });

  const commit = Effect.fn("Folder.commit")(function* (
    folderId: string,
    path: string,
    input: CommitFile,
    precondition: Precondition = Precondition.Unconditional(),
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

        const bytes = yield* collectBytes(
          content.body,
          INLINE_LIMIT,
          (size) => new FileTooLarge({ size }),
        );

        if (inlineText(bytes) !== undefined)
          return yield* new InvalidFile({ message: "Small UTF-8 content must use inline text" });
      }
    }

    const timestamp = DateTime.formatIso(yield* DateTime.now);

    return yield* decide(folderId, (view) =>
      Effect.gen(function* () {
        const current = liveHead(view, normalized);

        if (current?.hash === body.hash) return { value: current, items: [] };
        yield* condition(current, precondition);

        const fields = {
          path: normalized,
          version: view.last + 1,
          hash: body.hash,
          size: body.size,
          author: body.author,
        };

        const value: File = body.text === undefined ? fields : { ...fields, text: body.text };

        const head: Head = { version: value.version, hash: value.hash, txid: body.txid };

        return {
          value: head,
          items: [DurableState.upsert("file", value, { headers: { txid: body.txid, timestamp } })],
        };
      }),
    );
  });

  const remove = Effect.fn("Folder.delete")(function* (
    folderId: string,
    path: string,
    input: DeleteFile,
    precondition: Precondition = Precondition.Unconditional(),
  ) {
    const normalized = yield* normalisePath(path);
    const body = yield* Schema.decodeEffect(DeleteFile)(input).pipe(Effect.mapError(invalid));

    if (Precondition.$is("IfAbsent")(precondition))
      return yield* new InvalidFile({ message: "Delete does not accept If-None-Match" });
    yield* requireFolder(folderId);
    const timestamp = DateTime.formatIso(yield* DateTime.now);

    return yield* decide(folderId, (view) =>
      Effect.gen(function* () {
        const current = liveHead(view, normalized);
        // A delete of a missing path answers with no txid and appends nothing (D12).
        const absent: Removed = {};

        if (current === undefined) return { value: absent, items: [] };
        yield* condition(current, precondition);

        return {
          value: { txid: body.txid },
          items: [
            DurableState.remove("file", normalized, { headers: { txid: body.txid, timestamp } }),
          ],
        };
      }),
    );
  });

  const read = Effect.fn("Folder.read")(function* (folderId: string, path: string) {
    const normalized = yield* normalisePath(path);
    yield* requireFolder(folderId);
    const live = (yield* currentView(folderId)).files.get(normalized);

    if (live === undefined) return yield* new MissingFile({ message: "File not found" });
    const value = fileOf(normalized, live);

    if (!live.inline) return { head: headOf(live), value };

    return {
      head: headOf(live),
      value: { ...value, text: yield* inlineTextOf(folderId, normalized, live) },
    };
  });

  return { create, commit, remove, read };
});

type FileChangeItem = DurableState.Change<typeof Files.collections>;

/** A delete answer. It has no txid when nothing was deleted. */
interface Removed {
  readonly txid?: string;
}

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
