import { CommitBoundary, DurableState } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { Context, DateTime, Effect, Layer, Option, Schema, Semaphore } from "effect";
import { BlobStore } from "./blob-store.ts";
import {
  BlobMissing,
  CommitFile,
  CreateFolder,
  DeleteFile,
  Deleted,
  DeletedFile,
  File,
  FileTooLarge,
  FolderId,
  Head,
  FILE_LIMIT,
  InvalidFile,
  MissingFile,
  Tombstone,
  WriteConflict,
  normalisePath,
} from "./domain.ts";
import { registerFolder } from "./folder-registry.ts";
import { fileOf, folderView, headOf, type FolderView } from "./folder-view.ts";
import { Files, Folders } from "./streams.ts";

/** Save the folder view after this many landed writes, and on the first write in a process. */
export const CHECKPOINT_EVERY = 32;

const invalid = () => new InvalidFile({ message: "Invalid request" });

/** A parent past the folder's last version names a change that does not exist. */
const checkParent = (view: FolderView, parent: number | null) =>
  parent !== null && parent > view.last
    ? Effect.fail(new InvalidFile({ message: "Parent is not a version in this folder" }))
    : Effect.void;

/**
 * Rule 2: a txid in the retry memory already landed. A retry gets the first answer.
 * The memory checks the path and the kind of change, so a txid that a client uses
 * again for another path, or for a delete after a commit, gets `400`.
 */
const recall = (view: FolderView, txid: string, path: string, kind: "commit" | "delete") => {
  const landed = Option.fromUndefinedOr(view.recent.get(txid));

  return Option.exists(
    landed,
    (entry) => entry.path !== path || (entry.hash === null) !== (kind === "delete"),
  )
    ? Effect.fail(new InvalidFile({ message: "This txid was used for another change" }))
    : Effect.succeed(landed);
};

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
   * folds the new tail and decides again. The offset only orders appends. It never
   * refuses a write for being stale.
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
  ) {
    const normalized = yield* normalisePath(path);
    const body = yield* Schema.decodeEffect(CommitFile)(input).pipe(Effect.mapError(invalid));

    if (body.size > FILE_LIMIT) return yield* new FileTooLarge({ size: body.size });
    yield* requireFolder(folderId);

    // The blob store checked the hash on upload. A commit only names stored content.
    if ((yield* blobs.head(body.hash)) !== body.size)
      return yield* new BlobMissing({ hash: body.hash });

    const timestamp = DateTime.formatIso(yield* DateTime.now);

    return yield* decide(folderId, (view) =>
      Effect.gen(function* () {
        yield* checkParent(view, body.parent);
        // Rule 2 comes first. A retry gets its first answer even when the head moved since.
        const landed = Option.getOrUndefined(yield* recall(view, body.txid, normalized, "commit"));

        if (landed !== undefined && landed.hash !== null) {
          const head: Head = {
            version: landed.version,
            parent: landed.parent,
            hash: landed.hash,
            txid: body.txid,
          };

          return { value: head, items: [] };
        }

        // Rule 1: the head already has this content. Nothing changes.
        const current = liveHead(view, normalized);

        if (current?.hash === body.hash) return { value: current, items: [] };

        // Rule 3: append. There is no precondition, so the last append is the head.
        const value: File = {
          path: normalized,
          version: view.last + 1,
          parent: body.parent,
          hash: body.hash,
          size: body.size,
          author: body.author,
        };

        const head: Head = {
          version: value.version,
          parent: value.parent,
          hash: value.hash,
          txid: body.txid,
        };

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
  ) {
    const normalized = yield* normalisePath(path);
    const body = yield* Schema.decodeEffect(DeleteFile)(input).pipe(Effect.mapError(invalid));
    yield* requireFolder(folderId);
    const timestamp = DateTime.formatIso(yield* DateTime.now);

    return yield* decide(folderId, (view) =>
      Effect.gen(function* (): Effect.fn.Return<
        { value: Removed; items: Array<FileChangeItem> },
        InvalidFile
      > {
        yield* checkParent(view, body.parent);
        // Rule 2 comes first: the file that a retry deleted is gone, and rule 1 would hide its answer.
        const landed = Option.getOrUndefined(yield* recall(view, body.txid, normalized, "delete"));

        if (landed !== undefined) {
          const deleted: Removed = {
            version: landed.version,
            parent: landed.parent,
            txid: body.txid,
          };

          return { value: deleted, items: [] };
        }

        // Rule 1: a delete of a path with no live file answers with no txid and appends nothing.
        const absent: Removed = {};

        if (!view.files.has(normalized)) return { value: absent, items: [] };

        const value: Tombstone = {
          path: normalized,
          version: view.last + 1,
          parent: body.parent,
          author: body.author,
          deleted: true,
        };

        const deleted: Removed = { version: value.version, parent: value.parent, txid: body.txid };

        return {
          value: deleted,
          items: [DurableState.upsert("file", value, { headers: { txid: body.txid, timestamp } })],
        };
      }),
    );
  });

  const read = Effect.fn("Folder.read")(function* (folderId: string, path: string) {
    const normalized = yield* normalisePath(path);
    yield* requireFolder(folderId);
    const view = yield* currentView(folderId);
    const live = view.files.get(normalized);

    if (live === undefined) {
      const version = view.removed.get(normalized);

      return yield* version === undefined
        ? new MissingFile({ message: "File not found" })
        : new DeletedFile({ version });
    }

    return { head: headOf(live), value: fileOf(normalized, live) };
  });

  return { create, commit, remove, read };
});

type FileChangeItem = DurableState.Change<typeof Files.collections>;

/** A delete answer. It is empty when nothing was deleted. */
type Removed = Deleted | Record<string, never>;

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
