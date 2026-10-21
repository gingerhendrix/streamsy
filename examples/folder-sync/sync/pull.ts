import { StreamAddress } from "@streamsy/core";
import { Array, Effect, Schema } from "effect";
import type { FileValue } from "../server/domain.ts";
import { applyChanges, isReset, isUpsert, type FileMessage } from "../server/messages.ts";
import { RemoteFailure, type SyncRemote } from "./client.ts";
import { folderPath, type SyncContext } from "./context.ts";
import { historyRewound, ignoredPath } from "./decisions.ts";
import type { Install } from "./install.ts";
import { localFiles } from "./local.ts";

export class HistoryRewound extends Schema.TaggedError<
  HistoryRewound,
  { readonly brand: unique symbol }
>()("HistoryRewound", {
  message: Schema.String,
}) {}

/** The metadata key of a folder's saved cursor. */
const cursorKey = (folderId: string) => `cursor:${folderId}`;

type Page = Effect.Success<ReturnType<(typeof SyncRemote.Service)["page"]>>;

/**
 * Remote reads for one sync run. A folder's cursor is saved only with the
 * changes that it covers, under the gate.
 */
export const puller = (context: SyncContext, { observe, install }: Install) => {
  const { path, state, remote, root, gate, warn } = context;

  /** Stop when the server's tail is behind the saved cursor: its history went backwards. */
  const guardFolder = Effect.fn("Sync.guardFolder")(function* (folderId: string) {
    const saved = yield* state.get(cursorKey(folderId));
    const tail = yield* remote.tail(folderId);

    if (saved !== undefined && historyRewound(saved, tail))
      return yield* new HistoryRewound({
        message: `Server history went backwards: ${folderId} cursor ${saved} is past tail ${tail}. Sync stopped. Restore the server history or use a new --dir.`,
      });

    return saved;
  });

  /** Read to the tail and fold the last change of each path, tombstones too. A 410 reads again from -1. */
  const readReplacement = Effect.fn("Sync.readReplacement")(function* (
    folderId: string,
    from: string,
    first?: Page,
  ) {
    const files = new Map<string, FileValue>();
    let cursor = from;
    let pending = first;

    while (true) {
      const page = pending ?? (yield* remote.page(folderId, cursor, false));
      pending = undefined;

      if (page.reset) {
        if (cursor === "-1")
          return yield* new RemoteFailure({
            message: `Folder history is unavailable from the beginning: ${folderId}`,
            retryable: false,
            status: 410,
          });
        files.clear();
        cursor = "-1";
        continue;
      }

      applyChanges(files, page.rows);

      if (page.cursor === cursor && !page.upToDate)
        return yield* new RemoteFailure({
          message: `Folder read did not advance: ${folderId}`,
          retryable: false,
          status: 0,
        });

      cursor = page.cursor;

      if (page.upToDate) break;
    }

    return { files, cursor };
  });

  /** Every path that a replacement must reconcile: the new state, known heads and `extra`. */
  const replacedPaths = Effect.fn("Sync.replacedPaths")(function* (
    folderId: string,
    files: ReadonlyMap<string, FileValue>,
    extra: Iterable<string> = [],
  ) {
    return new Set([...files.keys(), ...(yield* state.paths(folderId)), ...extra]);
  });

  /** A first sync of a folder. Local files that the server does not know become creates. */
  const initialiseFolder = Effect.fn("Sync.initialiseFolder")(function* (folderId: string) {
    const saved = yield* guardFolder(folderId);

    if (saved !== undefined) return yield* Effect.void;
    const { files, cursor } = yield* readReplacement(folderId, "-1");

    yield* gate.withPermit(
      Effect.gen(function* () {
        const local = yield* localFiles(path.join(root, folderPath(context, folderId)), warn);

        for (const file of yield* replacedPaths(folderId, files, local)) {
          if (ignoredPath(file)) continue;

          // A brand-new local path is a create, not a deletion discovered during replay.
          if (!files.has(file) && (yield* state.head(folderId, file)) === undefined)
            yield* observe(folderId, file);
          else yield* install(folderId, file, files.get(file) ?? null);
        }

        yield* state.set(cursorKey(folderId), cursor);
      }),
    );

    return yield* Effect.void;
  });

  const applyPage = Effect.fn("Sync.applyPage")(function* (
    folderId: string,
    rows: ReadonlyArray<FileMessage>,
    cursor: string,
  ) {
    yield* gate.withPermit(
      Effect.gen(function* () {
        for (const change of rows) {
          if (StreamAddress.isControl(change) || ignoredPath(change.key)) continue;
          yield* observe(folderId, change.key);
          yield* install(folderId, change.key, isUpsert(change) ? change.value : null);
        }

        yield* state.set(cursorKey(folderId), cursor);
      }),
    );
  });

  /** Follow one folder. With `once`, stop at the first up-to-date page. */
  const pullFolder = Effect.fn("Sync.pullFolder")(function* (folderId: string, once = false) {
    let cursor = (yield* state.get(cursorKey(folderId))) ?? "-1";
    let live = false;

    while (true) {
      const page: Page = yield* remote.page(folderId, cursor, live);

      if (page.reset || Array.some(page.rows, isReset)) {
        // A stored reset replaces state at this position. Only 410 reads from -1.
        // Finish the replacement pages before reconciling paths absent from the new state.
        const { files, cursor: replay } = yield* readReplacement(folderId, cursor, page);

        yield* gate.withPermit(
          Effect.gen(function* () {
            for (const file of yield* replacedPaths(folderId, files))
              yield* install(folderId, file, files.get(file) ?? null);
            yield* state.set(cursorKey(folderId), replay);
          }),
        );
        cursor = replay;
        live = false;

        if (once) return;
        continue;
      }

      yield* applyPage(folderId, page.rows, page.cursor);
      cursor = page.cursor;
      live = page.upToDate;

      if (once && page.upToDate) return;
    }
  });

  return { guardFolder, initialiseFolder, pullFolder };
};
