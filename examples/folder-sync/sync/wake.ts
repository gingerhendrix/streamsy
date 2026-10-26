import { Streamsy, type StreamId } from "@streamsy/core";
import { Array, Effect, Option } from "effect";
import { Files } from "../server/streams.ts";
import { RemoteFailure } from "./client.ts";
import type { SyncContext } from "./context.ts";

/** The metadata key of the saved wake cursor. */
const WAKE_CURSOR = "wakeCursor";

/** The folders that registry discovery started, and a catch-up of one of them. */
export interface ReadyFolders<E, R> {
  /** Every selected folder whose start has completed. */
  readonly ready: ReadonlySet<string>;
  /**
   * Read one folder from its saved cursor to its tail. It waits for a start in
   * progress, and does nothing for a folder that discovery did not start.
   */
  readonly catchUp: (folderId: string) => Effect.Effect<void, E, R>;
}

/**
 * One live read of the wake stream for the whole mirror, in place of one live
 * read for each folder. Each row names a folder `Files` stream and its tail.
 * A row past the folder's saved cursor catches that folder up. The folder
 * cursors are saved first and the wake cursor last, so a stop between the two
 * reads the same rows again, and the offset check skips the folders already read.
 */
export const waker = <E, R>(context: SyncContext, folders: ReadyFolders<E, R>) => {
  const { state, remote } = context;
  // Long-poll only after a page reached the tail.
  let live = false;

  const catchUpAll = Effect.suspend(() =>
    Effect.forEach([...folders.ready], folders.catchUp, { discard: true }),
  );

  // The streams of a wake snapshot that an earlier page left open.
  let snapshot: ReadonlySet<StreamId> | undefined;

  /** The wake actions of one page, using one read of the saved folder cursors. */
  const actionsOf = Effect.fn("Sync.wakeActions")(function* (
    rows: ReadonlyArray<Streamsy.WakeEvent>,
  ) {
    const cursors = new Map(
      Array.map(
        [...(yield* state.cursors())],
        ([folderId, saved]) => [Files.at({ folderId }).id, saved] as const,
      ),
    );

    return Streamsy.wakeActions(rows, { cursors, snapshot });
  });

  /** Catch up each followed folder that a page moved past its saved cursor. */
  const handle = Effect.fn("Sync.wakeItems")(function* (items: ReadonlyArray<Streamsy.WakeItem>) {
    // A reset drops every row. Each folder read reaches a tail at or past the rows after it.
    if (Array.some(items, Streamsy.WakeItem.$is("Reset"))) return yield* catchUpAll;

    for (const item of items) {
      if (Streamsy.WakeItem.$is("Dirty")(item)) {
        const folderId = Option.getOrUndefined(Files.parse(item.stream))?.folderId;

        // `catchUp` skips a folder that discovery did not start.
        if (folderId !== undefined) yield* folders.catchUp(folderId);
      } else if (Streamsy.WakeItem.$is("Removed")(item)) {
        const folderId = Option.getOrUndefined(Files.parse(item.stream))?.folderId;

        // Folder delete is outside this demo. Keep the mirror and its cursor.
        if (folderId !== undefined)
          yield* Effect.logWarning(`Wake stream removed ${folderId}; the mirror keeps it`);
      }
      // No compaction writes a snapshot yet, and folder delete is outside this demo.
    }

    return yield* Effect.void;
  });

  /** Handle one wake page, then save its cursor. */
  const follow = Effect.fn("Sync.wake")(function* () {
    const offset = (yield* state.get(WAKE_CURSOR)) ?? "-1";
    const page = yield* remote.wake(offset, live);

    if (page.reset) {
      if (offset === "-1")
        return yield* new RemoteFailure({
          message: "Wake stream history is unavailable from the beginning",
          retryable: false,
          status: 410,
        });
      // Rows before the saved cursor are gone. Catch up every folder, then read the wake from -1.
      yield* catchUpAll;
      yield* state.set(WAKE_CURSOR, "-1");
      snapshot = undefined;
      live = false;

      return yield* Effect.void;
    }

    const next = yield* actionsOf(page.rows);
    yield* handle(next.items);
    yield* state.set(WAKE_CURSOR, page.cursor);
    snapshot = next.snapshot;
    live = page.upToDate;

    return yield* Effect.void;
  });

  return { follow };
};
