import { CommitBoundary, DurableState } from "@streamsy/core";
import { DateTime, Effect } from "effect";
import { FolderConflict } from "./domain.ts";
import { applyChanges } from "./messages.ts";
import { Folders, type Folder } from "./streams.ts";

export interface Registration {
  readonly folderId: string;
  readonly title: string;
  readonly txid: string;
}

/**
 * Add one folder to the registry. The fold, the title check and the append run
 * in one transaction, and the append expects the offset that the fold read. A
 * repeated create with the same title answers the registered folder.
 *
 * `beforeAppend` runs in the same transaction, only for a new folder. The Bun
 * host creates the folder's `Files` stream there. The Worker creates it later,
 * in the folder object.
 */
export const registerFolder = Effect.fn("Folder.register")(function* <E = never, R = never>(
  input: Registration,
  beforeAppend: Effect.Effect<void, E, R> = Effect.void,
) {
  const folders = yield* Folders;
  const boundary = yield* CommitBoundary;

  return yield* boundary.withTransaction(
    Effect.gen(function* () {
      const { state, offset } = yield* folders.fold(
        {},
        () => new Map<string, Folder>(),
        (rows, change) => applyChanges(rows, [change]),
      );

      const existing = state.get(input.folderId);

      if (existing !== undefined) {
        if (existing.title !== input.title)
          return yield* new FolderConflict({ message: "Folder title differs" });

        return existing;
      }

      yield* beforeAppend;
      const value: Folder = { folderId: input.folderId, title: input.title };
      const timestamp = DateTime.formatIso(yield* DateTime.now);

      const changes = DurableState.changes(Folders.at({}), { offset }, [
        DurableState.upsert("folder", value, { headers: { txid: input.txid, timestamp } }),
      ]);

      yield* folders.append({}, changes, { expectedOffset: offset });

      return value;
    }),
  );
});
