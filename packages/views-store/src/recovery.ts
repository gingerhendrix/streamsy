import { Effect } from "effect";
import type {
  CheckpointDescriptor,
  JsonValue,
  MaintenanceCommit,
  RowKey,
  SaveCheckpoint,
  StoreError,
  ViewStoreService,
} from "./contracts.ts";
import { ViewCheckpointIncompatible, ViewCursorConflict } from "./errors.ts";

export interface RecoverySource<Item> {
  readonly readAfter: (
    cursor: string | undefined,
  ) => Effect.Effect<
    { readonly items: readonly Item[]; readonly afterExclusiveCursor: string | undefined },
    StoreError
  >;
}
export interface RecoveryFold<Item> {
  readonly fold: (
    state: ReadonlyMap<string, JsonValue>,
    items: readonly Item[],
  ) => Effect.Effect<
    {
      readonly state: ReadonlyMap<string, { readonly key: RowKey; readonly value: JsonValue }>;
      readonly commit: Omit<MaintenanceCommit, "expectedCursor" | "afterExclusiveCursor">;
    },
    StoreError
  >;
}
export interface RecoverOptions<Item> {
  readonly store: ViewStoreService;
  readonly checkpoint: CheckpointDescriptor;
  readonly source: RecoverySource<Item>;
  readonly reducer: RecoveryFold<Item>;
  readonly initialState?: ReadonlyMap<string, JsonValue>;
  readonly saveCheckpoint?: boolean;
  readonly now?: () => number;
}
export interface RecoveryResult {
  readonly checkpointCursor: string | undefined;
  readonly sourceCursor: string | undefined;
  readonly folded: number;
  readonly committed: boolean;
}

/** Backend-neutral checkpoint-plus-after-exclusive-suffix recovery orchestration. */
export const recover = <Item>(
  options: RecoverOptions<Item>,
): Effect.Effect<RecoveryResult, StoreError | ViewCursorConflict | ViewCheckpointIncompatible> =>
  Effect.gen(function* () {
    const checkpoint = yield* options.store.loadCheckpoint(options.checkpoint);
    const state = new Map<string, JsonValue>(options.initialState ?? []);
    for (const entry of checkpoint?.entries ?? [])
      state.set(JSON.stringify(entry.key), entry.value);
    const suffix = yield* options.source.readAfter(checkpoint?.sourceCursor);
    if (suffix.items.length === 0 || suffix.afterExclusiveCursor === undefined)
      return {
        checkpointCursor: checkpoint?.sourceCursor,
        sourceCursor: checkpoint?.sourceCursor,
        folded: 0,
        committed: false,
      };
    const folded = yield* options.reducer.fold(state, suffix.items);
    const expectedCursor = yield* options.store.sourceProgress(options.checkpoint);
    yield* options.store.commit({
      ...folded.commit,
      expectedCursor,
      afterExclusiveCursor: suffix.afterExclusiveCursor,
    });
    if (options.saveCheckpoint === true) {
      const save: SaveCheckpoint = {
        ...options.checkpoint,
        sourceCursor: suffix.afterExclusiveCursor,
        createdAtMs: options.now?.() ?? Date.now(),
        entries: [...folded.state.values()],
      };
      yield* options.store.saveCheckpoint(save);
    }
    return {
      checkpointCursor: checkpoint?.sourceCursor,
      sourceCursor: suffix.afterExclusiveCursor,
      folded: suffix.items.length,
      committed: true,
    };
  });
