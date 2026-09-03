import { Clock, Effect, Schema } from "effect";
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

const encodeJsonString = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

export interface RecoverySource<Item, SourceError = StoreError> {
  readonly readAfter: (
    cursor: string | undefined,
  ) => Effect.Effect<
    { readonly items: readonly Item[]; readonly afterExclusiveCursor: string | undefined },
    SourceError
  >;
}
export interface RecoveryFold<Item, FoldError = StoreError> {
  readonly fold: (
    state: ReadonlyMap<string, JsonValue>,
    items: readonly Item[],
  ) => Effect.Effect<
    {
      readonly state: ReadonlyMap<string, { readonly key: RowKey; readonly value: JsonValue }>;
      readonly commit: Omit<MaintenanceCommit, "expectedCursor" | "afterExclusiveCursor">;
    },
    FoldError
  >;
}
export interface RecoverOptions<Item, SourceError = StoreError, FoldError = StoreError> {
  readonly store: ViewStoreService;
  readonly checkpoint: CheckpointDescriptor;
  readonly source: RecoverySource<Item, SourceError>;
  readonly reducer: RecoveryFold<Item, FoldError>;
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
export const recover = <Item, SourceError = StoreError, FoldError = StoreError>(
  options: RecoverOptions<Item, SourceError, FoldError>,
): Effect.Effect<
  RecoveryResult,
  StoreError | ViewCursorConflict | ViewCheckpointIncompatible | SourceError | FoldError
> =>
  Effect.gen(function* () {
    const checkpoint = yield* options.store.loadCheckpoint(options.checkpoint);
    const state = new Map<string, JsonValue>(options.initialState ?? []);
    for (const entry of checkpoint?.entries ?? [])
      state.set(encodeJsonString(entry.key), entry.value);
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
      const createdAtMs =
        options.now === undefined ? yield* Clock.currentTimeMillis : options.now();
      const save: SaveCheckpoint = {
        ...options.checkpoint,
        sourceCursor: suffix.afterExclusiveCursor,
        createdAtMs,
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
