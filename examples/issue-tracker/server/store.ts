/**
 * Maintained state, as a service.
 *
 * The adapter presents the Slice 1 application contract over generic
 * `ViewStore` state plus application-owned command bookkeeping:
 *
 * - generic relation rows and reducer state;
 * - generic source progress, change history, and reducer checkpoints;
 * - application publication progress and source sequence allocation;
 * - application command receipts.
 *
 * Row and state are equal values today, because this reducer's state *is* the
 * published row. They are still stored apart: a reducer whose state carries
 * bookkeeping the sink must not publish is the normal case, and collapsing the
 * two now would hide that seam behind a coincidence.
 *
 * Generic JSON values are decoded through the declared Schema on the way out.
 * A durable value that no longer decodes becomes a typed
 * {@link StoreRestorePoison} rather than a row the board would serve — the same
 * law `issue-tracker-projections` established for its State restores.
 */
import {
  makeMemoryBacking,
  memoryService,
  recover as recoverView,
  type Checkpoint,
  type JsonValue,
  type StoredChange,
  type StoreError,
  type ViewStoreService,
} from "@streamsy/views-store";
import { Clock, Context, Effect, Layer } from "effect";
import { planHash } from "@streamsy/views";
import type { Change, JsonObject } from "@streamsy/views-ir";
import { issueLifecycle, issues } from "../domain/declaration.ts";
import { decodeIssueRow, type IssueRow } from "../domain/issue.ts";
import { decodeCatalogRow, type CatalogCollection, type CatalogRow } from "../domain/catalog.ts";
import {
  CommandIdConflict,
  MaintenanceFault,
  StoreRestorePoison,
  StoreUnavailable,
} from "./errors.ts";

/** What the view has consumed, and what the sink has published. */
export interface ViewProgress {
  /** After-exclusive source cursor already folded into `view_rows`. */
  readonly checkpoint: string | undefined;
  /** Source cursor whose changes are durably on the sink's State stream. */
  readonly published: string | undefined;
}

export interface CommandReceipt {
  readonly commandId: string;
  readonly workspaceId: string;
  readonly commandKind: "create-issue" | "change-status";
  readonly targetId: string;
  readonly requestHash: string;
  readonly eventId: string;
  readonly eventSequence: number;
  /** The offset the *original* append received. */
  readonly eventOffset: string;
}

/** One atomic advance of the maintained view. */
export interface CommitInput {
  readonly expectedCheckpoint: string | undefined;
  readonly checkpoint: string;
  /** Final reducer state per touched key; also the maintained row. */
  readonly rows: ReadonlyMap<string, IssueRow>;
  /**
   * One past the highest `sequence` this batch folded.
   *
   * The next command reads it to number its own event, so numbering survives a
   * restart by being derived from durable facts rather than from a counter the
   * process happened to hold.
   */
  readonly nextSequence: number;
  readonly changes: readonly Change<IssueRow, string>[];
}

export interface StateCommitInput {
  readonly checkpoint: string;
  readonly rows: ReadonlyMap<string, CatalogRow>;
}

export interface RecoverySuffix {
  readonly items: readonly JsonObject[];
  readonly cursor: string;
  readonly maxSequence: number;
}
export interface RecoveryFoldResult {
  readonly rows: ReadonlyMap<string, IssueRow>;
  readonly changes: readonly Change<IssueRow, string>[];
}

export interface IssueStoreService {
  readonly progress: (workspaceId: string) => Effect.Effect<ViewProgress, StoreUnavailable>;
  /** Prior reducer state for exactly the keys a batch touches. */
  readonly reducerStates: (
    workspaceId: string,
    keys: readonly string[],
  ) => Effect.Effect<ReadonlyMap<string, IssueRow>, StoreUnavailable | StoreRestorePoison>;
  readonly rows: (
    workspaceId: string,
  ) => Effect.Effect<readonly IssueRow[], StoreUnavailable | StoreRestorePoison>;
  /** Commit rows, reducer state and the source checkpoint together or not at all. */
  readonly commit: (
    workspaceId: string,
    input: CommitInput,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly markPublished: (
    workspaceId: string,
    position: string,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly receipt: (
    workspaceId: string,
    commandId: string,
  ) => Effect.Effect<CommandReceipt | undefined, StoreUnavailable>;
  readonly recordReceipt: (
    receipt: CommandReceipt,
  ) => Effect.Effect<void, StoreUnavailable | CommandIdConflict>;
  /** Next source sequence for a workspace: one past the highest folded event. */
  readonly nextSequence: (workspaceId: string) => Effect.Effect<number, StoreUnavailable>;
  readonly stateCheckpoint: (
    sourceId: string,
    partitionId: string,
  ) => Effect.Effect<string | undefined, StoreUnavailable>;
  readonly stateRows: (
    sourceId: string,
    collection: CatalogCollection,
    partitionId: string,
  ) => Effect.Effect<readonly CatalogRow[], StoreUnavailable | StoreRestorePoison>;
  /** Commit current State rows and their after-exclusive source checkpoint atomically. */
  readonly commitState: (
    sourceId: string,
    partitionId: string,
    input: StateCommitInput,
  ) => Effect.Effect<void, StoreUnavailable>;
  /** The first call in one process returns the latest durable recovery anchor. */
  readonly takeRecoveryCheckpoint: (
    workspaceId: string,
  ) => Effect.Effect<Checkpoint | undefined, StoreUnavailable | StoreRestorePoison>;
  readonly saveCheckpoint: (
    workspaceId: string,
    sourceCursor: string,
  ) => Effect.Effect<void, StoreUnavailable | StoreRestorePoison>;
  readonly recoverSuffix: (
    workspaceId: string,
    suffix: RecoverySuffix,
    fold: (
      current: ReadonlyMap<string, IssueRow>,
      items: readonly JsonObject[],
    ) => Effect.Effect<RecoveryFoldResult, MaintenanceFault>,
  ) => Effect.Effect<RecoveryFoldResult, StoreUnavailable | StoreRestorePoison | MaintenanceFault>;
}

export class IssueStore extends Context.Service<IssueStore, IssueStoreService>()(
  "issue-tracker/IssueStore",
) {}

/** Decode one durable JSON value into a maintained row, or fail with typed poison. */
export const restoreRow = (
  table: string,
  key: string,
  json: string,
): Effect.Effect<IssueRow, StoreRestorePoison> =>
  Effect.try({
    try: () => decodeIssueRow(JSON.parse(json)),
    catch: (cause) =>
      new StoreRestorePoison({
        table,
        key,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

interface WorkspaceMemory {
  published?: string;
  nextSequence: number;
}

interface StateSourceMemory {
  readonly rows: Map<string, string>;
  checkpoint?: string;
}

export interface IssueStoreBoundary {
  readonly progress: (
    workspaceId: string,
  ) => Effect.Effect<{ published?: string; nextSequence: number }, StoreUnavailable>;
  readonly markPublished: (
    workspaceId: string,
    position: string,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly updateNextSequence: (
    workspaceId: string,
    nextSequence: number,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly receipt: (
    workspaceId: string,
    commandId: string,
  ) => Effect.Effect<CommandReceipt | undefined, StoreUnavailable>;
  readonly recordReceipt: (
    receipt: CommandReceipt,
  ) => Effect.Effect<void, StoreUnavailable | CommandIdConflict>;
  readonly stateCheckpoint: IssueStoreService["stateCheckpoint"];
  readonly stateRows: IssueStoreService["stateRows"];
  readonly commitState: IssueStoreService["commitState"];
}

export interface MemoryStoreOptions {
  /**
   * Durable values to start from, as raw JSON text.
   *
   * Tests use this to plant a malformed row and prove the restore path fails
   * typed instead of serving it.
   */
  readonly preload?: Readonly<Record<string, Readonly<Record<string, string>>>>;
}

/**
 * The in-memory store.
 *
 * It is not a stub: it implements the same commit ordering and the same typed
 * restore path as SQLite, so the declaration runs unchanged on both.
 */
export const memoryLayer = (options: MemoryStoreOptions = {}): Layer.Layer<IssueStore> =>
  Layer.sync(IssueStore, () => {
    const workspaces = new Map<string, WorkspaceMemory>();
    const receipts = new Map<string, CommandReceipt>();
    const stateSources = new Map<string, StateSourceMemory>();
    const viewStore = memoryService(makeMemoryBacking());

    const workspace = (workspaceId: string): WorkspaceMemory => {
      const existing = workspaces.get(workspaceId);
      if (existing !== undefined) return existing;
      const created: WorkspaceMemory = { nextSequence: 0 };
      workspaces.set(workspaceId, created);
      return created;
    };
    const stateSource = (sourceId: string, partitionId: string): StateSourceMemory => {
      const id = `${sourceId}\u0000${partitionId}`;
      const existing = stateSources.get(id);
      if (existing !== undefined) return existing;
      const created: StateSourceMemory = { rows: new Map() };
      stateSources.set(id, created);
      return created;
    };

    const boundary: IssueStoreBoundary = {
      progress: (workspaceId) =>
        Effect.sync(() => ({
          published: workspace(workspaceId).published,
          nextSequence: workspace(workspaceId).nextSequence,
        })),
      markPublished: (workspaceId, position) =>
        Effect.sync(() => {
          workspace(workspaceId).published = position;
        }),
      updateNextSequence: (workspaceId, nextSequence) =>
        Effect.sync(() => {
          const state = workspace(workspaceId);
          state.nextSequence = Math.max(state.nextSequence, nextSequence);
        }),
      receipt: (workspaceId, commandId) =>
        Effect.sync(() => receipts.get(`${workspaceId}\u0000${commandId}`)),
      recordReceipt: (receipt) =>
        Effect.gen(function* () {
          const key = `${receipt.workspaceId}\u0000${receipt.commandId}`;
          const existing = receipts.get(key);
          if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(receipt)) {
            return yield* new CommandIdConflict({
              workspaceId: receipt.workspaceId,
              commandId: receipt.commandId,
            });
          }
          receipts.set(key, receipt);
          return undefined;
        }),
      stateCheckpoint: (sourceId, partitionId) =>
        Effect.sync(() => stateSource(sourceId, partitionId).checkpoint),
      stateRows: (sourceId, collection, partitionId) =>
        Effect.gen(function* () {
          const restored: CatalogRow[] = [];
          for (const [key, json] of [...stateSource(sourceId, partitionId).rows].toSorted(
            ([left], [right]) => left.localeCompare(right),
          )) {
            const decoded = yield* Effect.try({
              try: () => decodeCatalogRow(collection, JSON.parse(json)).row,
              catch: (cause) =>
                new StoreRestorePoison({
                  table: "source_state_rows",
                  key,
                  detail: cause instanceof Error ? cause.message : String(cause),
                }),
            });
            restored.push(decoded);
          }
          return restored;
        }),
      commitState: (sourceId, partitionId, input) =>
        Effect.sync(() => {
          const state = stateSource(sourceId, partitionId);
          for (const [key, row] of input.rows) state.rows.set(key, JSON.stringify(row));
          state.checkpoint = input.checkpoint;
        }),
    };
    return issueStoreAdapter(viewStore, boundary, options.preload);
  });

export function issueStoreAdapter(
  viewStore: ViewStoreService,
  boundary: IssueStoreBoundary,
  preload: MemoryStoreOptions["preload"] = {},
): IssueStoreService {
  const recoveryTaken = new Set<string>();
  return IssueStore.of({
    progress: Effect.fn("IssueStore.progress")(function* (workspaceId: string) {
      const checkpoint = yield* mapStoreUnavailable(
        "progress",
        viewStore.sourceProgress(identity(workspaceId)),
      );
      const app = yield* boundary.progress(workspaceId);
      return { checkpoint, published: app.published };
    }),
    reducerStates: Effect.fn("IssueStore.reducerStates")(function* (workspaceId, keys) {
      const restored = new Map<string, IssueRow>();
      for (const key of keys) {
        const planted = preload?.[workspaceId]?.[key];
        if (planted !== undefined) {
          restored.set(key, yield* restoreRow("reducer_state", key, planted));
          continue;
        }
        const value = yield* mapStoreError(
          "reducerStates",
          viewStore.getReducerState(reducerRef(workspaceId), key),
        );
        if (value !== undefined)
          restored.set(key, yield* decodeStoredRow("reducer_state", key, value));
      }
      return restored;
    }),
    rows: Effect.fn("IssueStore.rows")(function* (workspaceId) {
      const snapshot = yield* mapStoreError(
        "rows",
        viewStore.snapshotRows(relationRef(workspaceId)),
      );
      const rows: IssueRow[] = [];
      for (const row of snapshot.rows)
        rows.push(yield* decodeStoredRow("view_rows", JSON.stringify(row.key), row.value));
      for (const [key, json] of Object.entries(preload?.[workspaceId] ?? {})) {
        if (!snapshot.rows.some((row) => row.key === key))
          rows.push(yield* restoreRow("view_rows", key, json));
      }
      return rows.toSorted((left, right) => left.issueId.localeCompare(right.issueId));
    }),
    commit: Effect.fn("IssueStore.commit")(function* (workspaceId, input) {
      yield* mapStoreUnavailable(
        "commit",
        viewStore.commit(
          {
            identity: identity(workspaceId),
            expectedCursor: input.expectedCheckpoint,
            afterExclusiveCursor: input.checkpoint,
            batchId: input.checkpoint,
            committedAtMs: input.nextSequence,
            rows: [...input.rows].map(([key, row]) => ({
              kind: "put" as const,
              namespace: relationRef(workspaceId),
              key,
              value: encodeRow(row),
            })),
            reducerStates: [...input.rows].map(([key, row]) => ({
              kind: "put" as const,
              namespace: reducerRef(workspaceId),
              key,
              value: encodeRow(row),
            })),
            changes: input.changes.map(encodeChange),
          },
          { keepLastBatches: 256 },
        ),
      );
      yield* boundary.updateNextSequence(workspaceId, input.nextSequence);
    }),
    markPublished: boundary.markPublished,
    receipt: boundary.receipt,
    recordReceipt: boundary.recordReceipt,
    nextSequence: Effect.fn("IssueStore.nextSequence")(function* (workspaceId) {
      return (yield* boundary.progress(workspaceId)).nextSequence;
    }),
    takeRecoveryCheckpoint: Effect.fn("IssueStore.takeRecoveryCheckpoint")(function* (workspaceId) {
      if (recoveryTaken.has(workspaceId)) return undefined;
      recoveryTaken.add(workspaceId);
      return yield* mapStoreError(
        "loadCheckpoint",
        viewStore.loadCheckpoint(checkpointDescriptor(workspaceId)),
      );
    }),
    saveCheckpoint: Effect.fn("IssueStore.saveCheckpoint")(function* (workspaceId, sourceCursor) {
      const snapshot = yield* mapStoreError(
        "checkpointRows",
        viewStore.snapshotRows(relationRef(workspaceId)),
      );
      const createdAtMs = yield* Clock.currentTimeMillis;
      yield* mapStoreError(
        "saveCheckpoint",
        viewStore.saveCheckpoint({
          ...checkpointDescriptor(workspaceId),
          sourceCursor,
          createdAtMs,
          entries: snapshot.rows,
        }),
      );
    }),
    stateCheckpoint: boundary.stateCheckpoint,
    stateRows: boundary.stateRows,
    commitState: boundary.commitState,
    recoverSuffix: Effect.fn("IssueStore.recoverSuffix")(function* (workspaceId, suffix, fold) {
      let result: RecoveryFoldResult | undefined;
      yield* recoverView<JsonObject, never, MaintenanceFault | StoreRestorePoison>({
        store: viewStore,
        checkpoint: checkpointDescriptor(workspaceId),
        source: {
          readAfter: () =>
            Effect.succeed({
              items: suffix.items,
              afterExclusiveCursor: suffix.cursor,
            }),
        },
        reducer: {
          fold: (state, items) =>
            Effect.gen(function* () {
              const current = new Map<string, IssueRow>();
              for (const value of state.values()) {
                const row = yield* decodeStoredRow("checkpoint_entries", "recovery", value);
                current.set(row.issueId, row);
              }
              const folded = yield* fold(current, items);
              result = folded;
              return {
                state: new Map(
                  [...folded.rows].map(([key, row]) => [key, { key, value: encodeRow(row) }]),
                ),
                commit: {
                  identity: identity(workspaceId),
                  batchId: suffix.cursor,
                  committedAtMs: suffix.maxSequence + 1,
                  rows: [...folded.rows].map(([key, row]) => ({
                    kind: "put" as const,
                    namespace: relationRef(workspaceId),
                    key,
                    value: encodeRow(row),
                  })),
                  reducerStates: [...folded.rows].map(([key, row]) => ({
                    kind: "put" as const,
                    namespace: reducerRef(workspaceId),
                    key,
                    value: encodeRow(row),
                  })),
                  changes: folded.changes.map(encodeChange),
                },
              };
            }),
        },
      }).pipe(Effect.mapError(recoveryError("recoverSuffix")));
      yield* boundary.updateNextSequence(workspaceId, suffix.maxSequence + 1);
      if (result === undefined) return yield* Effect.die("recovery folded no result");
      return result;
    }),
  });
}

const PLAN_HASH = planHash(issues.plan);
const identity = (workspaceId: string) => ({
  planName: issues.name,
  planHash: PLAN_HASH,
  partition: workspaceId,
  sourceId: "issue-tracker.issue-events",
});
const relationRef = (workspaceId: string) => ({ ...identity(workspaceId), id: issues.name });
const reducerRef = (workspaceId: string) => ({
  ...identity(workspaceId),
  id: issueLifecycle.ref.name,
});
const checkpointDescriptor = (workspaceId: string) => ({
  ...identity(workspaceId),
  reducerId: issueLifecycle.ref.name,
  reducerVersion: issueLifecycle.ref.version,
});

function encodeRow(row: IssueRow): JsonValue {
  return {
    issueId: row.issueId,
    workspaceId: row.workspaceId,
    projectId: row.projectId,
    title: row.title,
    status: row.status,
    updatedAt: row.updatedAt,
  };
}
function encodeChange(change: Change<IssueRow, string>): StoredChange {
  if (change.kind === "enter")
    return { ...change, relationId: issues.name, after: encodeRow(change.after) };
  if (change.kind === "update")
    return {
      ...change,
      relationId: issues.name,
      before: encodeRow(change.before),
      after: encodeRow(change.after),
    };
  return { ...change, relationId: issues.name, before: encodeRow(change.before) };
}
const decodeStoredRow = (table: string, key: string, value: JsonValue) =>
  Effect.try({
    try: () => decodeIssueRow(value),
    catch: (cause) =>
      new StoreRestorePoison({
        table,
        key,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });
const mapStoreError = <A>(operation: string, effect: Effect.Effect<A, StoreError>) =>
  effect.pipe(
    Effect.mapError((error) =>
      error._tag === "ViewStateRestorePoison"
        ? new StoreRestorePoison({ table: error.table, key: error.key, detail: error.detail })
        : new StoreUnavailable({ operation, detail: JSON.stringify(error) }),
    ),
  );
const mapStoreUnavailable = <A>(operation: string, effect: Effect.Effect<A, StoreError>) =>
  effect.pipe(
    Effect.mapError((error) => new StoreUnavailable({ operation, detail: JSON.stringify(error) })),
  );

const recoveryError =
  (operation: string) => (error: StoreError | MaintenanceFault | StoreRestorePoison) => {
    if (error instanceof MaintenanceFault || error instanceof StoreRestorePoison) return error;
    return error._tag === "ViewStateRestorePoison"
      ? new StoreRestorePoison({ table: error.table, key: error.key, detail: error.detail })
      : new StoreUnavailable({ operation, detail: JSON.stringify(error) });
  };
