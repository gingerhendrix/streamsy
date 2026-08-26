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
  type HistoryPosition,
  type JsonValue,
  type StoredChange,
  type StoreError,
  type ViewStoreService,
} from "@streamsy/views-store";
import {
  makeMemoryOutboxBacking,
  outboxStoreLayer,
  OutboxStore,
  type OutboxDraft,
} from "@streamsy/effect-sink";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import { planHash } from "@streamsy/views";
import {
  maintainGraph,
  type OperatorStateSnapshot,
  type SourceChanges,
} from "@streamsy/views-engine";
import type { Change, JsonObject } from "@streamsy/views-ir";
import {
  issueLabelLifecycle,
  issueLabelMemberships,
  issueLifecycle,
  issues,
} from "../domain/declaration.ts";
import { decodeIssueRow, type IssueRow } from "../domain/issue.ts";
import { decodeIssueLabelRow, type IssueLabelRow } from "../domain/issue.ts";
import { decodeLabelCountRow, type LabelCountRow } from "../domain/issue.ts";
import { decodeProjectBoardCard, type ProjectBoardCard } from "../domain/issue.ts";
import { labelCounts, projectBoard } from "../domain/views.ts";
import { decodeCatalogRow, type CatalogCollection, type CatalogRow } from "../domain/catalog.ts";
import type { CommandKind } from "./commands.ts";
import {
  CommandIdConflict,
  MaintenanceFault,
  StoreRestorePoison,
  StoreUnavailable,
  TransitionHistoryExpired,
} from "./errors.ts";
import {
  decodeOperatorSnapshot,
  operatorMaintenanceCommit,
  operatorSnapshotRef,
} from "./operator-store-adapter.ts";

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
  readonly commandKind: CommandKind;
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

/**
 * How far the transition feed has been written, and on which producer sequence.
 *
 * The position is an A4 {@link HistoryPosition} over the committed change
 * history — not a native Durable Streams offset, and not an exchange arrival
 * index. It names a *batch this store committed*, which is exactly what the
 * feed publisher needs to know it has caught up. The two domains never meet:
 * the feed's own consumers resume by native offset, and that offset is never
 * read or written here.
 */
export interface TransitionProgress {
  /** The last committed change batch whose transitions are durably on the feed. */
  readonly position: HistoryPosition | undefined;
  /** The producer sequence the last feed append used. Zero means none yet. */
  readonly sequence: number;
}

/**
 * One maintenance step of an operator-graph product.
 *
 * `revision` is the graph's own committed revision — a counter over operator
 * state, not a source cursor and not a stream offset. It is what tells a
 * publisher whether the rows it holds are the rows the sink already carries.
 */
export interface GraphResult<Row> {
  readonly rows: readonly Row[];
  readonly changes: readonly Change<Row, string>[];
  readonly revision: number;
  readonly previousRevision: number;
}

/** One atomic advance of the maintained membership relation. */
export interface MembershipCommitInput {
  readonly expectedCheckpoint: string | undefined;
  readonly checkpoint: string;
  readonly rows: ReadonlyMap<string, IssueLabelRow>;
  readonly changes: readonly Change<IssueLabelRow, string>[];
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
  /**
   * Record one accepted command and the deliveries it implies, together.
   *
   * The receipt is the application's exactly-once record of an accepted
   * command, so it is also the only place an effect-sink enqueue can be made
   * exactly-once without inventing a second reconciliation mechanism. Both
   * writes land or neither does.
   */
  readonly recordReceipt: (
    receipt: CommandReceipt,
    deliveries?: readonly OutboxDraft[],
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
  readonly maintainBoard: (
    workspaceId: string,
    inputs: readonly SourceChanges[],
  ) => Effect.Effect<GraphResult<ProjectBoardCard>, StoreUnavailable | StoreRestorePoison>;
  readonly boardRows: (
    workspaceId: string,
  ) => Effect.Effect<readonly ProjectBoardCard[], StoreUnavailable | StoreRestorePoison>;
  /** After-exclusive cursor of the membership relation's own source. */
  readonly membershipProgress: (
    workspaceId: string,
  ) => Effect.Effect<string | undefined, StoreUnavailable>;
  readonly membershipStates: (
    workspaceId: string,
    keys: readonly string[],
  ) => Effect.Effect<ReadonlyMap<string, IssueLabelRow>, StoreUnavailable | StoreRestorePoison>;
  readonly membershipRows: (
    workspaceId: string,
  ) => Effect.Effect<readonly IssueLabelRow[], StoreUnavailable | StoreRestorePoison>;
  readonly membershipCommit: (
    workspaceId: string,
    input: MembershipCommitInput,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly maintainLabelCounts: (
    workspaceId: string,
    inputs: readonly SourceChanges[],
  ) => Effect.Effect<GraphResult<LabelCountRow>, StoreUnavailable | StoreRestorePoison>;
  /** The revision of one graph product already durably on its sink, if any. */
  readonly graphPublished: (
    workspaceId: string,
    product: string,
  ) => Effect.Effect<string | undefined, StoreUnavailable>;
  readonly markGraphPublished: (
    workspaceId: string,
    product: string,
    revision: string,
  ) => Effect.Effect<void, StoreUnavailable>;
  readonly labelCountRows: (
    workspaceId: string,
  ) => Effect.Effect<readonly LabelCountRow[], StoreUnavailable | StoreRestorePoison>;
  /**
   * Committed change batches of `issue-tracker.issues` after `position`.
   *
   * This is what makes the transition feed recoverable: the changes were
   * written in the same atomic commit as the rows, so a feed rebuilt from them
   * cannot contain a transition whose row commit did not land.
   */
  readonly committedIssueChanges: (
    workspaceId: string,
    position: HistoryPosition | undefined,
    limit: number,
  ) => Effect.Effect<
    readonly { readonly position: HistoryPosition; readonly changes: readonly StoredChange[] }[],
    StoreUnavailable | TransitionHistoryExpired
  >;
  readonly transitionProgress: (
    workspaceId: string,
  ) => Effect.Effect<TransitionProgress, StoreUnavailable>;
  readonly markTransitionsPublished: (
    workspaceId: string,
    progress: TransitionProgress,
  ) => Effect.Effect<void, StoreUnavailable>;
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
  transitions: TransitionProgress;
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
    deliveries: readonly OutboxDraft[],
  ) => Effect.Effect<void, StoreUnavailable | CommandIdConflict>;
  readonly stateCheckpoint: IssueStoreService["stateCheckpoint"];
  readonly stateRows: IssueStoreService["stateRows"];
  readonly commitState: IssueStoreService["commitState"];
  readonly transitionProgress: IssueStoreService["transitionProgress"];
  readonly markTransitionsPublished: IssueStoreService["markTransitionsPublished"];
  readonly graphPublished: IssueStoreService["graphPublished"];
  readonly markGraphPublished: IssueStoreService["markGraphPublished"];
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
export const memoryLayer = (
  options: MemoryStoreOptions = {},
): Layer.Layer<IssueStore | OutboxStore> => {
  // One backing per store, shared by the receipt boundary and the delivery
  // runtime — the same object, so an enqueue is visible to the next drain.
  const outbox = makeMemoryOutboxBacking();
  const store = Layer.sync(IssueStore, () => {
    const workspaces = new Map<string, WorkspaceMemory>();
    const receipts = new Map<string, CommandReceipt>();
    const stateSources = new Map<string, StateSourceMemory>();
    const graphPublications = new Map<string, string>();
    const viewStore = memoryService(makeMemoryBacking());

    const workspace = (workspaceId: string): WorkspaceMemory => {
      const existing = workspaces.get(workspaceId);
      if (existing !== undefined) return existing;
      const created: WorkspaceMemory = {
        nextSequence: 0,
        transitions: { position: undefined, sequence: 0 },
      };
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
      recordReceipt: (receipt, deliveries) =>
        Effect.gen(function* () {
          const key = `${receipt.workspaceId}\u0000${receipt.commandId}`;
          const existing = receipts.get(key);
          if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(receipt)) {
            return yield* new CommandIdConflict({
              workspaceId: receipt.workspaceId,
              commandId: receipt.commandId,
            });
          }
          // One synchronous step, so the memory host has the same all-or-nothing
          // receipt-and-enqueue boundary the SQLite transaction gives.
          yield* Effect.sync(() => {
            receipts.set(key, receipt);
            if (deliveries.length > 0) outbox.enqueue(deliveries);
          });
          return undefined;
        }),
      graphPublished: (workspaceId, product) =>
        Effect.sync(() => graphPublications.get(`${workspaceId}\u0000${product}`)),
      markGraphPublished: (workspaceId, product, revision) =>
        Effect.sync(() => {
          graphPublications.set(`${workspaceId}\u0000${product}`, revision);
        }),
      transitionProgress: (workspaceId) => Effect.sync(() => workspace(workspaceId).transitions),
      markTransitionsPublished: (workspaceId, progress) =>
        Effect.sync(() => {
          workspace(workspaceId).transitions = progress;
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
  return Layer.merge(store, outboxStoreLayer(outbox));
};

/**
 * One operator-graph product, maintained and read back through the A4 store.
 *
 * The board and the label counts are the same thing twice — a parameterised
 * plan, its own operator state, and a relation of decoded rows — so they are
 * one helper rather than two copies. What differs between them is the plan, the
 * decoder and the input source id, and those are exactly the arguments.
 */
interface GraphProduct<Row> {
  readonly plan: typeof projectBoard.plan;
  readonly name: string;
  readonly hash: string;
  readonly sourceId: string;
  readonly parameters: Readonly<Record<string, JsonValue>>;
  /**
   * The declared schema that turns committed JSON back into a row.
   *
   * This IS the parse boundary the unknown-parameter rule points at: the store
   * holds what the commit wrote, and nothing above it may see a row the schema
   * has not accepted.
   */
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Justified immediately above.
  readonly decode: (value: unknown) => Row;
  readonly table: string;
}

function graphMaintainer<Row>(viewStore: ViewStoreService, product: GraphProduct<Row>) {
  const identity = (workspaceId: string) => ({
    planName: product.name,
    planHash: product.hash,
    partition: workspaceId,
    sourceId: product.sourceId,
  });
  const relation = (workspaceId: string) => ({ ...identity(workspaceId), id: product.name });

  const decodeChange = (change: Change<JsonObject>): Change<Row, string> => {
    const key = Schema.decodeUnknownSync(Schema.String)(change.key);
    if (change.kind === "enter") return { kind: "enter", key, after: product.decode(change.after) };
    if (change.kind === "update") {
      return {
        kind: "update",
        key,
        before: product.decode(change.before),
        after: product.decode(change.after),
      };
    }
    return { kind: "exit", key, before: product.decode(change.before) };
  };

  const rows = Effect.fn("IssueStore.graphRows")(function* (workspaceId: string) {
    const snapshot = yield* mapStoreError(
      "graphRows",
      viewStore.snapshotRows(relation(workspaceId)),
    );
    return yield* Effect.forEach(snapshot.rows, (row) =>
      Effect.try({
        try: () => product.decode(row.value),
        catch: (cause) =>
          new StoreRestorePoison({
            table: product.table,
            key: JSON.stringify(row.key),
            detail: cause instanceof Error ? cause.message : String(cause),
          }),
      }),
    );
  });

  const maintain = Effect.fn("IssueStore.graphMaintain")(function* (
    workspaceId: string,
    inputs: readonly SourceChanges[],
  ) {
    const id = identity(workspaceId);
    const cursor = yield* mapStoreUnavailable("graphProgress", viewStore.sourceProgress(id));
    const stored = yield* mapStoreUnavailable(
      "graphSnapshot",
      viewStore.getOperatorValue(
        operatorSnapshotRef(product.plan, product.hash, workspaceId, id.sourceId),
        "state",
      ),
    );
    const state = decodeOperatorSnapshot(product.plan, stored);
    const reconciled = reconcileSourceInputs(state, inputs);
    const previousRevision = state?.revision ?? 0;
    if (state !== undefined && reconciled.every((input) => input.changes.length === 0)) {
      return {
        rows: yield* rows(workspaceId),
        changes: [],
        revision: previousRevision,
        previousRevision,
      };
    }
    const result = maintainGraph({
      plan: product.plan,
      state,
      parameters: product.parameters,
      inputs: reconciled,
    });
    yield* mapStoreUnavailable(
      "graphCommit",
      viewStore.commit(
        operatorMaintenanceCommit({
          plan: product.plan,
          planHash: product.hash,
          partition: workspaceId,
          sourceId: id.sourceId,
          expectedCursor: cursor,
          afterExclusiveCursor: String(result.state.revision),
          batchId: `revision-${result.state.revision}`,
          committedAtMs: result.state.revision,
          expectedRevision: state?.revision ?? 0,
          patch: result.patch,
          snapshot: result.state,
          relationId: product.name,
          changes: result.changes,
        }),
      ),
    );
    return {
      rows: result.rows.map((row) => product.decode(row.row)),
      changes: result.changes.map(decodeChange),
      revision: result.state.revision,
      previousRevision,
    };
  });

  return { rows, maintain };
}

export function issueStoreAdapter(
  viewStore: ViewStoreService,
  boundary: IssueStoreBoundary,
  preload: MemoryStoreOptions["preload"] = {},
): IssueStoreService {
  const recoveryTaken = new Set<string>();
  const board = graphMaintainer<ProjectBoardCard>(viewStore, {
    plan: projectBoard.plan,
    name: projectBoard.name,
    hash: planHash(projectBoard.plan),
    sourceId: "issue-tracker.enriched-inputs",
    parameters: { projectId: DEFAULT_PROJECT_ID },
    decode: decodeProjectBoardCard,
    table: "project_board",
  });
  const counts = graphMaintainer<LabelCountRow>(viewStore, {
    plan: labelCounts.plan,
    name: labelCounts.name,
    hash: planHash(labelCounts.plan),
    sourceId: "issue-tracker.label-inputs",
    parameters: { projectId: DEFAULT_PROJECT_ID },
    decode: decodeLabelCountRow,
    table: "label_counts",
  });
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
    recordReceipt: (receipt, deliveries = []) => boundary.recordReceipt(receipt, deliveries),
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
    maintainBoard: (workspaceId, inputs) => board.maintain(workspaceId, inputs),
    boardRows: (workspaceId) => board.rows(workspaceId),
    maintainLabelCounts: (workspaceId, inputs) => counts.maintain(workspaceId, inputs),
    labelCountRows: (workspaceId) => counts.rows(workspaceId),

    membershipProgress: Effect.fn("IssueStore.membershipProgress")(function* (workspaceId) {
      return yield* mapStoreUnavailable(
        "membershipProgress",
        viewStore.sourceProgress(membershipIdentity(workspaceId)),
      );
    }),
    membershipStates: Effect.fn("IssueStore.membershipStates")(function* (workspaceId, keys) {
      const restored = new Map<string, IssueLabelRow>();
      for (const key of keys) {
        const value = yield* mapStoreError(
          "membershipStates",
          viewStore.getReducerState(membershipReducerRef(workspaceId), key),
        );
        if (value !== undefined) {
          restored.set(key, yield* decodeStoredMembership("reducer_state", key, value));
        }
      }
      return restored;
    }),
    membershipRows: Effect.fn("IssueStore.membershipRows")(function* (workspaceId) {
      const snapshot = yield* mapStoreError(
        "membershipRows",
        viewStore.snapshotRows(membershipRelationRef(workspaceId)),
      );
      const rows: IssueLabelRow[] = [];
      for (const row of snapshot.rows) {
        rows.push(yield* decodeStoredMembership("view_rows", JSON.stringify(row.key), row.value));
      }
      return rows.toSorted((left, right) => left.membershipId.localeCompare(right.membershipId));
    }),
    membershipCommit: Effect.fn("IssueStore.membershipCommit")(function* (workspaceId, input) {
      yield* mapStoreUnavailable(
        "membershipCommit",
        viewStore.commit(
          {
            identity: membershipIdentity(workspaceId),
            expectedCursor: input.expectedCheckpoint,
            afterExclusiveCursor: input.checkpoint,
            batchId: input.checkpoint,
            committedAtMs: input.rows.size,
            rows: [...input.rows].map(([key, row]) => ({
              kind: "put" as const,
              namespace: membershipRelationRef(workspaceId),
              key,
              value: encodeMembership(row),
            })),
            reducerStates: [...input.rows].map(([key, row]) => ({
              kind: "put" as const,
              namespace: membershipReducerRef(workspaceId),
              key,
              value: encodeMembership(row),
            })),
            changes: input.changes.map(encodeMembershipChange),
          },
          { keepLastBatches: 256 },
        ),
      );
    }),

    committedIssueChanges: Effect.fn("IssueStore.committedIssueChanges")(
      function* (workspaceId, position, limit) {
        const batches = yield* viewStore
          .changesAfter(identity(workspaceId), position, limit, issues.name)
          .pipe(
            Effect.mapError((error) =>
              error._tag === "ViewHistoryExpired"
                ? new TransitionHistoryExpired({
                    workspaceId,
                    detail: `change history no longer reaches ${JSON.stringify(position)}`,
                  })
                : new StoreUnavailable({
                    operation: "committedIssueChanges",
                    detail: JSON.stringify(error),
                  }),
            ),
          );
        return batches.map((batch) => ({ position: batch.position, changes: batch.changes }));
      },
    ),
    transitionProgress: boundary.transitionProgress,
    markTransitionsPublished: boundary.markTransitionsPublished,
    graphPublished: boundary.graphPublished,
    markGraphPublished: boundary.markGraphPublished,
  });
}

function reconcileSourceInputs(
  state: OperatorStateSnapshot | undefined,
  inputs: readonly SourceChanges[],
): readonly SourceChanges[] {
  if (state === undefined) return inputs;
  return inputs.map((input) => {
    const relation = state.relations.find((candidate) => candidate.relationId === input.sourceId);
    const current = new Map(
      (relation?.rows ?? []).map((row) => [JSON.stringify(row.key), row.row] as const),
    );
    const changes: Change<JsonObject>[] = [];
    for (const change of input.changes) {
      const key = JSON.stringify(change.key);
      const before = current.get(key);
      if (change.kind === "exit") {
        if (before !== undefined) changes.push({ kind: "exit", key: change.key, before });
        current.delete(key);
        continue;
      }
      if (before === undefined) {
        changes.push({ kind: "enter", key: change.key, after: change.after });
      } else if (JSON.stringify(before) !== JSON.stringify(change.after)) {
        changes.push({ kind: "update", key: change.key, before, after: change.after });
      }
      current.set(key, change.after);
    }
    return { sourceId: input.sourceId, changes };
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

/** The project both graph products are parameterised at. See §"known limits". */
export const DEFAULT_PROJECT_ID = "streamsy";

const MEMBERSHIP_PLAN_HASH = planHash(issueLabelMemberships.plan);
const membershipIdentity = (workspaceId: string) => ({
  planName: issueLabelMemberships.name,
  planHash: MEMBERSHIP_PLAN_HASH,
  partition: workspaceId,
  sourceId: "issue-tracker.issue-label-events",
});
const membershipRelationRef = (workspaceId: string) => ({
  ...membershipIdentity(workspaceId),
  id: issueLabelMemberships.name,
});
const membershipReducerRef = (workspaceId: string) => ({
  ...membershipIdentity(workspaceId),
  id: issueLabelLifecycle.ref.name,
});

function encodeMembership(row: IssueLabelRow): JsonValue {
  return {
    membershipId: row.membershipId,
    issueId: row.issueId,
    labelId: row.labelId,
    workspaceId: row.workspaceId,
    attached: row.attached,
    updatedAt: row.updatedAt,
  };
}

function encodeMembershipChange(change: Change<IssueLabelRow, string>): StoredChange {
  if (change.kind === "enter") {
    return {
      ...change,
      relationId: issueLabelMemberships.name,
      after: encodeMembership(change.after),
    };
  }
  if (change.kind === "update") {
    return {
      ...change,
      relationId: issueLabelMemberships.name,
      before: encodeMembership(change.before),
      after: encodeMembership(change.after),
    };
  }
  return {
    ...change,
    relationId: issueLabelMemberships.name,
    before: encodeMembership(change.before),
  };
}

const decodeStoredMembership = (table: string, key: string, value: JsonValue) =>
  Effect.try({
    try: () => decodeIssueLabelRow(value),
    catch: (cause) =>
      new StoreRestorePoison({
        table,
        key,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

function encodeRow(row: IssueRow): JsonValue {
  const encoded = {
    issueId: row.issueId,
    workspaceId: row.workspaceId,
    projectId: row.projectId,
    title: row.title,
    status: row.status,
    updatedAt: row.updatedAt,
  };
  // `assigneeId` is an optional key, so the durable value carries it only when
  // the row does. Writing `assigneeId: undefined` instead would give an
  // unassigned row a field, and the schema would then reject its own encoding.
  if (row.assigneeId === undefined) return encoded;
  return { ...encoded, assigneeId: row.assigneeId };
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
