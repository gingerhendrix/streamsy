/**
 * One maintenance pass: source suffix → decode → engine → commit → publish.
 *
 * The pass is the whole executable path between a durable fact and a public
 * product, and it is deliberately one function. Reading resumes from the
 * committed checkpoint, so a restart continues where the last commit left off
 * and no event is folded twice. Publication is tracked separately from the
 * checkpoint, because a process can die between committing rows and appending
 * them to the sink — and when it does, the durable rows are the authority and
 * the sink is rebuilt from them.
 */
import type { JsonValue, ReadStreamOptions } from "@streamsy/core";
import { ReadStreams } from "@streamsy/streams";
import { Effect, Schema } from "effect";
import { catalogRow, type CatalogCollection } from "../../domain/catalog.ts";
import {
  boardIssues,
  boardLabelCounts,
  issueLabelLifecycle,
  issueLabelMemberships,
  issueLifecycle,
  issues,
  labels,
  projects,
  users,
} from "../../domain/declaration.ts";
import {
  decodeIssueEvent,
  decodeIssueLabelEvent,
  decodeIssueLabelRow,
  decodeIssueRow,
  type IssueLabelRow,
  type IssueRow,
} from "../../domain/issue.ts";
import type { Change, JsonObject } from "@streamsy/views/ir";
import type { SourceChanges } from "@streamsy/views/engine";
import { maintain, ReducerFault, touchedKeys } from "../../views/engine.ts";
import {
  AppendRejected,
  MaintenanceFault,
  SourcePoison,
  StoreRestorePoison,
  StreamUnavailable,
} from "../errors.ts";
import { IssueSink } from "../publication/sink.ts";
import {
  IssueStore,
  type GraphHistoryLeg,
  type GraphInputPositions,
  type GraphProductId,
  type GraphResult,
} from "../persistence/store.ts";
import { catchUpStateSource, type StateIngestionReport } from "./state-ingestion.ts";
import { publishTransitions } from "../publication/transitions.ts";
import { Streams, type WorkspaceBindings } from "../transport/streams.ts";

export interface MaintenanceReport {
  readonly workspaceId: string;
  /** After-exclusive source cursor now committed. */
  readonly checkpoint: string | undefined;
  readonly folded: number;
  readonly changes: readonly Change<IssueRow, string>[];
  /**
   * How the board State sink was brought up to the board graph's committed
   * revision by this pass.
   *
   * It reports the *sink*, not the source: `none` means the sink already
   * carried the revision the graph now holds, `changes` that the graph's own
   * deltas were appended, and `snapshot` that the sink was rebuilt from the
   * committed rows. A pass that folds no issue event can still report `changes`
   * or `snapshot`, because a project or user rename moves the board rows
   * without moving the issue checkpoint. Conversely `none` never means "the
   * work was skipped": it means the sink is already authoritative.
   */
  readonly publication: "none" | "changes" | "snapshot";
  /**
   * What the joined catalog collections ingested during this pass.
   *
   * The pass owns the catalog catch-up because both graph products join it, so
   * it is also the only thing that can report what was folded. A reader that
   * did its own catch-up afterwards would always report zero — the pass would
   * already have consumed the suffix.
   */
  readonly catalog: readonly StateIngestionReport[];
  /** Transitions appended to the declared feed by this pass. */
  readonly transitions: number;
}

/**
 * Bring `issue-tracker.issues` up to the durable tail of its source, then bring
 * the sink up to the committed rows.
 */
export const advance = Effect.fn("Maintenance.advance")(function* (workspaceId: string) {
  const store = yield* IssueStore;
  const sink = yield* IssueSink;

  const before = yield* store.progress(workspaceId);
  const recovery = yield* store.takeRecoveryCheckpoint(workspaceId);
  const suffix = yield* readSuffix(
    (bindings) => bindings.issueEvents(workspaceId),
    recovery?.sourceCursor ?? before.checkpoint,
  );

  let checkpoint = before.checkpoint;
  let changes: readonly Change<IssueRow, string>[] = [];

  if (suffix.items.length > 0) {
    const result =
      recovery === undefined
        ? yield* Effect.gen(function* () {
            const keys = touchedKeys(issues.plan, suffix.items);
            const current = yield* store.reducerStates(workspaceId, keys);
            const folded = yield* fold(current, suffix.items);
            yield* store.commit(workspaceId, {
              expectedCheckpoint: before.checkpoint,
              checkpoint: suffix.cursor,
              rows: folded.rows,
              nextSequence: suffix.maxSequence + 1,
              changes: folded.changes,
            });
            return folded;
          })
        : yield* store.recoverSuffix(workspaceId, suffix, fold);
    changes = result.changes;
    checkpoint = suffix.cursor;
    if ((suffix.maxSequence + 1) % 2 === 0) {
      yield* store.saveCheckpoint(workspaceId, suffix.cursor);
    }
  }

  /**
   * The membership relation folds from its own canonical stream, on its own
   * checkpoint. It is a second fact family rather than a second reader of the
   * first, so its progress is independent: a crash between the two commits
   * leaves each one resuming from what it actually folded.
   */
  yield* advanceMemberships(workspaceId);

  /**
   * The transition feed is brought up to the *committed* change history, which
   * is written in the same atomic commit as the rows. That is what makes a
   * crash between the two survivable: the batch is still owed and the next pass
   * finds it, and the producer lane refuses to write it twice.
   */
  const transitions = yield* publishTransitions(workspaceId);

  /**
   * The catalog is caught up here rather than in each reader, because both
   * graph products join it: the board needs projects and users, the label
   * counts need labels. A product derived from a stale catalog is a product
   * that disagrees with the collection endpoint serving the same rows.
   */
  const catalogReports = yield* catchUpJoinedCatalog(workspaceId);

  /**
   * Both graphs are fed from *durable* sources behind their own positions, not
   * from the local variables above.
   *
   * The variables are what the pass happened to fold, and a pass can commit a
   * source relation and then fail before a graph consumes it — the transition
   * feed, the catalog catch-up and the sink appends all sit in between. When
   * that happened the changes existed nowhere but memory, so the next pass read
   * an empty suffix and the published product stayed wrong until the same key
   * changed again. Reading each leg back from what is committed, behind a
   * position that only advances inside the graph's own commit, makes the
   * delivery at-least-once instead of best-effort.
   */
  const boardInputs = yield* graphInputs(workspaceId, "board", BOARD_LEGS);
  const board = yield* store.maintainBoard(workspaceId, boardInputs.inputs, boardInputs.positions);

  const countInputs = yield* graphInputs(workspaceId, "label-counts", LABEL_COUNT_LEGS);
  const counts = yield* store.maintainLabelCounts(
    workspaceId,
    countInputs.inputs,
    countInputs.positions,
  );
  yield* publishGraph(workspaceId, boardLabelCounts.name, counts, {
    publish: (rows) => sink.publishLabelCounts(workspaceId, rows),
    republish: (rows) => sink.republishLabelCounts(workspaceId, rows),
  });

  /**
   * The board publishes on its *graph's* revision, exactly as the label counts
   * do — not on the issue source checkpoint.
   *
   * The board graph joins the project and user catalogs, so a pass in which
   * only the catalog moved still produces a new board revision with the
   * recomputed cards in it. Keying the sink append on the issue checkpoint made
   * that pass look like "nothing to publish", and the checked sink then served
   * the old project or user name on every card whose issue did not happen to
   * change afterwards — per card, and for as long as that stayed true. The
   * graph revision is the only marker that moves whenever the rows move, which
   * is precisely the condition the sink has to track.
   */
  const boardPublication = yield* publishGraph(workspaceId, boardIssues.name, board, {
    publish: (changed) => sink.publish(workspaceId, changed),
    republish: (rows) => sink.republish(workspaceId, rows),
  });
  return report(workspaceId, checkpoint, suffix.items.length, changes, boardPublication);

  function report(
    id: string,
    cursor: string | undefined,
    folded: number,
    published: readonly Change<IssueRow, string>[],
    publication: MaintenanceReport["publication"],
  ): MaintenanceReport {
    return {
      workspaceId: id,
      checkpoint: cursor,
      folded,
      changes: published,
      publication,
      catalog: catalogReports,
      transitions,
    };
  }
});

/**
 * How many committed batches one graph input leg drains per pass.
 *
 * It matches the store's own `keepLastBatches` retention, so a pass always
 * drains everything the history still holds and a leg can never fall behind the
 * window by accumulating a backlog it declined to read.
 */
const GRAPH_INPUT_BATCHES = 256;

/** One input leg of a graph product, and where its durable position comes from. */
type GraphLegSpec =
  | { readonly kind: "history"; readonly sourceId: string; readonly leg: GraphHistoryLeg }
  | { readonly kind: "catalog"; readonly sourceId: string; readonly collection: CatalogCollection };

const BOARD_LEGS: readonly GraphLegSpec[] = [
  { kind: "history", sourceId: issues.name, leg: "issues" },
  { kind: "catalog", sourceId: projects.name, collection: "projects" },
  { kind: "catalog", sourceId: users.name, collection: "users" },
];

const LABEL_COUNT_LEGS: readonly GraphLegSpec[] = [
  { kind: "history", sourceId: issueLabelMemberships.name, leg: "memberships" },
  { kind: "history", sourceId: issues.name, leg: "issues" },
  { kind: "catalog", sourceId: labels.name, collection: "labels" },
];

/**
 * Read one product's whole input set back from durable state.
 *
 * Every leg answers the same two questions — what has this product not consumed
 * yet, and what position would say it has — so the positions the graph commit
 * writes are exactly the positions these reads were taken at. A leg that
 * answers "nothing new" leaves its recorded position untouched.
 */
const graphInputs = Effect.fn("Maintenance.graphInputs")(function* (
  workspaceId: string,
  product: GraphProductId,
  legs: readonly GraphLegSpec[],
) {
  const store = yield* IssueStore;
  const recorded = yield* store.graphInputPositions(workspaceId, product);
  const positions = new Map(Object.entries(recorded));
  const inputs: SourceChanges[] = [];
  for (const spec of legs) {
    const resolved =
      spec.kind === "history"
        ? yield* historyLeg(workspaceId, spec, recorded)
        : yield* catalogLeg(workspaceId, spec, recorded);
    inputs.push({ sourceId: spec.sourceId, changes: resolved.changes });
    if (resolved.position !== undefined) positions.set(spec.sourceId, resolved.position);
  }
  return { inputs, positions: Object.fromEntries(positions) };
});

/**
 * A leg fed from a committed relation's change history.
 *
 * With no recorded position the leg has never been delivered under this
 * mechanism — a database written before it existed, or a graph whose product is
 * new — and the honest delivery is the whole relation, because reading "from
 * the start" of a bounded history would silently mean "from whatever survived
 * retention". Once a position exists the leg is incremental.
 */
const historyLeg = Effect.fn("Maintenance.historyLeg")(function* (
  workspaceId: string,
  spec: { readonly sourceId: string; readonly leg: GraphHistoryLeg },
  recorded: GraphInputPositions,
) {
  const store = yield* IssueStore;
  const at = recorded[spec.sourceId];
  if (at === undefined) {
    const head = yield* store.graphHistoryHead(workspaceId, spec.leg);
    const changes = yield* store.graphHistorySnapshot(workspaceId, spec.leg);
    return { changes, position: head === undefined ? undefined : encodeHistoryPosition(head) };
  }
  const batches = yield* store.graphHistoryChanges(
    workspaceId,
    spec.leg,
    yield* decodeHistoryPosition(spec.sourceId, at),
    GRAPH_INPUT_BATCHES,
  );
  const last = batches.at(-1);
  return {
    changes: batches.flatMap((batch) => batch.changes),
    position: last === undefined ? undefined : encodeHistoryPosition(last.position),
  };
});

/**
 * A leg fed from an ingested State collection.
 *
 * Ingestion keeps rows and a checkpoint but no change history, so the delivery
 * is the whole collection whenever the graph's recorded checkpoint is behind
 * ingestion's. That is exact rather than approximate: a catalog row never
 * exits — deletes are refused at ingestion — so a full set of enters reconciles
 * to precisely the rows the graph is missing. It is also cheap, because a
 * workspace catalog is small and the read happens only when the checkpoint has
 * actually moved.
 */
const catalogLeg = Effect.fn("Maintenance.catalogLeg")(function* (
  workspaceId: string,
  spec: { readonly sourceId: string; readonly collection: CatalogCollection },
  recorded: GraphInputPositions,
) {
  const store = yield* IssueStore;
  const checkpoint = yield* store.stateCheckpoint(spec.sourceId, workspaceId);
  if (checkpoint === undefined || recorded[spec.sourceId] === checkpoint) {
    return { changes: [], position: checkpoint };
  }
  const rows = yield* store.stateRows(spec.sourceId, spec.collection, workspaceId);
  const changes = yield* Effect.forEach(rows, (row) => {
    const decoded = catalogRow(spec.collection, row);
    return Schema.decodeEffect(JsonObjectSchema)(decoded.row).pipe(
      Effect.map((after): Change<JsonObject> => ({ kind: "enter", key: decoded.key, after })),
      Effect.mapError(
        (cause) =>
          new StoreRestorePoison({
            table: spec.collection,
            key: decoded.key,
            detail: String(cause),
          }),
      ),
    );
  });
  return { changes, position: checkpoint };
});

const JsonObjectSchema = Schema.Record(Schema.String, Schema.Json);
const RecordedHistoryPosition = Schema.Struct({ epoch: Schema.Finite, sequence: Schema.Finite });
const RecordedHistoryPositionJson = Schema.fromJsonString(RecordedHistoryPosition);
const encodeHistoryPosition = Schema.encodeUnknownSync(RecordedHistoryPositionJson);

/** A recorded position that no longer parses is durable corruption, never a restart from zero. */
const decodeHistoryPosition = (sourceId: string, value: string) =>
  Schema.decodeEffect(RecordedHistoryPositionJson)(value).pipe(
    Effect.mapError(
      (cause) =>
        new StoreRestorePoison({
          table: "__graph_inputs__",
          key: sourceId,
          detail: String(cause),
        }),
    ),
  );

/**
 * Fold every membership fact after the relation's own checkpoint.
 *
 * It reuses the same interpreter the issue relation uses, because it is the
 * same shape of work: a fact source, a reducer, one keyed relation. What
 * differs is only which stream, which reducer and which decoder.
 */
const advanceMemberships = Effect.fn("Maintenance.advanceMemberships")(function* (
  workspaceId: string,
) {
  const store = yield* IssueStore;
  const checkpoint = yield* store.membershipProgress(workspaceId);
  const suffix = yield* readSuffix(
    (bindings) => bindings.issueLabelEvents(workspaceId),
    checkpoint,
    decodeIssueLabelEvent,
  );
  if (suffix.items.length === 0) return [];

  const keys = touchedKeys(issueLabelMemberships.plan, suffix.items);
  const current = yield* store.membershipStates(workspaceId, keys);
  const folded = yield* Effect.try({
    try: () =>
      maintain<IssueLabelRow>({
        plan: issueLabelMemberships.plan,
        reducer: issueLabelLifecycle,
        decodeRow: decodeIssueLabelRow,
        current,
        items: suffix.items,
      }),
    catch: (cause) =>
      new MaintenanceFault({
        view: issueLabelMemberships.name,
        phase: cause instanceof ReducerFault ? cause.phase : "plan",
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });
  yield* store.membershipCommit(workspaceId, {
    expectedCheckpoint: checkpoint,
    checkpoint: suffix.cursor,
    rows: folded.rows,
    changes: folded.changes,
  });
  return folded.changes;
});

/** The catalog collections the two graph products join, brought to their tails. */
const catchUpJoinedCatalog = Effect.fn("Maintenance.catchUpJoinedCatalog")(function* (
  workspaceId: string,
) {
  const reports: StateIngestionReport[] = [];
  for (const collection of ["projects", "users", "labels"] as const) {
    reports.push(yield* catchUpStateSource(collection, workspaceId));
  }
  return reports;
});

/**
 * Bring one graph product's sink up to the graph's committed revision.
 *
 * The revision is the product's own durable identity, so the decision is exact:
 * nothing to do when the sink already carries this revision, the batch's
 * changes when the sink carries exactly the revision they were computed from,
 * and a full snapshot otherwise — which is what a process that died between the
 * graph commit and the append gets, and is safe because a snapshot replaces
 * rather than accumulates.
 */
const publishGraph = Effect.fn("Maintenance.publishGraph")(function* <Row>(
  workspaceId: string,
  product: string,
  result: GraphResult<Row>,
  sink: {
    readonly publish: (
      changes: readonly Change<Row, string>[],
    ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
    readonly republish: (
      rows: readonly Row[],
    ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
  },
) {
  const store = yield* IssueStore;
  const published = yield* store.graphPublished(workspaceId, product);
  const revision = String(result.revision);
  if (published === revision) return "none" as const;
  if (published === String(result.previousRevision)) {
    // The sink already carries the revision these changes were computed from,
    // so the deltas are exactly what it is missing. A revision that moved
    // without producing any is a graph step no consumer can observe.
    if (result.changes.length === 0) {
      yield* store.markGraphPublished(workspaceId, product, revision);
      return "none" as const;
    }
    yield* sink.publish(result.changes);
    yield* store.markGraphPublished(workspaceId, product, revision);
    return "changes" as const;
  }
  yield* sink.republish(result.rows);
  yield* store.markGraphPublished(workspaceId, product, revision);
  return "snapshot" as const;
});

interface SourceSuffix {
  readonly items: readonly JsonObject[];
  readonly cursor: string;
  readonly maxSequence: number;
}

/**
 * Read every durable fact after `checkpoint`, decoding each one through the
 * declared source schema.
 *
 * The read is catch-up only. A live read would make the pass unbounded, and the
 * command path needs a pass that finishes.
 */
const readSuffix = Effect.fn("Maintenance.readSuffix")(function* (
  bind: (bindings: WorkspaceBindings) => ReturnType<WorkspaceBindings["issueEvents"]>,
  checkpoint: string | undefined,
  decode: (value: JsonValue) => { readonly sequence: number } = decodeIssueEvent,
) {
  const streams = yield* Streams;
  const binding = bind(streams.bindings);

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const reads = yield* ReadStreams;
      // An absent checkpoint means "from the start", which the read protocol
      // expresses by omitting `offset` rather than by a sentinel value.
      const options: ReadStreamOptions = { live: false };
      if (checkpoint !== undefined) options.offset = checkpoint;
      const opened = yield* reads.open(binding, options);
      if (opened.status !== "ok") {
        return { items: [], cursor: checkpoint ?? "", maxSequence: -1 } satisfies SourceSuffix;
      }

      const items: JsonObject[] = [];
      let cursor = checkpoint ?? "";
      let maxSequence = -1;

      for (;;) {
        const next = yield* opened.session.next;
        if (next.done === true) break;
        const batch = next.value;
        if (batch.kind !== "json") {
          return yield* new SourcePoison({
            sourceId: binding.streamId,
            position: batch.offset,
            detail: `expected a json batch, received ${batch.kind}`,
          });
        }
        for (const value of batch.items) {
          const event = yield* decodeSourceItem(binding.streamId, batch.offset, value, decode);
          const decoded = yield* Schema.decodeEffect(JsonObjectSchema)(event).pipe(
            Effect.mapError(
              (cause) =>
                new SourcePoison({
                  sourceId: binding.streamId,
                  position: batch.offset,
                  detail: String(cause),
                }),
            ),
          );
          items.push(decoded);
          maxSequence = Math.max(maxSequence, event.sequence);
        }
        cursor = batch.offset;
        if (batch.upToDate) break;
      }

      return { items, cursor, maxSequence } satisfies SourceSuffix;
    }),
  );
});

/** A durable fact the declared source schema rejects is typed poison, never a served row. */
const decodeSourceItem = (
  sourceId: string,
  position: string,
  value: JsonValue,
  decode: (input: JsonValue) => { readonly sequence: number },
) =>
  Effect.try({
    try: () => decode(value),
    catch: (cause) =>
      new SourcePoison({
        sourceId,
        position,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

/** Run the declaration's plan. A reducer fault is typed, never a partially folded row. */
const fold = (current: ReadonlyMap<string, IssueRow>, items: readonly JsonObject[]) =>
  Effect.try({
    try: () =>
      maintain<IssueRow>({
        plan: issues.plan,
        reducer: issueLifecycle,
        decodeRow: decodeIssueRow,
        current,
        items,
      }),
    catch: (cause) =>
      new MaintenanceFault({
        view: issues.name,
        phase: cause instanceof ReducerFault ? cause.phase : "plan",
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });
