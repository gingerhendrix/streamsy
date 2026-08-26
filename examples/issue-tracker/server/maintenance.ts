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
import { ReadStreams } from "@streamsy/experimental/effect";
import { Effect } from "effect";
import {
  boardLabelCounts,
  issueLabelLifecycle,
  issueLabelMemberships,
  issueLifecycle,
  issues,
  labels,
  projects,
  users,
} from "../domain/declaration.ts";
import {
  decodeIssueEvent,
  decodeIssueLabelEvent,
  decodeIssueLabelRow,
  decodeIssueRow,
  type IssueLabelRow,
  type IssueRow,
} from "../domain/issue.ts";
import type { Change, JsonObject } from "@streamsy/views-ir";
import { maintain, ReducerFault, touchedKeys } from "../views/engine.ts";
import { AppendRejected, MaintenanceFault, SourcePoison, StreamUnavailable } from "./errors.ts";
import { IssueSink } from "./sink.ts";
import { IssueStore, type GraphResult } from "./store.ts";
import { catchUpStateSource, stateSourceId, type StateIngestionReport } from "./state-ingestion.ts";
import { publishTransitions } from "./transitions.ts";
import { Streams, type WorkspaceBindings } from "./streams.ts";

export interface MaintenanceReport {
  readonly workspaceId: string;
  /** After-exclusive source cursor now committed. */
  readonly checkpoint: string | undefined;
  readonly folded: number;
  readonly changes: readonly Change<IssueRow, string>[];
  /** How the sink was brought up to the committed checkpoint. */
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
  const memberships = yield* advanceMemberships(workspaceId);

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
  const catalogChanges = {
    projects: encodeChanges(catalogReports, projects.name),
    users: encodeChanges(catalogReports, users.name),
    labels: encodeChanges(catalogReports, labels.name),
  };

  const after = yield* store.progress(workspaceId);
  const board = yield* store.maintainBoard(workspaceId, [
    {
      sourceId: "issue-tracker.issues",
      changes: changes.map((change) => JSON.parse(JSON.stringify(change))),
    },
    { sourceId: projects.name, changes: catalogChanges.projects },
    { sourceId: users.name, changes: catalogChanges.users },
  ]);

  const counts = yield* store.maintainLabelCounts(workspaceId, [
    {
      sourceId: issueLabelMemberships.name,
      changes: memberships.map((change) => JSON.parse(JSON.stringify(change))),
    },
    {
      sourceId: "issue-tracker.issues",
      changes: changes.map((change) => JSON.parse(JSON.stringify(change))),
    },
    { sourceId: labels.name, changes: catalogChanges.labels },
  ]);
  yield* publishGraph(workspaceId, boardLabelCounts.name, counts, {
    publish: (rows) => sink.publishLabelCounts(workspaceId, rows),
    republish: (rows) => sink.republishLabelCounts(workspaceId, rows),
  });

  if (after.checkpoint === undefined || after.published === after.checkpoint) {
    return report(workspaceId, checkpoint, suffix.items.length, changes, "none");
  }

  const inSync =
    after.published !== undefined && after.published === before.checkpoint && changes.length > 0;
  if (inSync) {
    yield* sink.publish(workspaceId, board.changes);
    yield* store.markPublished(workspaceId, after.checkpoint);
    return report(workspaceId, checkpoint, suffix.items.length, changes, "changes");
  }

  yield* sink.republish(workspaceId, board.rows);
  yield* store.markPublished(workspaceId, after.checkpoint);
  return report(workspaceId, checkpoint, suffix.items.length, changes, "snapshot");

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

/** One collection's ingested changes, in the shape the operator graph consumes. */
function encodeChanges(
  reports: readonly StateIngestionReport[],
  sourceId: string,
): readonly Change<JsonObject>[] {
  const found = reports.find((report) => stateSourceId(report.collection) === sourceId);
  return (found?.changes ?? []).map((change) => JSON.parse(JSON.stringify(change)));
}

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
          // SAFETY: `event` is a value the declared source schema accepted, so
          // it is a JSON object whose fields are exactly the ones the source
          // declares — which is what the engine reads through its scopes.
          // oxlint-disable-next-line anti-slop/no-chained-type-assertions, anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
          const decoded = event as unknown as JsonObject;
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
