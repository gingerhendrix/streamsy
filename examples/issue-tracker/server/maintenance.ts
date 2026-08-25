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
import { issueLifecycle, issues } from "../domain/declaration.ts";
import { decodeIssueEvent, decodeIssueRow, type IssueRow } from "../domain/issue.ts";
import type { Change, JsonObject } from "@streamsy/views-ir";
import { maintain, ReducerFault, touchedKeys } from "../views/engine.ts";
import { MaintenanceFault, SourcePoison } from "./errors.ts";
import { IssueSink } from "./sink.ts";
import { IssueStore } from "./store.ts";
import { Streams } from "./streams.ts";

export interface MaintenanceReport {
  readonly workspaceId: string;
  /** After-exclusive source cursor now committed. */
  readonly checkpoint: string | undefined;
  readonly folded: number;
  readonly changes: readonly Change<IssueRow, string>[];
  /** How the sink was brought up to the committed checkpoint. */
  readonly publication: "none" | "changes" | "snapshot";
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
  const suffix = yield* readSuffix(workspaceId, recovery?.sourceCursor ?? before.checkpoint);

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

  // Publication is a separate durable step, so its progress is read again
  // rather than assumed from the commit above.
  const after = yield* store.progress(workspaceId);
  const board = yield* store.maintainBoard(workspaceId, [
    {
      sourceId: "issue-tracker.issues",
      changes: changes.map((change) => JSON.parse(JSON.stringify(change))),
    },
  ]);
  if (after.checkpoint === undefined || after.published === after.checkpoint) {
    return report(workspaceId, checkpoint, suffix.items.length, changes, "none");
  }

  // `undefined === undefined` on a workspace's first pass would look "in sync"
  // while the sink has no snapshot boundary at all, so a never-published sink
  // is explicitly not in sync.
  const inSync =
    after.published !== undefined && after.published === before.checkpoint && changes.length > 0;
  if (inSync) {
    yield* sink.publish(workspaceId, board.changes);
    yield* store.markPublished(workspaceId, after.checkpoint);
    return report(workspaceId, checkpoint, suffix.items.length, changes, "changes");
  }

  // Either the sink has never been published, or publication fell behind by
  // more than this pass. Rebuild it from the durable rows and let consumers
  // reset — convergence, not a replay of messages nobody recorded.
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
    return { workspaceId: id, checkpoint: cursor, folded, changes: published, publication };
  }
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
  workspaceId: string,
  checkpoint: string | undefined,
) {
  const streams = yield* Streams;
  const binding = streams.bindings.issueEvents(workspaceId);

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
          const event = yield* decodeSourceItem(binding.streamId, batch.offset, value);
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
const decodeSourceItem = (sourceId: string, position: string, value: JsonValue) =>
  Effect.try({
    try: () => decodeIssueEvent(value),
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
