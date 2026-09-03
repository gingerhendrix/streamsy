/**
 * The `streamSink` runtime: one durable feed of issue transitions.
 *
 * `issueTransitions` declares `changes(issues)`, so this module's only job is
 * to write the maintained relation's changes down in the order the fold
 * produced them, and to read them back in that same order. There is no sort
 * anywhere in this file, and that is the point: the fold observes Durable
 * Stream arrival order, so the feed is in arrival order, and a fact that
 * arrives out of domain sequence is visible as the transition it actually
 * caused.
 *
 * **Publication is driven from the committed change history, not from the
 * in-memory batch.** That closes the Wave B-i gap where a process dying between
 * the row commit and the feed append kept the rows and lost that batch's
 * transitions. Two properties do it, and both are needed:
 *
 * - The changes were written in the *same atomic commit* as the rows, so a
 *   batch that exists to publish is a batch whose rows landed. A crash before
 *   the append leaves the batch owed, and the next pass finds it.
 * - The append runs on a **producer lane** whose sequence is durable, so a
 *   crash *after* the append and before the marker moves replays the same
 *   sequence, the protocol answers `duplicate`, and no transition is written
 *   twice. Delivery is at-least-once; the feed is exactly-once.
 *
 * The recovery window is the store's change-history retention, and a batch that
 * has fallen out of it is {@link TransitionHistoryExpired} — fail-stop, because
 * a silently missing transition is a hole in a log whose whole contract is that
 * it has none. In practice at most one batch is ever owed, because every
 * maintenance pass publishes.
 *
 * The action sink's outbox is deliberately still not used here. The outbox
 * delivers external effects at least once with a dead-letter terminus; this
 * feed is a replayable log whose contract is arrival order and native-offset
 * resume, and a dead-lettered transition would be exactly the hole above.
 */
import { defaultOffsetGenerator, type JsonValue, type ReadStreamOptions } from "@streamsy/core";
import { AppendStreams, ReadStreams } from "@streamsy/streams";
import type { StreamSinkPage } from "@streamsy/sinks/server/stream";
import type { Change } from "@streamsy/views/ir";
import { Effect, Schema } from "effect";
import { issues, issueTransitions } from "../../domain/declaration.ts";
import {
  decodeIssueRow,
  decodeIssueTransition,
  type IssueRow,
  type IssueTransition,
} from "../../domain/issue.ts";
import type { StoredChange } from "@streamsy/views/store";
import {
  AppendRejected,
  MaintenanceFault,
  SourcePoison,
  StreamUnavailable,
  type TransitionHistoryExpired,
} from "../errors.ts";
import { IssueStore } from "../persistence/store.ts";
import { Streams } from "../transport/streams.ts";

/** One page of the feed is bounded, so a consumer's catch-up read stays finite. */
export const TRANSITION_PAGE_LIMIT = 500;

/**
 * Project the maintained relation's changes onto published transitions.
 *
 * Pure, and order-preserving by construction: the input order is the output
 * order. `occurredAt` is the row's own `updatedAt`, so a transition carries the
 * instant the fold assigned rather than the instant it was published.
 */
export function transitionsOf(
  changes: readonly Change<IssueRow, string>[],
): readonly IssueTransition[] {
  return changes.map((change) => {
    if (change.kind === "exit") {
      return {
        workspaceId: change.before.workspaceId,
        issueId: change.before.issueId,
        change: "exit" as const,
        status: change.before.status,
        title: change.before.title,
        occurredAt: change.before.updatedAt,
      };
    }
    const entered = {
      workspaceId: change.after.workspaceId,
      issueId: change.after.issueId,
      status: change.after.status,
      title: change.after.title,
      occurredAt: change.after.updatedAt,
    };
    return change.kind === "enter"
      ? { ...entered, change: "enter" as const }
      : { ...entered, change: "update" as const, previousStatus: change.before.status };
  });
}

/**
 * The producer lane the feed is written on.
 *
 * One lane per feed stream, and the stream is already per workspace, so the id
 * is a constant. The epoch never moves: a second writer would be a second host
 * owning one workspace partition, which the host's keying forbids.
 */
export const TRANSITION_PRODUCER_ID = "issue-tracker-transitions";

/** How many committed batches one publication pass drains. */
export const TRANSITION_PUBLISH_BATCHES = 64;

/**
 * Bring the feed up to every change batch the store has committed.
 *
 * Returns how many transitions this pass appended. Zero is the steady state on
 * a pass that folded nothing.
 */
export const publishTransitions = Effect.fn("Transitions.publish")(function* (workspaceId: string) {
  const store = yield* IssueStore;
  const streams = yield* Streams;
  const appends = yield* AppendStreams;
  const binding = streams.bindings.issueTransitions(workspaceId);

  let progress = yield* store.transitionProgress(workspaceId);
  let published = 0;

  for (;;) {
    const batches = yield* store.committedIssueChanges(
      workspaceId,
      progress.position,
      TRANSITION_PUBLISH_BATCHES,
    );
    if (batches.length === 0) return published;

    for (const batch of batches) {
      const transitions = transitionsOf(yield* restoreChanges(workspaceId, batch.changes));
      if (transitions.length > 0) {
        const appended = yield* appends
          .appendJsonBatch(binding, transitions.map(encodeTransition), {
            producer: {
              producerId: TRANSITION_PRODUCER_ID,
              producerEpoch: 0,
              producerSeq: progress.sequence,
            },
          })
          .pipe(
            Effect.mapError(
              (error) =>
                new StreamUnavailable({ streamId: binding.streamId, status: String(error) }),
            ),
          );
        // `duplicate` is the recovery path succeeding: this batch was appended
        // before the marker moved, and the lane refused to write it twice.
        if (appended.status !== "appended" && appended.status !== "duplicate") {
          return yield* new AppendRejected({
            stream: binding.streamId,
            status: appended.status,
          });
        }
        published += transitions.length;
        progress = { position: batch.position, sequence: progress.sequence + 1 };
      } else {
        progress = { position: batch.position, sequence: progress.sequence };
      }
      yield* store.markTransitionsPublished(workspaceId, progress);
    }
    if (batches.length < TRANSITION_PUBLISH_BATCHES) return published;
  }
});

/**
 * Decode one committed batch back into typed relation changes.
 *
 * The history holds the same JSON the commit wrote, so a value that no longer
 * decodes is a declaration change that left durable state behind — a fault,
 * never a transition quietly dropped from the feed.
 */
const restoreChanges = Effect.fn("Transitions.restoreChanges")(function* (
  workspaceId: string,
  stored: readonly StoredChange[],
) {
  const changes: Change<IssueRow, string>[] = [];
  for (const change of stored) {
    if (change.relationId !== issues.name) continue;
    // The `issues` relation declares `key: "issueId"`, so the commit wrote a
    // string. `RowKey` admits composites for relations that declare them; this
    // one does not, and the declared key is what decides.
    const key = decodeIssueRowKey(change.key);
    changes.push(
      yield* Effect.try({
        try: (): Change<IssueRow, string> => {
          if (change.kind === "enter") {
            return { kind: "enter", key, after: decodeIssueRow(change.after) };
          }
          if (change.kind === "update") {
            return {
              kind: "update",
              key,
              before: decodeIssueRow(change.before),
              after: decodeIssueRow(change.after),
            };
          }
          return { kind: "exit", key, before: decodeIssueRow(change.before) };
        },
        catch: (cause) =>
          new MaintenanceFault({
            view: issues.name,
            phase: "transitions",
            detail: `${workspaceId}/${key}: ${cause instanceof Error ? cause.message : String(cause)}`,
          }),
      }),
    );
  }
  return changes;
});

/** What a publication pass can fail with, named once for the callers that map it. */
export type TransitionPublishError =
  | StreamUnavailable
  | AppendRejected
  | MaintenanceFault
  | TransitionHistoryExpired;

/** A read that could not resume, separated from a feed that is simply unavailable. */
export class TransitionReadFailure {
  readonly _tag = "TransitionReadFailure";
  constructor(
    readonly reason: "unavailable" | "invalid-offset" | "history-unavailable",
    readonly detail: string,
  ) {}
}

/**
 * Read one bounded page of the feed after `offset`.
 *
 * Every message is decoded through the declared transition schema before it is
 * counted as an event, so a corrupted feed is typed poison rather than a served
 * value. The returned cursor is always a batch boundary, so a consumer that
 * resumes from it never re-reads or skips half a batch.
 */
export const readTransitions = Effect.fn("Transitions.read")(function* (
  workspaceId: string,
  offset: string | undefined,
) {
  const streams = yield* Streams;
  const binding = streams.bindings.issueTransitions(workspaceId);

  /**
   * A resume position is checked against the canonical offset token before it
   * reaches the reader, because a reader that is handed an unparseable offset
   * reads from the start instead of failing — which would silently replay the
   * whole feed to a consumer that asked for a suffix. Rejecting here is what
   * lowers to the declared `replay-from-start` fallback, so the consumer is
   * told to rebuild rather than handed duplicates it cannot detect.
   */
  if (offset !== undefined && !defaultOffsetGenerator.isValid(offset)) {
    return yield* Effect.fail(
      new TransitionReadFailure("invalid-offset", `not a native offset token: ${offset}`),
    );
  }

  return yield* Effect.scoped(
    Effect.gen(function* () {
      const reads = yield* ReadStreams;
      const options: ReadStreamOptions = { live: false };
      if (offset !== undefined) options.offset = offset;
      const opened = yield* reads
        .open(binding, options)
        .pipe(Effect.mapError((error) => readFailure(offset, String(error))));
      if (opened.status !== "ok") {
        return yield* Effect.fail(readFailure(offset, `feed is ${opened.status}`));
      }

      const events: IssueTransition[] = [];
      let cursor = offset ?? "-1";
      let upToDate = false;

      while (events.length < TRANSITION_PAGE_LIMIT) {
        const next = yield* opened.session.next.pipe(
          Effect.mapError((error) => readFailure(offset, String(error))),
        );
        if (next.done === true) {
          upToDate = true;
          break;
        }
        const batch = next.value;
        if (batch.kind !== "json") {
          return yield* new SourcePoison({
            sourceId: binding.streamId,
            position: batch.offset,
            detail: `expected a json batch, received ${batch.kind}`,
          });
        }
        for (const value of batch.items) {
          events.push(yield* decodeTransition(binding.streamId, batch.offset, value));
        }
        cursor = batch.offset;
        if (batch.upToDate) {
          upToDate = true;
          break;
        }
      }

      return { events, nextOffset: cursor, upToDate } satisfies StreamSinkPage;
    }),
  );
});

/** The declared feed route for one workspace, built from the declaration itself. */
export const transitionFeedRoute = (workspaceId: string): string =>
  issueTransitions.compiledRoute.build({ workspaceId });

/**
 * A read failure below the reader, told apart by whether a suffix was asked for.
 *
 * The offset token itself was already checked above, so a failure that carries
 * one is the history no longer being readable from that point rather than a
 * malformed request. A read with no offset could not have failed to resume at
 * all, so it is simply an unavailable feed.
 */
function readFailure(offset: string | undefined, detail: string): TransitionReadFailure {
  return new TransitionReadFailure(
    offset === undefined ? "unavailable" : "history-unavailable",
    detail,
  );
}

const decodeTransition = (sourceId: string, position: string, value: JsonValue) =>
  Effect.try({
    try: () => decodeIssueTransition(value),
    catch: (cause) =>
      new SourcePoison({
        sourceId,
        position,
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });

/**
 * The row key of a committed `issue-tracker.issues` change.
 *
 * `RowKey` admits composites for relations that declare them; this relation
 * declares `key: "issueId"`, so a value that is not a string is a change from a
 * relation this feed does not publish — a fault, not a stringified object.
 */
const decodeIssueRowKey = Schema.decodeUnknownSync(Schema.String);

/** The wire shape of one transition. Written out, so an added row field cannot leak into the feed. */
function encodeTransition(transition: IssueTransition): JsonValue {
  const encoded = {
    workspaceId: transition.workspaceId,
    issueId: transition.issueId,
    change: transition.change,
    status: transition.status,
    title: transition.title,
    occurredAt: transition.occurredAt,
  };
  return transition.previousStatus === undefined
    ? encoded
    : { ...encoded, previousStatus: transition.previousStatus };
}
