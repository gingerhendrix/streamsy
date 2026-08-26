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
 * Publication is a separate durable append from the row commit that produced
 * it. A process that dies between the two loses the transitions of that one
 * batch while keeping its rows, because the rows are the authority.
 *
 * The Effect sink's outbox is deliberately *not* used to close that gap, even
 * though both tracks now live in one tree. The outbox delivers external effects
 * at least once, keyed and retried, with a dead-letter terminus; this feed is a
 * replayable log whose contract is arrival order and native-offset resume. A
 * dead-lettered transition would be an undetectable hole in that log, which is
 * worse than losing a whole batch. The outbox also enqueues from the command
 * receipt, so it could not carry facts appended straight to the durable source
 * — which the feed's own ordering test does. Closing this atomically is
 * Integration 2 work; see `verification-wave-bi-integration.md`.
 */
import { defaultOffsetGenerator, type JsonValue, type ReadStreamOptions } from "@streamsy/core";
import { AppendStreams, ReadStreams } from "@streamsy/experimental/effect";
import type { StreamSinkPage } from "@streamsy/sinks/effect";
import type { Change } from "@streamsy/views-ir";
import { Effect } from "effect";
import { issueTransitions } from "../domain/declaration.ts";
import { decodeIssueTransition, type IssueRow, type IssueTransition } from "../domain/issue.ts";
import { AppendRejected, SourcePoison, StreamUnavailable } from "./errors.ts";
import { Streams } from "./streams.ts";

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

/** Append one maintenance pass's transitions, in the order the fold produced them. */
export const publishTransitions = Effect.fn("Transitions.publish")(function* (
  workspaceId: string,
  changes: readonly Change<IssueRow, string>[],
) {
  const transitions = transitionsOf(changes);
  if (transitions.length === 0) return 0;
  const streams = yield* Streams;
  const appends = yield* AppendStreams;
  const binding = streams.bindings.issueTransitions(workspaceId);
  const appended = yield* appends
    .appendJsonBatch(binding, transitions.map(encodeTransition))
    .pipe(
      Effect.mapError(
        (error) => new StreamUnavailable({ streamId: binding.streamId, status: String(error) }),
      ),
    );
  if (appended.status !== "appended" && appended.status !== "duplicate") {
    return yield* new AppendRejected({ stream: binding.streamId, status: appended.status });
  }
  return transitions.length;
});

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
