/**
 * The source half of the exchange, read inside the source partition's runtime.
 *
 * This module never crosses a domain. It runs where the workspace's own
 * `Streams` service lives, reads that workspace's canonical issue-event log,
 * and returns plain values. The exchange engine is what carries those values
 * to another partition — so a workspace runtime still cannot reach anything a
 * workspace runtime could not reach before.
 *
 * The position is an **arrival index**: how many items of the durable log the
 * exchange has consumed. It is deliberately not a native Durable Streams
 * offset. An offset is the transport's token for resuming a *stream*, and the
 * moment the exchange held one, its cursor would be in the same position
 * domain as a sink client's resume point — which is exactly the mixing gate 8
 * forbids. An arrival index is a count in the application's own domain, so
 * there is nothing to mix.
 *
 * Reading from the start each pass is the same bounded scan the command path
 * already performs for sequence allocation, and it is what makes the exchange
 * observe *arrival* order rather than domain-sequence order — a fact appended
 * out of sequence by another process reaches the inbox in the order it landed.
 */
import type { ReadStreamOptions } from "@streamsy/core";
import { ReadStreams } from "@streamsy/experimental/effect";
import { Effect } from "effect";
import type { AssignmentActivity } from "../domain/exchange.ts";
import { decodeIssueEvent } from "../domain/issue.ts";
import { SourcePoison, StreamUnavailable } from "./errors.ts";
import { Streams } from "./streams.ts";

/** One pass reads at most this many records, so a pass stays finite. */
export const EXCHANGE_SOURCE_PAGE_LIMIT = 500;

/** Upper bound on items scanned in one pass, matching the command path's own bound. */
export const EXCHANGE_SOURCE_MAX_ITEMS = 10_000;

export interface AssignmentActivityPage {
  readonly records: readonly AssignmentActivity[];
  /** The arrival index to resume from: items consumed, including skipped ones. */
  readonly arrival: number;
  /** True when the scan reached the durable tail rather than the page limit. */
  readonly upToDate: boolean;
}

/**
 * Every assignment fact after `afterArrival`, in the order the log presented it.
 *
 * Records carry only what the canonical fact carries. Nothing is enriched from
 * maintained state, so one fact projects to one row whenever it is read.
 */
export const readAssignmentActivity = Effect.fn("Exchange.readAssignmentActivity")(function* (
  workspaceId: string,
  afterArrival: number,
  limit: number = EXCHANGE_SOURCE_PAGE_LIMIT,
) {
  const streams = yield* Streams;
  const binding = streams.bindings.issueEvents(workspaceId);
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const reads = yield* ReadStreams;
      const options: ReadStreamOptions = { live: false };
      const opened = yield* reads
        .open(binding, options)
        .pipe(Effect.mapError(unavailable(binding.streamId)));
      if (opened.status !== "ok") {
        // A workspace whose log has not been created yet has consumed nothing.
        return {
          records: [],
          arrival: afterArrival,
          upToDate: true,
        } satisfies AssignmentActivityPage;
      }

      const records: AssignmentActivity[] = [];
      let index = 0;
      let arrival = afterArrival;
      let upToDate = false;

      scan: for (;;) {
        const next = yield* opened.session.next.pipe(
          Effect.mapError(unavailable(binding.streamId)),
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
          const position = index;
          index += 1;
          if (index > EXCHANGE_SOURCE_MAX_ITEMS) {
            return yield* new SourcePoison({
              sourceId: binding.streamId,
              position: batch.offset,
              detail: `exchange scan exceeded ${EXCHANGE_SOURCE_MAX_ITEMS} items`,
            });
          }
          if (position < afterArrival) continue;
          const event = yield* Effect.try({
            try: () => decodeIssueEvent(value),
            catch: (cause) =>
              new SourcePoison({
                sourceId: binding.streamId,
                position: batch.offset,
                detail: cause instanceof Error ? cause.message : String(cause),
              }),
          });
          arrival = position + 1;
          if (event.type === "IssueAssigned") {
            records.push({
              workspaceId: event.workspaceId,
              issueId: event.issueId,
              assigneeId: event.assigneeId,
              status: event.status,
              eventId: event.eventId,
              occurredAt: event.occurredAt,
              sequence: event.sequence,
              arrival: position,
            });
          }
          if (records.length >= limit) break scan;
        }
        if (batch.upToDate) {
          upToDate = true;
          break;
        }
      }

      return { records, arrival, upToDate } satisfies AssignmentActivityPage;
    }),
  );
});

const unavailable = (streamId: string) => (cause: unknown) =>
  new StreamUnavailable({ streamId, status: String(cause) });
