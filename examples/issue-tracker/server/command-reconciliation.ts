/** Bounded canonical-source scan used for sequence allocation and rare receipt recovery. */
import { ZERO_OFFSET, type ReadStreamOptions } from "@streamsy/core";
import { ReadStreams } from "@streamsy/experimental/effect";
import { Effect } from "effect";
import { decodeIssueEvent, type IssueEvent } from "../domain/issue.ts";
import { CommandRecoveryExhausted, SourcePoison } from "./errors.ts";
import { Streams } from "./streams.ts";

export const COMMAND_SCAN_MAX_BATCHES = 512;
export const COMMAND_SCAN_MAX_ITEMS = 10_000;

export interface CanonicalIssueSource {
  readonly tail: string;
  readonly maxSequence: number;
  readonly match: { readonly event: IssueEvent; readonly offset: string } | undefined;
}

export const scanCanonicalIssueSource = Effect.fn("Commands.scanCanonicalIssueSource")(function* (
  workspaceId: string,
  commandId: string,
) {
  const streams = yield* Streams;
  const binding = streams.bindings.issueEvents(workspaceId);
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const reads = yield* ReadStreams;
      // One message per batch preserves the exact native offset of a recovered
      // command even when later appends already exist.
      const options: ReadStreamOptions = { live: false, batchSize: 1 };
      const opened = yield* reads.open(binding, options);
      if (opened.status !== "ok") {
        return {
          tail: ZERO_OFFSET,
          maxSequence: -1,
          match: undefined,
        } satisfies CanonicalIssueSource;
      }

      let tail = ZERO_OFFSET;
      let maxSequence = -1;
      let batches = 0;
      let items = 0;
      let match: CanonicalIssueSource["match"];
      for (;;) {
        const next = yield* opened.session.next;
        if (next.done === true) break;
        const batch = next.value;
        batches += 1;
        if (batch.kind !== "json") {
          return yield* new SourcePoison({
            sourceId: binding.streamId,
            position: batch.offset,
            detail: `expected a json batch, received ${batch.kind}`,
          });
        }
        items += batch.items.length;
        if (batches > COMMAND_SCAN_MAX_BATCHES || items > COMMAND_SCAN_MAX_ITEMS) {
          return yield* new CommandRecoveryExhausted({
            workspaceId,
            maxBatches: COMMAND_SCAN_MAX_BATCHES,
            maxItems: COMMAND_SCAN_MAX_ITEMS,
          });
        }
        for (const value of batch.items) {
          const event = yield* Effect.try({
            try: () => decodeIssueEvent(value),
            catch: (cause) =>
              new SourcePoison({
                sourceId: binding.streamId,
                position: batch.offset,
                detail: cause instanceof Error ? cause.message : String(cause),
              }),
          });
          maxSequence = Math.max(maxSequence, event.sequence);
          if (event.eventId === commandId) {
            if (match !== undefined) {
              return yield* new SourcePoison({
                sourceId: binding.streamId,
                position: batch.offset,
                detail: `command event ${commandId} appears more than once`,
              });
            }
            match = { event, offset: batch.offset };
          }
        }
        tail = batch.offset;
        if (batch.upToDate) break;
      }
      return { tail, maxSequence, match } satisfies CanonicalIssueSource;
    }),
  );
});
