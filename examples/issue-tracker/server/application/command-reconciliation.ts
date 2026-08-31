/** Bounded canonical-source scan used for sequence allocation and rare receipt recovery. */
import { ZERO_OFFSET, type JsonValue, type ReadStreamOptions } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { ReadStreams, type StreamReadError } from "@streamsy/experimental/effect";
import { Effect } from "effect";
import { decodeIssueEvent, decodeIssueLabelEvent, type IssueEvent } from "../../domain/issue.ts";
import type { IssueLabelEvent } from "../../domain/issue.ts";
import { CommandRecoveryExhausted, SourcePoison } from "../errors.ts";
import { Streams, type WorkspaceBindings } from "../transport/streams.ts";

export const COMMAND_SCAN_MAX_BATCHES = 512;
export const COMMAND_SCAN_MAX_ITEMS = 10_000;

/**
 * One canonical fact stream, scanned for a command.
 *
 * The scan is generic over the fact family because both families need exactly
 * the same three answers — the tail to CAS against, the highest sequence
 * allocated so far, and whether this command's fact is already durable — and a
 * second copy of that loop is a second place for the bound checks to drift.
 */
export interface CanonicalSource<Event> {
  readonly tail: string;
  readonly maxSequence: number;
  readonly match: { readonly event: Event; readonly offset: string } | undefined;
}

export type CanonicalIssueSource = CanonicalSource<IssueEvent>;
export type CanonicalLabelSource = CanonicalSource<IssueLabelEvent>;

interface CanonicalFact {
  readonly eventId: string;
  readonly sequence: number;
}

export const scanCanonicalSource = Effect.fn("Commands.scanCanonicalSource")(function* <
  Event extends CanonicalFact,
>(
  bind: (bindings: WorkspaceBindings) => StreamBinding,
  workspaceId: string,
  commandId: string,
  decode: (value: JsonValue) => Event,
) {
  const streams = yield* Streams;
  const binding = bind(streams.bindings);
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
        } satisfies CanonicalSource<Event>;
      }

      let tail = ZERO_OFFSET;
      let maxSequence = -1;
      let batches = 0;
      let items = 0;
      let match: CanonicalSource<Event>["match"];
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
            try: () => decode(value),
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
      return { tail, maxSequence, match } satisfies CanonicalSource<Event>;
    }),
  );
});

/** The issue fact family. */
export const scanCanonicalIssueSource = (
  workspaceId: string,
  commandId: string,
): Effect.Effect<
  CanonicalIssueSource,
  SourcePoison | CommandRecoveryExhausted | StreamReadError,
  Streams | ReadStreams
> =>
  scanCanonicalSource(
    (bindings) => bindings.issueEvents(workspaceId),
    workspaceId,
    commandId,
    decodeIssueEvent,
  );

/** The membership fact family. */
export const scanCanonicalLabelSource = (
  workspaceId: string,
  commandId: string,
): Effect.Effect<
  CanonicalLabelSource,
  SourcePoison | CommandRecoveryExhausted | StreamReadError,
  Streams | ReadStreams
> =>
  scanCanonicalSource(
    (bindings) => bindings.issueLabelEvents(workspaceId),
    workspaceId,
    commandId,
    decodeIssueLabelEvent,
  );
