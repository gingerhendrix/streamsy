/**
 * The delivery runtime: one bounded, serialized pass over a sink's outbox.
 *
 * `drain` is the whole runtime. It claims the entries in one lane whose next
 * attempt instant has arrived, and delivers them **one at a time in enqueue
 * order** — never concurrently, because an external effect that must not be
 * duplicated must also not be interleaved with its own retry.
 *
 * Serialized is not the same as blocking. A failing entry is rescheduled into
 * the future and the pass moves on to the next one, so a poisoned delivery
 * cannot hold a lane's healthy traffic hostage. That is the difference between
 * an ordered queue and a stuck one.
 *
 * A pass has exactly one operational failure mode: the outbox itself. Every
 * handler outcome — a typed refusal, a thrown defect, a payload that no longer
 * decodes — is recorded against the entry and the pass keeps going, so a broken
 * handler can never fail the caller that asked for a drain.
 */
import { Cause, Clock, Effect, Exit } from "effect";
import { backoffAfter, type CheckedEffectSink, type EffectSinkRelation } from "./contract.ts";
import {
  EffectSinkDeliveryFailure,
  type DeadLetterReason,
  type OutboxUnavailable,
} from "./errors.ts";
import { OutboxStore, type OutboxDraft, type OutboxEntry } from "./outbox.ts";

/** One attempt at one delivery, as the handler sees it. */
export interface EffectSinkDelivery<Payload> {
  readonly entryId: number;
  readonly sink: string;
  readonly partitionId: string;
  /**
   * The delivery's durable identity.
   *
   * A handler that has already accepted this key must accept it again without
   * repeating the effect. Delivery is at-least-once by construction — the
   * process can die between a successful handler call and the outbox write that
   * records it — so absorbing a repeat is the handler's half of the contract.
   */
  readonly idempotencyKey: string;
  /** One-based: `1` on the first attempt. */
  readonly attempt: number;
  readonly payload: Payload;
}

export interface EffectSinkHandler<Payload, Requirements = never> {
  readonly deliver: (
    delivery: EffectSinkDelivery<Payload>,
  ) => Effect.Effect<void, EffectSinkDeliveryFailure, Requirements>;
}

/** How a handler refuses the delivery it was given, without restating its own identity. */
export interface RefuseDelivery {
  /** The effect may still succeed later: the runtime spends another attempt. */
  readonly retryable: (detail: string) => EffectSinkDeliveryFailure;
  /** The effect can never succeed for this payload: the runtime dead-letters it now. */
  readonly permanent: (detail: string) => EffectSinkDeliveryFailure;
}

/**
 * Bind a delivery function to a sink's declared handler.
 *
 * A handler's own dependencies stay ordinary Effect requirements, so a host
 * supplies them with a layer and the declaration never mentions the transport:
 * the same sink delivers through a real notifier in the host and through a
 * recording fake in a test, with no branch in between.
 */
export function effectSinkHandler<Payload, From extends EffectSinkRelation, Requirements = never>(
  sink: CheckedEffectSink<Payload, From>,
  deliver: (
    delivery: EffectSinkDelivery<Payload>,
    refuse: RefuseDelivery,
  ) => Effect.Effect<void, EffectSinkDeliveryFailure, Requirements>,
): EffectSinkHandler<Payload, Requirements> {
  return {
    deliver: (delivery) =>
      deliver(delivery, {
        retryable: (detail) =>
          EffectSinkDeliveryFailure.retryable(sink.handler.name, delivery.idempotencyKey, detail),
        permanent: (detail) =>
          EffectSinkDeliveryFailure.permanent(sink.handler.name, delivery.idempotencyKey, detail),
      }),
  };
}

export interface DrainOptions {
  /** Restrict the pass to one lane. Omitted, it drains every lane of the sink. */
  readonly partitionId?: string;
  /** Upper bound on entries claimed by one pass. */
  readonly limit?: number;
}

export interface DrainReport {
  readonly sink: string;
  readonly claimed: number;
  readonly delivered: number;
  /** Entries that failed and are scheduled for another attempt. */
  readonly retried: number;
  readonly deadLettered: number;
}

export const DEFAULT_DRAIN_LIMIT = 100;

/** Lower declared payloads to outbox drafts. Pure, so a caller can write them transactionally. */
export function draftsFor<Payload, From extends EffectSinkRelation>(
  sink: CheckedEffectSink<Payload, From>,
  payloads: readonly Payload[],
  enqueuedAtMs: number,
): readonly OutboxDraft[] {
  return payloads.map((payload) => ({
    sink: sink.name,
    partitionId: sink.partitionBy(payload),
    idempotencyKey: sink.idempotencyKey(payload),
    payload: sink.payload.encode(payload),
    enqueuedAtMs,
  }));
}

/** Deliver every due entry in one lane, serially, applying the declared retry policy. */
export const drain = <Payload, From extends EffectSinkRelation, Requirements = never>(
  sink: CheckedEffectSink<Payload, From>,
  handler: EffectSinkHandler<Payload, Requirements>,
  options: DrainOptions = {},
): Effect.Effect<DrainReport, OutboxUnavailable, OutboxStore | Requirements> =>
  Effect.gen(function* () {
    const store = yield* OutboxStore;
    const startedAtMs = yield* Clock.currentTimeMillis;
    const claimed = yield* store.claimDue(
      sink.name,
      options.partitionId,
      startedAtMs,
      options.limit ?? DEFAULT_DRAIN_LIMIT,
    );

    let delivered = 0;
    let retried = 0;
    let deadLettered = 0;

    for (const entry of claimed) {
      const attempt = entry.attempts + 1;
      const decoded = decodePayload(sink, entry);
      if (decoded.kind === "poison") {
        yield* settle(entry, attempt, "payload-poison", decoded.detail);
        deadLettered += 1;
        continue;
      }

      const exit = yield* Effect.exit(
        handler.deliver({
          entryId: entry.id,
          sink: entry.sink,
          partitionId: entry.partitionId,
          idempotencyKey: entry.idempotencyKey,
          attempt,
          payload: decoded.payload,
        }),
      );
      if (Exit.isSuccess(exit)) {
        const at = yield* Clock.currentTimeMillis;
        yield* store.markDelivered(entry.id, attempt, at);
        delivered += 1;
        continue;
      }
      // An interrupted pass has decided nothing about this entry, so it is left
      // exactly as it was claimed and the whole drain unwinds.
      if (Cause.hasInterrupts(exit.cause)) return yield* Effect.interrupt;

      const failure = failureOf(sink, entry, exit.cause);
      const reason = deadLetterReason(sink, attempt, failure);
      if (reason !== undefined) {
        yield* settle(entry, attempt, reason, failure.detail);
        deadLettered += 1;
        continue;
      }
      const at = yield* Clock.currentTimeMillis;
      yield* store.reschedule(
        entry.id,
        attempt,
        at + backoffAfter(sink.delivery, attempt),
        failure.detail,
      );
      retried += 1;
    }

    return {
      sink: sink.name,
      claimed: claimed.length,
      delivered,
      retried,
      deadLettered,
    } satisfies DrainReport;

    function settle(
      entry: OutboxEntry,
      attempt: number,
      reason: DeadLetterReason,
      detail: string,
    ): Effect.Effect<void, OutboxUnavailable> {
      return Clock.currentTimeMillis.pipe(
        Effect.flatMap((at) => store.deadLetter(entry.id, attempt, reason, detail, at)),
      );
    }
  });

type DecodedPayload<Payload> =
  | { readonly kind: "payload"; readonly payload: Payload }
  | { readonly kind: "poison"; readonly detail: string };

/**
 * A stored payload the declared codec now rejects is dead on arrival.
 *
 * Retrying it would re-run the same decode against the same bytes forever, so
 * it dead-letters immediately — fail-stop for that entry, and only that entry.
 */
function decodePayload<Payload, From extends EffectSinkRelation>(
  sink: CheckedEffectSink<Payload, From>,
  entry: OutboxEntry,
): DecodedPayload<Payload> {
  try {
    return { kind: "payload", payload: sink.payload.decode(JSON.parse(entry.payload)) };
  } catch (cause) {
    return { kind: "poison", detail: describe(cause) };
  }
}

/** Every non-interrupt cause becomes one retryable refusal; a thrown handler is still a handler. */
function failureOf<Payload, From extends EffectSinkRelation>(
  sink: CheckedEffectSink<Payload, From>,
  entry: OutboxEntry,
  cause: Cause.Cause<EffectSinkDeliveryFailure>,
): EffectSinkDeliveryFailure {
  const declared = cause.reasons.find((reason): reason is Cause.Fail<EffectSinkDeliveryFailure> => {
    const { _tag: tag } = reason;
    return tag === "Fail";
  });
  if (declared !== undefined) return declared.error;
  return EffectSinkDeliveryFailure.retryable(
    sink.handler.name,
    entry.idempotencyKey,
    Cause.pretty(cause).slice(0, 500),
  );
}

function deadLetterReason<Payload, From extends EffectSinkRelation>(
  sink: CheckedEffectSink<Payload, From>,
  attempt: number,
  failure: EffectSinkDeliveryFailure,
): DeadLetterReason | undefined {
  if (!failure.retryable) return "permanent";
  if (attempt >= sink.delivery.maxAttempts) return "attempts-exhausted";
  return undefined;
}

function describe(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}
