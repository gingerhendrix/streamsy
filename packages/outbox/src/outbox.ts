import {
  Array,
  Cause,
  Clock,
  Effect,
  Exit,
  Option,
  Predicate,
  Result,
  Schema,
  Semaphore,
} from "effect";
import { InvalidConfiguration, PayloadEncodingFailure, type DeliveryFailure } from "./errors.ts";
import {
  OutboxStore,
  Settlement,
  type OutboxBacking,
  type OutboxDraft,
  type EnqueueReport,
} from "./store.ts";

export interface RetryPolicy {
  readonly maxAttempts: number;
  readonly initialDelayMs: number;
  readonly factor: number;
  readonly maxDelayMs: number;
}

export interface Spec<Payload> {
  readonly name: string;
  readonly payload: Schema.Codec<Payload, unknown>;
  readonly idempotencyKey: (payload: Payload) => string;
  readonly partitionBy?: (payload: Payload) => string;
  readonly retry: RetryPolicy;
}

const DefinitionTypeId: unique symbol = Symbol("streamsy/outbox/Definition");

export interface Definition<Payload> extends Spec<Payload> {
  readonly [DefinitionTypeId]: true;
}

export type PayloadOf<D> = D extends Definition<infer Payload> ? Payload : never;

const isPositiveSafeInteger = (value: number): boolean => Number.isSafeInteger(value) && value > 0;

/** Inert declaration. Invalid configuration throws a tagged InvalidConfiguration. */
export function define<Payload>(spec: Spec<Payload>): Definition<Payload> {
  if (!isPositiveSafeInteger(spec.retry.maxAttempts))
    throw new InvalidConfiguration({ detail: "maxAttempts must be a positive safe integer" });
  const { initialDelayMs, factor, maxDelayMs } = spec.retry;

  if (
    !Number.isFinite(initialDelayMs) ||
    initialDelayMs < 0 ||
    !Number.isFinite(factor) ||
    factor < 1 ||
    !Number.isSafeInteger(maxDelayMs) ||
    maxDelayMs < initialDelayMs
  )
    throw new InvalidConfiguration({
      detail:
        "Retry requires finite nonnegative delays, factor >= 1, and a safe integer cap >= initialDelayMs",
    });

  return Object.freeze({
    ...spec,
    [DefinitionTypeId]: true as const,
    retry: Object.freeze({ ...spec.retry }),
  });
}

/** Encode the entire batch before the first storage write. The caller owns its transaction. */
export const enqueue = Effect.fn("Outbox.enqueue")(function* <Payload>(
  definition: Definition<Payload>,
  payloads: readonly Payload[],
) {
  const at = yield* Clock.currentTimeMillis;
  const drafts: OutboxDraft[] = [];
  const json = Schema.fromJsonString(definition.payload);

  for (const payload of payloads) {
    const encodedPayload = yield* Schema.encodeEffect(json)(payload).pipe(
      Effect.mapError(
        (cause) => new PayloadEncodingFailure({ outbox: definition.name, detail: cause.message }),
      ),
    );

    const identity = yield* Effect.try({
      try: () => ({
        idempotencyKey: definition.idempotencyKey(payload),
        partitionId: definition.partitionBy?.(payload) ?? "",
      }),
      catch: (cause) =>
        new PayloadEncodingFailure({ outbox: definition.name, detail: String(cause) }),
    });

    drafts.push({ name: definition.name, ...identity, encodedPayload, enqueuedAtMs: at });
  }

  const store = yield* OutboxStore;
  const report: EnqueueReport = yield* store.enqueue(drafts);

  return report;
});

export interface Delivery<Payload> {
  readonly name: string;
  readonly partitionId: string;
  readonly idempotencyKey: string;
  readonly attempt: number;
  readonly payload: Payload;
}

export type Deliver<Payload, Requirements = never> = (
  delivery: Delivery<Payload>,
) => Effect.Effect<void, DeliveryFailure, Requirements>;

export interface DrainOptions {
  /** Filters the pass to one partition. Partitions of one name still drain one at a time. */
  readonly partitionId?: string;
  readonly limit?: number;
  readonly ordered?: boolean;
}

export interface DrainReport {
  readonly name: string;
  readonly attempted: number;
  readonly delivered: number;
  readonly retried: number;
  readonly deadLettered: number;
  /** The pass stopped at its limit, so due work may remain. */
  readonly more: boolean;
}

const DEFAULT_DRAIN_LIMIT = 100;

const DETAIL_LENGTH = 500;

const permits = new WeakMap<OutboxBacking, Map<string, Semaphore.Semaphore>>();

function permitFor(store: OutboxBacking, name: string): Semaphore.Semaphore {
  let byName = permits.get(store);

  if (byName === undefined) {
    byName = new Map();
    permits.set(store, byName);
  }

  let permit = byName.get(name);

  if (permit === undefined) {
    permit = Semaphore.makeUnsafe(1);
    byName.set(name, permit);
  }

  return permit;
}

/**
 * Clamp the delay before adding; zero delays never multiply an overflowing exponent.
 * A deadline beyond the safe range already means "never", so it clamps too.
 */
function deadline(policy: RetryPolicy, attempt: number, at: number): number {
  const delay =
    policy.initialDelayMs === 0
      ? 0
      : Math.min(policy.maxDelayMs, policy.initialDelayMs * policy.factor ** (attempt - 1));

  return Math.min(at + Math.round(delay), Number.MAX_SAFE_INTEGER);
}

/** Decide what one finished delivery records. Interruption is handled before this. */
function classify(
  exit: Exit.Exit<void, DeliveryFailure>,
  attempt: number,
  policy: RetryPolicy,
  now: number,
): Settlement {
  return Exit.match(exit, {
    onSuccess: () => Settlement.Delivered({ attempts: attempt, atMs: now }),
    onFailure: (cause) => {
      const refusal = Cause.findErrorOption(cause);
      const permanent = Option.exists(refusal, Predicate.isTagged("PermanentDeliveryFailure"));

      const detail = Option.match(refusal, {
        onSome: (failure) => failure.detail,
        onNone: () => Cause.pretty(cause).slice(0, DETAIL_LENGTH),
      });

      if (permanent || attempt >= policy.maxAttempts)
        return Settlement.Dead({
          attempts: attempt,
          reason: permanent ? "permanent" : "attempts-exhausted",
          detail,
          atMs: now,
        });

      return Settlement.Retry({
        attempts: attempt,
        nextAttemptAtMs: deadline(policy, attempt, now),
        detail,
      });
    },
  });
}

const count = (settlements: readonly Settlement[], tag: Settlement["_tag"]): number =>
  Array.filter(settlements, Settlement.$is(tag)).length;

/**
 * One bounded pass. Same-store drains serialize per name across all partitions.
 * Hosts own overlapping scopes across store objects/processes. Permits are non-reentrant.
 */
export const drain = Effect.fn("Outbox.drain")(function* <Payload, Requirements = never>(
  definition: Definition<Payload>,
  deliver: Deliver<Payload, Requirements>,
  options: DrainOptions = {},
) {
  const limit = options.limit ?? DEFAULT_DRAIN_LIMIT;

  if (!isPositiveSafeInteger(limit))
    return yield* new InvalidConfiguration({
      detail: "drain limit must be a positive safe integer",
    });
  const store = yield* OutboxStore;

  return yield* permitFor(store, definition.name).withPermit(
    Effect.gen(function* () {
      const scope = {
        name: definition.name,
        partitionId: options.partitionId,
        ordered: options.ordered,
      };

      const entries = yield* store.readDue(scope, yield* Clock.currentTimeMillis, limit);
      const json = Schema.fromJsonString(definition.payload);
      const settlements: Settlement[] = [];

      for (const entry of entries) {
        if (entry.nextAttemptAtMs > (yield* Clock.currentTimeMillis)) break;
        const attempt = entry.attempts + 1;
        const decoded = yield* Schema.decodeEffect(json)(entry.encodedPayload).pipe(Effect.result);

        if (Result.isFailure(decoded)) {
          const poison = Settlement.Dead({
            attempts: attempt,
            reason: "payload-poison",
            detail: decoded.failure.message.slice(0, DETAIL_LENGTH),
            atMs: yield* Clock.currentTimeMillis,
          });

          yield* store.settle(entry.id, poison);
          settlements.push(poison);
          continue;
        }

        const exit = yield* Effect.exit(
          Effect.suspend(() =>
            deliver({
              name: entry.name,
              partitionId: entry.partitionId,
              idempotencyKey: entry.idempotencyKey,
              attempt,
              payload: decoded.success,
            }),
          ),
        );

        if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)) return yield* Effect.interrupt;

        const settlement = classify(
          exit,
          attempt,
          definition.retry,
          yield* Clock.currentTimeMillis,
        );

        yield* store.settle(entry.id, settlement);
        settlements.push(settlement);

        if (options.ordered === true && Settlement.$is("Retry")(settlement)) break;
      }

      const attempted = settlements.length;

      const report: DrainReport = {
        name: definition.name,
        attempted,
        delivered: count(settlements, "Delivered"),
        retried: count(settlements, "Retry"),
        deadLettered: count(settlements, "Dead"),
        more: attempted >= limit,
      };

      return report;
    }),
  );
});
