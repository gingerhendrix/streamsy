import type { JsonValue, StreamBatch } from "@streamsy/core";
import type { StreamBinding } from "@streamsy/experimental/binding";
import {
  coverage,
  decodeStreamIdentity,
  sourceAck,
  sourceWatermark,
  type Coverage,
  type SourceAck,
} from "@streamsy/experimental/causal";
import { AppendStreams, ReadStreams } from "@streamsy/experimental/effect";
import {
  MESH_LINEAGE_TYPE,
  catchUp,
  decodeLineageEvent,
  type CatchUpLimits,
  type MeshLineageEvent,
  type ProducerLane,
  type ProjectionBoundary,
} from "@streamsy/experimental/ivm-mesh";
import { Effect, Schema } from "effect";

export const COUNTER_COLLECTION = "counter-contribution";

const CounterValueFields = {
  counterId: Schema.NonEmptyString,
  delta: Schema.Int,
};

export const CounterIncrement = Schema.Struct(CounterValueFields);
export interface CounterIncrement extends Schema.Schema.Type<typeof CounterIncrement> {}

export const CounterContribution = Schema.Struct({
  ...CounterValueFields,
  sourcePosition: Schema.NonEmptyString,
});
export interface CounterContribution extends Schema.Schema.Type<typeof CounterContribution> {}

export const CounterContributionEvent = Schema.Struct({
  type: Schema.Literal(COUNTER_COLLECTION),
  key: Schema.NonEmptyString,
  value: CounterContribution,
  headers: Schema.Struct({ operation: Schema.Literal("upsert") }),
});
export interface CounterContributionEvent extends Schema.Schema.Type<
  typeof CounterContributionEvent
> {}

export class MalformedCounterState extends Schema.TaggedError<MalformedCounterState>()(
  "MalformedCounterState",
  { message: Schema.String, cause: Schema.Defect() },
) {}

export class UnregisteredCounterStateCollection extends Schema.TaggedError<UnregisteredCounterStateCollection>()(
  "UnregisteredCounterStateCollection",
  { collection: Schema.String },
) {}

export type CounterConsumerResult =
  | { readonly status: "caught-up" }
  | { readonly status: "not-found" | "gone" };

export interface CounterConsumerSnapshot {
  readonly contributions: readonly (readonly [string, CounterContribution])[];
  readonly lineage?: MeshLineageEvent;
  readonly targetResume?: string;
}

export interface CounterConsumerView {
  readonly counters: Readonly<Record<string, number>>;
  readonly lineage?: MeshLineageEvent;
  readonly targetResume?: string;
}

export type CounterObserver = (view: CounterConsumerView) => void;

const defaultLimits: CatchUpLimits = {
  maxItems: 100,
  maxPages: 100,
  maxBatches: 100,
  maxBytes: 100_000,
};

/**
 * Wire form of one source fact.
 *
 * The same schema decodes source batches and encodes the exact appended JSON.
 */
const CounterIncrementJson = Schema.fromJsonString(CounterIncrement);
const encodeIncrement = Schema.encodeSync(CounterIncrementJson);
const decodeIncrement = Schema.decodeUnknownSync(CounterIncrement);
const StateEventEnvelope = Schema.Struct({ type: Schema.String });

/** Append one source fact through the fixed binding and return its exact acknowledgement. */
export const appendCounterIncrement = Effect.fn("CausalCounter.appendIncrement")(function* (
  source: StreamBinding,
  increment: CounterIncrement,
) {
  const validated = decodeIncrement(increment);
  const appends = yield* AppendStreams;
  const result = yield* appends.append(source, encodeIncrement(validated), {
    contentType: "application/json",
  });
  return result.status === "appended"
    ? { ...result, ack: sourceAck(source.identity, result.offset) }
    : result;
});

/** Run the example's bounded one-source projection once. */
export function projectCounterIncrements(options: {
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly limits?: CatchUpLimits;
}) {
  return catchUp({
    source: options.source,
    target: options.target,
    lane: options.lane,
    limits: options.limits ?? defaultLimits,
    decode: decodeIncrements,
    reduce: contributionEvents,
  });
}

function decodeIncrements(batch: StreamBatch): readonly CounterIncrement[] {
  if (batch.kind !== "json") throw new TypeError("Counter source must be JSON");
  return batch.items.map((item) => decodeIncrement(item));
}

function contributionEvents(
  increments: readonly CounterIncrement[],
  boundary: ProjectionBoundary,
): readonly JsonValue[] {
  return increments.map((increment, index) => ({
    type: COUNTER_COLLECTION,
    key: `${encodeURIComponent(increment.counterId)}:${boundary.source.position}:${index}`,
    value: {
      counterId: increment.counterId,
      delta: increment.delta,
      sourcePosition: boundary.source.position,
    },
    headers: { operation: "upsert" },
  }));
}

/**
 * Example-local eager State consumer.
 *
 * It explicitly registers only counter contributions and mesh lineage. Each
 * delivered JSON batch is validated and applied to copies before the visible
 * state, resume position, and observer notification advance together.
 */
export class EagerCounterConsumer {
  private contributions: Map<string, CounterContribution>;
  private lineage: MeshLineageEvent | undefined;
  private targetResume: string | undefined;

  constructor(snapshot?: CounterConsumerSnapshot) {
    this.contributions = new Map(snapshot?.contributions ?? []);
    this.lineage = snapshot?.lineage;
    this.targetResume = snapshot?.targetResume;
  }

  catchUp(target: StreamBinding, observer?: CounterObserver) {
    return EagerCounterConsumer.catchUpEffect(this, target, observer);
  }

  private static readonly catchUpEffect = Effect.fn("CausalCounter.EagerConsumer.catchUp")(
    (consumer: EagerCounterConsumer, target: StreamBinding, observer?: CounterObserver) =>
      Effect.gen(function* () {
        const reads = yield* ReadStreams;
        const opened = yield* reads.open(target, {
          ...(consumer.targetResume === undefined ? {} : { offset: consumer.targetResume }),
          live: false,
        });
        if (opened.status !== "ok") return opened;
        while (true) {
          const next = yield* opened.session.next;
          if (next.done) break;
          const batch = next.value;
          if (batch.kind !== "json") {
            return yield* new MalformedCounterState({
              message: "Counter target must be JSON State",
              cause: batch,
            });
          }
          if (batch.items.length === 0) continue;
          yield* EagerCounterConsumer.applyTransactionEffect(
            consumer,
            batch.items,
            batch.offset,
            observer,
          );
        }
        const ended = yield* opened.session.done;
        if (ended.status !== "done") return yield* Effect.interrupt;
        return { status: "caught-up" as const };
      }).pipe(Effect.scoped),
  );

  counterValue(counterId: string): number | undefined {
    let total = 0;
    let found = false;
    for (const contribution of this.contributions.values()) {
      if (contribution.counterId !== counterId) continue;
      total += contribution.delta;
      found = true;
    }
    return found ? total : undefined;
  }

  syncedThrough(ack: SourceAck): Coverage {
    if (!this.lineage) return { status: "not-yet" };
    return coverage(
      sourceWatermark(
        decodeStreamIdentity(this.lineage.value.sourceIdentity),
        this.lineage.value.sourceThrough,
      ),
      ack,
    );
  }

  snapshot(): CounterConsumerSnapshot {
    return {
      contributions: Array.from(this.contributions.entries()),
      ...(this.lineage === undefined ? {} : { lineage: this.lineage }),
      ...(this.targetResume === undefined ? {} : { targetResume: this.targetResume }),
    };
  }

  view(): CounterConsumerView {
    const counters: Record<string, number> = {};
    for (const contribution of this.contributions.values()) {
      counters[contribution.counterId] =
        (counters[contribution.counterId] ?? 0) + contribution.delta;
    }
    return {
      counters,
      ...(this.lineage === undefined ? {} : { lineage: this.lineage }),
      ...(this.targetResume === undefined ? {} : { targetResume: this.targetResume }),
    };
  }

  private static readonly applyTransactionEffect = Effect.fn(
    "CausalCounter.EagerConsumer.applyTransaction",
  )(function* (
    consumer: EagerCounterConsumer,
    events: readonly JsonValue[],
    targetResume: string,
    observer?: CounterObserver,
  ) {
    const contributions = new Map(consumer.contributions);
    let lineage = consumer.lineage;
    for (const event of events) {
      const envelope = yield* Schema.decodeUnknownEffect(StateEventEnvelope)(event).pipe(
        Effect.mapError(
          (cause) => new MalformedCounterState({ message: "State event must have a type", cause }),
        ),
      );
      if (envelope.type === COUNTER_COLLECTION) {
        const decoded = yield* Schema.decodeUnknownEffect(CounterContributionEvent)(event).pipe(
          Effect.mapError(
            (cause) =>
              new MalformedCounterState({ message: "Malformed counter contribution", cause }),
          ),
        );
        contributions.set(decoded.key, decoded.value);
      } else if (envelope.type === MESH_LINEAGE_TYPE) {
        lineage = yield* decodeLineageEvent(event);
      } else {
        return yield* new UnregisteredCounterStateCollection({ collection: envelope.type });
      }
    }
    consumer.contributions = contributions;
    consumer.lineage = lineage;
    consumer.targetResume = targetResume;
    observer?.(consumer.view());
    return undefined;
  });
}
