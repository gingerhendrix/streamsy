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
import { Effect } from "effect";

export const COUNTER_COLLECTION = "counter-contribution";

export interface CounterIncrement {
  readonly counterId: string;
  readonly delta: number;
}

export interface CounterContribution {
  readonly counterId: string;
  readonly delta: number;
  readonly sourcePosition: string;
}

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

/** Append one source fact through the fixed binding and return its exact acknowledgement. */
export const appendCounterIncrement = Effect.fn("CausalCounter.appendIncrement")(function* (
  source: StreamBinding,
  increment: CounterIncrement,
) {
  const validated = validateIncrement(increment);
  const appends = yield* AppendStreams;
  const result = yield* appends.append(source, JSON.stringify(validated), {
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
  return batch.items.map((item) => validateIncrement(item));
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
    const self = this;
    return Effect.fn("CausalCounter.EagerConsumer.catchUp")(function* () {
      const reads = yield* ReadStreams;
      const opened = yield* reads.open(target, {
        ...(self.targetResume === undefined ? {} : { offset: self.targetResume }),
        live: false,
      });
      if (opened.status !== "ok") throw new Error(`Counter target read failed: ${opened.status}`);
      yield* Effect.gen(function* () {
        while (true) {
          const next = yield* opened.session.next;
          if (next.done) break;
          const batch = next.value;
          if (batch.kind !== "json") throw new TypeError("Counter target must be JSON State");
          if (batch.items.length === 0) continue;
          yield* self.applyTransaction(batch.items, batch.offset, observer);
        }
        const ended = yield* opened.session.done;
        if (ended.status !== "done") return yield* Effect.interrupt;
      }).pipe(Effect.ensuring(opened.session.cancel("counter consumer complete")));
    })();
  }

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

  private applyTransaction(
    events: readonly JsonValue[],
    targetResume: string,
    observer?: CounterObserver,
  ) {
    const self = this;
    return Effect.fn("CausalCounter.EagerConsumer.applyTransaction")(function* () {
      const contributions = new Map(self.contributions);
      let lineage = self.lineage;
      for (const event of events) {
        if (!isRecord(event) || typeof event.type !== "string") {
          throw new TypeError("State event must have a type");
        }
        if (event.type === COUNTER_COLLECTION) {
          const decoded = decodeContribution(event);
          contributions.set(decoded.key, decoded.value);
        } else if (event.type === MESH_LINEAGE_TYPE) {
          lineage = yield* decodeLineageEvent(event);
        } else {
          throw new TypeError(`Unregistered State collection: ${event.type}`);
        }
      }
      self.contributions = contributions;
      self.lineage = lineage;
      self.targetResume = targetResume;
      observer?.(self.view());
    })();
  }
}

function validateIncrement(value: unknown): CounterIncrement {
  if (!isRecord(value) || typeof value.counterId !== "string" || value.counterId.length === 0) {
    throw new TypeError("Counter increment requires a non-empty counterId");
  }
  if (typeof value.delta !== "number" || !Number.isSafeInteger(value.delta)) {
    throw new TypeError("Counter increment delta must be a safe integer");
  }
  return { counterId: value.counterId, delta: value.delta };
}

function decodeContribution(event: Record<string, unknown>): {
  readonly key: string;
  readonly value: CounterContribution;
} {
  if (typeof event.key !== "string" || event.key.length === 0) {
    throw new TypeError("Counter contribution requires a key");
  }
  if (!isRecord(event.headers) || event.headers.operation !== "upsert") {
    throw new TypeError("Counter contribution must be an upsert");
  }
  const value = validateContribution(event.value);
  return { key: event.key, value };
}

function validateContribution(value: unknown): CounterContribution {
  const increment = validateIncrement(value);
  if (!isRecord(value) || typeof value.sourcePosition !== "string") {
    throw new TypeError("Counter contribution requires a source position");
  }
  return { ...increment, sourcePosition: value.sourcePosition };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
