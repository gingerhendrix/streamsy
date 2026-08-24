import type { ClientAppendResult, JsonValue } from "@streamsy/core";
import { Context, Effect, Layer, Schema } from "effect";
import { streamIdentityEquals } from "../causal.ts";
import type { StreamBinding } from "../binding.ts";
import {
  AppendStreams,
  type AppendOutcome,
  ReadStreams,
  type ReadStreamsService,
} from "../effect/streams.ts";
import { IncompatibleLineage, MalformedLineage, type StreamReadError } from "../effect/errors.ts";
import type { ProducerLane } from "./lane.ts";
import {
  MESH_LINEAGE_TYPE,
  MESH_RESERVED_TYPE_PREFIX,
  createLineageEvent,
  decodeLineageEvent,
  ensureLineageCompatible,
  type MeshLineageEvent,
} from "./state-meta.ts";
import { decodeStateFact } from "./schemas.ts";

const isJsonObject = Schema.is(Schema.Record(Schema.String, Schema.Json));
const isString = Schema.is(Schema.String);

export interface RecoveredDerivedState {
  readonly status: "ready";
  readonly targetOffset: string;
  readonly sourceThrough?: string;
  readonly nextProducerSeq: number;
  readonly producerId: string;
  readonly producerEpoch: number;
}

export type DerivedRecoveryResult =
  | RecoveredDerivedState
  | { readonly status: "not-found" | "gone" };
export type DerivedRecoveryError = StreamReadError | MalformedLineage | IncompatibleLineage;

export interface DerivedRecoveryService {
  readonly recover: (
    target: StreamBinding,
    lane: ProducerLane,
  ) => Effect.Effect<DerivedRecoveryResult, DerivedRecoveryError>;
}

export class DerivedRecovery extends Context.Service<DerivedRecovery, DerivedRecoveryService>()(
  "@streamsy/experimental/DerivedRecovery",
) {}

/**
 * Recovered lineage plus the durable application facts that produced the
 * current target State.
 *
 * Recovery scans complete target history, so restoration cost is O(history).
 * Snapshots are deliberately deferred.
 */
export interface RecoveredDerivedHistory {
  readonly status: "ready";
  readonly checkpoint: RecoveredDerivedState;
  readonly facts: readonly JsonValue[];
}

export type DerivedHistoryResult =
  | RecoveredDerivedHistory
  | { readonly status: "not-found" | "gone" };

export interface DerivedStateHistoryService {
  readonly recoverHistory: (
    target: StreamBinding,
    lane: ProducerLane,
  ) => Effect.Effect<DerivedHistoryResult, DerivedRecoveryError>;
}

export class DerivedStateHistory extends Context.Service<
  DerivedStateHistory,
  DerivedStateHistoryService
>()("@streamsy/experimental/DerivedStateHistory") {}

export interface AppendDerivedStateBatchOptions {
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly previous: RecoveredDerivedState;
  readonly sourceThrough: string;
  readonly facts: readonly JsonValue[];
}

export type AppendDerivedStateResult =
  | {
      readonly status: "appended" | "sequence-already-accepted";
      readonly offset: string;
      readonly checkpoint: RecoveredDerivedState;
    }
  | { readonly status: "not-found" | "gone" }
  | { readonly status: "output-conflict"; readonly reason: string; readonly offset?: string }
  | Extract<ClientAppendResult, { status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq" }>;

export const DerivedRecoveryLive = Layer.effect(
  DerivedRecovery,
  Effect.gen(function* () {
    const reads = yield* ReadStreams;
    const scan = makeScan(reads);
    return DerivedRecovery.of({
      recover: Effect.fn("DerivedRecovery.recover")((target, lane) =>
        scan(target, lane).pipe(
          Effect.map((result) => (result.status === "ready" ? result.checkpoint : result)),
        ),
      ),
    });
  }),
);

export const DerivedStateHistoryLive = Layer.effect(
  DerivedStateHistory,
  Effect.gen(function* () {
    const reads = yield* ReadStreams;
    return DerivedStateHistory.of({ recoverHistory: makeScan(reads) });
  }),
);

/** Deterministic recovery capability for processor tests. */
export const DerivedRecoveryTest = (recover: DerivedRecoveryService["recover"]) =>
  Layer.succeed(DerivedRecovery, DerivedRecovery.of({ recover }));

export const DerivedStateHistoryTest = (
  recoverHistory: DerivedStateHistoryService["recoverHistory"],
) => Layer.succeed(DerivedStateHistory, DerivedStateHistory.of({ recoverHistory }));

const makeScan = (reads: ReadStreamsService) =>
  Effect.fn("DerivedStateHistory.recoverHistory")((target: StreamBinding, lane: ProducerLane) =>
    Effect.gen(function* () {
      assertTargetMatchesLane(target, lane);
      const opened = yield* reads.open(target);
      if (opened.status !== "ok") return opened;
      const session = opened.session;
      if (session.startOffset === undefined) {
        return yield* new MalformedLineage({
          message: "Derived State recovery read did not provide a start offset",
          cause: opened,
        });
      }
      let targetOffset = session.startOffset;
      let lineage: MeshLineageEvent | undefined;
      let lastWasLineage = false;
      let sawItems = false;
      const facts: JsonValue[] = [];

      const pull = Effect.gen(function* () {
        while (true) {
          const next = yield* session.next;
          if (next.done) break;
          const batch = next.value;
          targetOffset = batch.offset;
          if (batch.kind !== "json") {
            return yield* new IncompatibleLineage({
              message: "Derived State target is not JSON",
            });
          }
          for (const item of batch.items) {
            sawItems = true;
            lastWasLineage = false;
            if (!isJsonObject(item) || !isString(item.type)) {
              facts.push(item);
              continue;
            }
            if (item.type === MESH_LINEAGE_TYPE) {
              const decoded = yield* decodeLineageEvent(item);
              yield* ensureLineageCompatible(decoded, lane);
              lineage = decoded;
              lastWasLineage = true;
            } else if (item.type.startsWith(MESH_RESERVED_TYPE_PREFIX)) {
              return yield* new IncompatibleLineage({
                message: `Unknown reserved State type ${item.type}`,
              });
            } else {
              facts.push(item);
            }
          }
        }
        const ended = yield* session.done;
        if (ended.status === "cancelled") return yield* Effect.interrupt;
        if (sawItems && (!lineage || !lastWasLineage)) {
          return yield* new IncompatibleLineage({
            message: "Derived State history does not end at a lineage transaction boundary",
          });
        }
        const checkpointBase = {
          status: "ready" as const,
          targetOffset,
          nextProducerSeq: lineage?.value.nextProducerSeq ?? 0,
          producerId: lane.producerId,
          producerEpoch: lane.producerEpoch,
        };
        const recoveredCheckpoint: RecoveredDerivedState =
          lineage === undefined
            ? checkpointBase
            : { ...checkpointBase, sourceThrough: lineage.value.sourceThrough };
        return {
          status: "ready" as const,
          checkpoint: recoveredCheckpoint,
          facts,
        };
      });
      return yield* pull;
    }).pipe(Effect.scoped),
  );

export const recoverDerivedState = Effect.fn("recoverDerivedState")(function* (
  target: StreamBinding,
  lane: ProducerLane,
) {
  const recovery = yield* DerivedRecovery;
  return yield* recovery.recover(target, lane);
});

export const recoverDerivedStateHistory = Effect.fn("recoverDerivedStateHistory")(function* (
  target: StreamBinding,
  lane: ProducerLane,
) {
  const history = yield* DerivedStateHistory;
  return yield* history.recoverHistory(target, lane);
});

export const appendDerivedStateBatch = Effect.fn("appendDerivedStateBatch")(function* (
  options: AppendDerivedStateBatchOptions,
) {
  validateAppendInput(options);
  const appends = yield* AppendStreams;
  const recovery = yield* DerivedRecovery;
  const nextProducerSeq = options.previous.nextProducerSeq + 1;
  if (!Number.isSafeInteger(nextProducerSeq)) throw new TypeError("Producer sequence exhausted");
  const metadata = createLineageEvent(options.lane, {
    sourceThrough: options.sourceThrough,
    nextProducerSeq,
  });
  const result = yield* appends.appendJsonBatch(options.target, [...options.facts, metadata], {
    expectedOffset: options.previous.targetOffset,
    producer: {
      producerId: options.lane.producerId,
      producerEpoch: options.lane.producerEpoch,
      producerSeq: options.previous.nextProducerSeq,
    },
  });
  if (result.status === "appended") {
    return {
      status: "appended" as const,
      offset: result.offset,
      checkpoint: checkpoint(options, result.offset, nextProducerSeq),
    };
  }
  if (result.status === "duplicate") {
    return yield* reconcileDuplicate(options, result, nextProducerSeq, recovery);
  }
  return classifyAppendOutcome(result);
});

function reconcileDuplicate(
  options: AppendDerivedStateBatchOptions,
  duplicate: Extract<AppendOutcome, { status: "duplicate" }>,
  nextProducerSeq: number,
  recovery: DerivedRecoveryService,
) {
  return Effect.gen(function* () {
    if (
      duplicate.producerEpoch !== options.lane.producerEpoch ||
      duplicate.producerSeq !== options.previous.nextProducerSeq
    ) {
      return yield* new IncompatibleLineage({
        message: "Duplicate producer high-water does not match the submitted batch sequence",
      });
    }
    const recovered = yield* recovery.recover(options.target, options.lane);
    if (recovered.status !== "ready") return recovered;
    if (
      recovered.targetOffset !== duplicate.offset ||
      recovered.sourceThrough !== options.sourceThrough ||
      recovered.nextProducerSeq !== nextProducerSeq
    ) {
      return yield* new IncompatibleLineage({
        message: "In-band lineage does not reconcile the accepted producer sequence",
      });
    }
    return {
      status: "sequence-already-accepted" as const,
      offset: duplicate.offset,
      checkpoint: recovered,
    };
  });
}

function classifyAppendOutcome(
  result: Exclude<AppendOutcome, { status: "appended" | "duplicate" }>,
): AppendDerivedStateResult {
  if (result.status === "conflict") {
    const conflict: Extract<AppendDerivedStateResult, { status: "output-conflict" }> = {
      status: "output-conflict",
      reason: result.conflictReason,
    };
    if ("offset" in result) return { ...conflict, offset: result.offset };
    return conflict;
  }
  if (result.status === "closed")
    return { status: "output-conflict", reason: "closed", offset: result.offset };
  return result;
}

function validateAppendInput(options: AppendDerivedStateBatchOptions): void {
  assertTargetMatchesLane(options.target, options.lane);
  if (
    options.previous.producerId !== options.lane.producerId ||
    options.previous.producerEpoch !== options.lane.producerEpoch
  ) {
    throw new TypeError("Recovered state does not belong to the configured producer lane");
  }
  if (
    options.previous.sourceThrough !== undefined &&
    options.sourceThrough <= options.previous.sourceThrough
  ) {
    throw new TypeError("sourceThrough must advance beyond the recovered checkpoint");
  }
  for (const fact of options.facts) {
    decodeStateFact(fact);
  }
}

function checkpoint(
  options: AppendDerivedStateBatchOptions,
  targetOffset: string,
  nextProducerSeq: number,
): RecoveredDerivedState {
  return {
    status: "ready",
    targetOffset,
    sourceThrough: options.sourceThrough,
    nextProducerSeq,
    producerId: options.lane.producerId,
    producerEpoch: options.lane.producerEpoch,
  };
}

function assertTargetMatchesLane(target: StreamBinding, lane: ProducerLane): void {
  if (!streamIdentityEquals(target.identity, lane.target))
    throw new TypeError("Target binding identity does not match the producer lane");
}
