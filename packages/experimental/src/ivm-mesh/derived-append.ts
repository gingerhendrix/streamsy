import type { ClientAppendResult, ClientFailure, JsonValue } from "@streamsy/core";
import { streamIdentityEquals } from "../causal.ts";
import type { StreamBinding } from "../binding.ts";
import type { ProducerLane } from "./lane.ts";
import {
  MESH_LINEAGE_TYPE,
  MESH_RESERVED_TYPE_PREFIX,
  assertFactTypeAllowed,
  assertLineageCompatible,
  createLineageEvent,
  decodeLineageEvent,
  type MeshLineageEvent,
} from "./state-meta.ts";

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
  | { readonly status: "not-found" | "gone" | "cancelled" }
  | { readonly status: "malformed-output" | "incompatible-output"; readonly message: string }
  | ClientFailure;

export interface AppendDerivedStateBatchOptions {
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  /** Durable state returned before constructing this deterministic transition. */
  readonly previous: RecoveredDerivedState;
  readonly sourceThrough: string;
  readonly facts: readonly JsonValue[];
  readonly signal?: AbortSignal;
}

export type AppendDerivedStateResult =
  | {
      readonly status: "appended" | "sequence-already-accepted";
      readonly offset: string;
      readonly checkpoint: RecoveredDerivedState;
    }
  | { readonly status: "not-found" | "gone" | "cancelled" }
  | { readonly status: "output-conflict"; readonly reason: string; readonly offset?: string }
  | { readonly status: "malformed-output" | "incompatible-output"; readonly message: string }
  | Extract<ClientAppendResult, { status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq" }>
  | ClientFailure;

/**
 * Recover the authoritative target tail and latest in-band lineage by reading
 * State history. This incubation path is intentionally O(output history).
 */
export async function recoverDerivedState(
  target: StreamBinding,
  lane: ProducerLane,
  signal?: AbortSignal,
): Promise<DerivedRecoveryResult> {
  assertTargetMatchesLane(target, lane);
  const read = await target.client.stream(target.streamId).read<JsonValue>({ signal });
  if (read.status !== "ok") return read;

  let targetOffset = read.session.startOffset ?? "-1";
  let lineage: MeshLineageEvent | undefined;
  let lastWasLineage = false;
  let sawItems = false;
  try {
    for await (const batch of read.session) {
      targetOffset = batch.offset;
      if (batch.kind !== "json") {
        read.session.cancel("derived State target is not JSON");
        return { status: "incompatible-output", message: "Derived State target is not JSON" };
      }
      for (const item of batch.items) {
        sawItems = true;
        lastWasLineage = false;
        if (!isRecord(item) || typeof item.type !== "string") continue;
        if (item.type === MESH_LINEAGE_TYPE) {
          try {
            const decoded = decodeLineageEvent(item);
            assertLineageCompatible(decoded, lane);
            lineage = decoded;
            lastWasLineage = true;
          } catch (error) {
            return classifiedMetadataFailure(error);
          }
        } else if (item.type.startsWith(MESH_RESERVED_TYPE_PREFIX)) {
          return {
            status: "incompatible-output",
            message: `Unknown reserved State type ${item.type}`,
          };
        }
      }
    }
  } catch (error) {
    return failure("transport", "Failed while reading derived State history", true, error);
  }

  const ended = await read.session.done;
  if (ended.status === "error") return ended;
  if (ended.status === "cancelled") return { status: "cancelled" };
  if (sawItems && (!lineage || !lastWasLineage)) {
    return {
      status: "incompatible-output",
      message: "Derived State history does not end at a lineage transaction boundary",
    };
  }
  return {
    status: "ready",
    targetOffset,
    sourceThrough: lineage?.value.sourceThrough,
    nextProducerSeq: lineage?.value.nextProducerSeq ?? 0,
    producerId: lane.producerId,
    producerEpoch: lane.producerEpoch,
  };
}

/** Append one deterministic State transaction through the fixed client seam. */
export async function appendDerivedStateBatch(
  options: AppendDerivedStateBatchOptions,
): Promise<AppendDerivedStateResult> {
  validateAppendInput(options);
  const nextProducerSeq = options.previous.nextProducerSeq + 1;
  if (!Number.isSafeInteger(nextProducerSeq)) throw new TypeError("Producer sequence exhausted");
  const metadata = createLineageEvent(options.lane, {
    sourceThrough: options.sourceThrough,
    nextProducerSeq,
  });
  const events: JsonValue[] = [...options.facts, metadata as unknown as JsonValue];
  const handle = options.target.client.stream(options.target.streamId);
  const result = await handle.appendJsonBatch(events, {
    signal: options.signal,
    expectedOffset: options.previous.targetOffset,
    producer: {
      producerId: options.lane.producerId,
      producerEpoch: options.lane.producerEpoch,
      producerSeq: options.previous.nextProducerSeq,
    },
  });
  if (result.status === "appended") {
    return {
      status: "appended",
      offset: result.offset,
      checkpoint: checkpoint(options, result.offset, nextProducerSeq),
    };
  }
  if (result.status === "duplicate") {
    return reconcileDuplicate(options, result, nextProducerSeq);
  }
  if (result.status === "conflict") {
    return {
      status: "output-conflict",
      reason: result.conflictReason,
      ...(result.status === "conflict" && "offset" in result ? { offset: result.offset } : {}),
    };
  }
  if (result.status === "closed") {
    return { status: "output-conflict", reason: "closed", offset: result.offset };
  }
  return result;
}

async function reconcileDuplicate(
  options: AppendDerivedStateBatchOptions,
  duplicate: Extract<ClientAppendResult, { status: "duplicate" }>,
  nextProducerSeq: number,
): Promise<AppendDerivedStateResult> {
  if (
    duplicate.producerEpoch !== options.lane.producerEpoch ||
    duplicate.producerSeq !== options.previous.nextProducerSeq
  ) {
    return {
      status: "incompatible-output",
      message: "Duplicate producer high-water does not match the submitted batch sequence",
    };
  }
  const recovered = await recoverDerivedState(options.target, options.lane, options.signal);
  if (recovered.status !== "ready") return recovered;
  if (
    recovered.targetOffset !== duplicate.offset ||
    recovered.sourceThrough !== options.sourceThrough ||
    recovered.nextProducerSeq !== nextProducerSeq
  ) {
    return {
      status: "incompatible-output",
      message: "In-band lineage does not reconcile the accepted producer sequence",
    };
  }
  // This proves compatible durable progress, not equality with the retried payload.
  return { status: "sequence-already-accepted", offset: duplicate.offset, checkpoint: recovered };
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
    assertFactTypeAllowed(fact);
    validateStateFact(fact);
  }
}

function validateStateFact(value: JsonValue): void {
  if (!isRecord(value) || typeof value.key !== "string" || value.key.length === 0) {
    throw new TypeError("State fact event requires a non-empty key");
  }
  if (!isRecord(value.headers)) throw new TypeError("State fact event requires headers");
  const operation = value.headers.operation;
  if (!["insert", "update", "upsert", "delete"].includes(String(operation))) {
    throw new TypeError("State fact event has an invalid operation");
  }
  if (operation !== "delete" && !("value" in value)) {
    throw new TypeError(`${String(operation)} State fact event requires value`);
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
  if (!streamIdentityEquals(target.identity, lane.target)) {
    throw new TypeError("Target binding identity does not match the producer lane");
  }
}

function classifiedMetadataFailure(error: unknown): DerivedRecoveryResult {
  const message = error instanceof Error ? error.message : "Invalid lineage metadata";
  return {
    status: message.includes("incompatible") ? "incompatible-output" : "malformed-output",
    message,
  };
}

function failure(
  code: ClientFailure["code"],
  message: string,
  retryable: boolean,
  cause?: unknown,
): ClientFailure {
  return { status: "error", code, message, retryable, cause };
}

function isRecord(value: unknown): value is Record<string, JsonValue> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
