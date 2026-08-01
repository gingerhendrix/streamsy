import { encodeStreamIdentity } from "../causal.ts";
import type { ProducerLane } from "./lane.ts";

export const MESH_RESERVED_TYPE_PREFIX = "__streamsy.";
export const MESH_LINEAGE_TYPE = "__streamsy.mesh.lineage.v1";
export const MESH_LINEAGE_KEY = "checkpoint";
export const MESH_LINEAGE_FORMAT = "streamsy.mesh.lineage.v1";

export interface MeshLineageValue {
  readonly format: typeof MESH_LINEAGE_FORMAT;
  readonly processorId: string;
  readonly processorVersion: string;
  readonly outputGeneration: string;
  readonly sourceIdentity: string;
  readonly targetIdentity: string;
  readonly sourceThrough: string;
  readonly producerId: string;
  readonly producerEpoch: number;
  /** Sequence to use for the next append batch. */
  readonly nextProducerSeq: number;
}

export interface MeshLineageEvent {
  readonly type: typeof MESH_LINEAGE_TYPE;
  readonly key: typeof MESH_LINEAGE_KEY;
  readonly value: MeshLineageValue;
  readonly headers: { readonly operation: "upsert" };
}

export interface LineageCheckpoint {
  readonly sourceThrough: string;
  readonly nextProducerSeq: number;
}

export function createLineageEvent(
  lane: ProducerLane,
  checkpoint: LineageCheckpoint,
): MeshLineageEvent {
  validateRealPosition(checkpoint.sourceThrough);
  validateSequence(checkpoint.nextProducerSeq);
  return {
    type: MESH_LINEAGE_TYPE,
    key: MESH_LINEAGE_KEY,
    value: {
      format: MESH_LINEAGE_FORMAT,
      processorId: lane.processorId,
      processorVersion: lane.processorVersion,
      outputGeneration: lane.outputGeneration,
      sourceIdentity: encodeStreamIdentity(lane.source),
      targetIdentity: encodeStreamIdentity(lane.target),
      sourceThrough: checkpoint.sourceThrough,
      producerId: lane.producerId,
      producerEpoch: lane.producerEpoch,
      nextProducerSeq: checkpoint.nextProducerSeq,
    },
    headers: { operation: "upsert" },
  };
}

/** Decode and validate durable metadata. Throws only inside recovery's typed-error boundary. */
export function decodeLineageEvent(value: unknown): MeshLineageEvent {
  if (!isRecord(value)) throw new TypeError("Lineage event must be an object");
  if (value.type !== MESH_LINEAGE_TYPE || value.key !== MESH_LINEAGE_KEY) {
    throw new TypeError("Lineage event has an invalid reserved type or key");
  }
  if (!isRecord(value.headers) || value.headers.operation !== "upsert") {
    throw new TypeError("Lineage event must be an upsert");
  }
  if (!isRecord(value.value)) throw new TypeError("Lineage event value must be an object");
  const row = value.value;
  if (row.format !== MESH_LINEAGE_FORMAT) throw new TypeError("Unsupported lineage format");
  for (const field of [
    "processorId",
    "processorVersion",
    "outputGeneration",
    "sourceIdentity",
    "targetIdentity",
    "sourceThrough",
    "producerId",
  ] as const) {
    if (typeof row[field] !== "string" || row[field].length === 0) {
      throw new TypeError(`Lineage ${field} must be a non-empty string`);
    }
  }
  validateRealPosition(row.sourceThrough);
  validateSequence(row.producerEpoch, "producerEpoch");
  validateSequence(row.nextProducerSeq);
  return value as unknown as MeshLineageEvent;
}

export function assertLineageCompatible(event: MeshLineageEvent, lane: ProducerLane): void {
  const expected = createLineageEvent(lane, {
    sourceThrough: event.value.sourceThrough,
    nextProducerSeq: event.value.nextProducerSeq,
  }).value;
  for (const field of [
    "format",
    "processorId",
    "processorVersion",
    "outputGeneration",
    "sourceIdentity",
    "targetIdentity",
    "producerId",
    "producerEpoch",
  ] as const) {
    if (event.value[field] !== expected[field]) {
      throw new TypeError(`Lineage ${field} is incompatible with the configured lane`);
    }
  }
}

export function assertFactTypeAllowed(value: unknown): void {
  if (!isRecord(value) || typeof value.type !== "string" || value.type.length === 0) {
    throw new TypeError("State fact event requires a non-empty type");
  }
  if (value.type.startsWith(MESH_RESERVED_TYPE_PREFIX)) {
    throw new TypeError(`State fact event type uses reserved prefix ${MESH_RESERVED_TYPE_PREFIX}`);
  }
}

function validateRealPosition(value: unknown): asserts value is string {
  if (typeof value !== "string" || value.length === 0 || value === "-1" || value === "now") {
    throw new TypeError("sourceThrough must be a real durable-stream position");
  }
}

function validateSequence(value: unknown, name = "nextProducerSeq"): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
