import { Effect, Schema } from "effect";
import { encodeStreamIdentity } from "../causal.ts";
import { IncompatibleLineage, MalformedLineage } from "../effect/errors.ts";
import type { ProducerLane } from "./lane.ts";

export const MESH_RESERVED_TYPE_PREFIX = "__streamsy.";
export const MESH_LINEAGE_TYPE = "__streamsy.mesh.lineage.v1";
export const MESH_LINEAGE_KEY = "checkpoint";
export const MESH_LINEAGE_FORMAT = "streamsy.mesh.lineage.v1";

export const MeshLineageValue = Schema.Struct({
  format: Schema.Literal(MESH_LINEAGE_FORMAT),
  processorId: Schema.NonEmptyString,
  processorVersion: Schema.NonEmptyString,
  outputGeneration: Schema.NonEmptyString,
  sourceIdentity: Schema.NonEmptyString,
  targetIdentity: Schema.NonEmptyString,
  sourceThrough: Schema.NonEmptyString,
  producerId: Schema.NonEmptyString,
  producerEpoch: Schema.Int,
  nextProducerSeq: Schema.Int,
});
export interface MeshLineageValue extends Schema.Schema.Type<typeof MeshLineageValue> {}

export const MeshLineageEvent = Schema.Struct({
  type: Schema.Literal(MESH_LINEAGE_TYPE),
  key: Schema.Literal(MESH_LINEAGE_KEY),
  value: MeshLineageValue,
  headers: Schema.Struct({ operation: Schema.Literal("upsert") }),
});
export interface MeshLineageEvent extends Schema.Schema.Type<typeof MeshLineageEvent> {}

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
  return MeshLineageEvent.make({
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
  });
}

/** Decode unknown durable lineage and keep all validation failures typed. */
export const decodeLineageEvent = Effect.fn("MeshLineage.decode")(function* (value: unknown) {
  const event = yield* Schema.decodeUnknownEffect(MeshLineageEvent)(value).pipe(
    Effect.mapError(
      (cause) => new MalformedLineage({ message: "Malformed mesh lineage event", cause }),
    ),
  );
  if (
    event.value.sourceThrough === "-1" ||
    event.value.sourceThrough === "now" ||
    event.value.producerEpoch < 0 ||
    event.value.nextProducerSeq < 0
  ) {
    return yield* new MalformedLineage({
      message: "Lineage positions and producer sequences must be durable and non-negative",
      cause: event,
    });
  }
  return event;
});

export const ensureLineageCompatible = Effect.fn("MeshLineage.ensureCompatible")(function* (
  event: MeshLineageEvent,
  lane: ProducerLane,
) {
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
      return yield* new IncompatibleLineage({
        message: `Lineage ${field} is incompatible with the configured lane`,
      });
    }
  }
});

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
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
