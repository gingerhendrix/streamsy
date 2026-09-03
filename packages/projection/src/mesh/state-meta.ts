import type { JsonValue } from "@streamsy/core";
import { Effect, Schema } from "effect";
import { encodeStreamIdentity } from "@streamsy/streams/identity";
import { IncompatibleLineage, MalformedLineage } from "@streamsy/streams";
import type { ProducerLane } from "./lane.ts";
import { DurablePosition, NonNegativeInt, StateFact } from "./schemas.ts";

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
  sourceThrough: DurablePosition,
  producerId: Schema.NonEmptyString,
  producerEpoch: NonNegativeInt,
  nextProducerSeq: NonNegativeInt,
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
export const decodeLineageEvent = Effect.fn("MeshLineage.decode")(function* (value: JsonValue) {
  return yield* Schema.decodeUnknownEffect(MeshLineageEvent)(value).pipe(
    Effect.mapError(
      (cause) => new MalformedLineage({ message: "Malformed mesh lineage event", cause }),
    ),
  );
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
  return undefined;
});

export { StateFact };
