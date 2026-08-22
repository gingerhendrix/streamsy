import type { JsonValue } from "@streamsy/core";
import { Effect, Schema } from "effect";
import { encodeStreamIdentity, type StreamIdentity } from "../causal.ts";
import { IncompatibleLineage, MalformedLineage } from "../effect/errors.ts";
import type { ProducerLane } from "./lane.ts";
import { DurablePosition, NonNegativeInt } from "./schemas.ts";
import { MESH_RESERVED_TYPE_PREFIX } from "./state-meta.ts";

/** Versioned recovery law implemented by the dynamic fan-in kernel. */
export const FAN_IN_STATE_KIND = "dynamic-fan-in-state/v1";

export const FAN_IN_CHECKPOINT_TYPE = "__streamsy.mesh.fan-in.checkpoint.v1";
export const FAN_IN_CHECKPOINT_KEY = "checkpoint";
export const FAN_IN_CHECKPOINT_FORMAT = "streamsy.mesh.fan-in.checkpoint.v1";
export const FAN_IN_MEMBER_TYPE = "__streamsy.mesh.fan-in.member.v1";
export const FAN_IN_MEMBER_FORMAT = "streamsy.mesh.fan-in.member.v1";

export const FanInCheckpointValue = Schema.Struct({
  format: Schema.Literal(FAN_IN_CHECKPOINT_FORMAT),
  kind: Schema.Literal(FAN_IN_STATE_KIND),
  processorId: Schema.NonEmptyString,
  processorVersion: Schema.NonEmptyString,
  outputGeneration: Schema.NonEmptyString,
  membershipIdentity: Schema.NonEmptyString,
  targetIdentity: Schema.NonEmptyString,
  producerId: Schema.NonEmptyString,
  producerEpoch: NonNegativeInt,
  nextProducerSeq: NonNegativeInt,
  /** Membership-source position incorporated by this transaction. */
  membershipThrough: Schema.NullOr(DurablePosition),
});
export interface FanInCheckpointValue extends Schema.Schema.Type<typeof FanInCheckpointValue> {}

export const FanInCheckpointEvent = Schema.Struct({
  type: Schema.Literal(FAN_IN_CHECKPOINT_TYPE),
  key: Schema.Literal(FAN_IN_CHECKPOINT_KEY),
  value: FanInCheckpointValue,
  headers: Schema.Struct({ operation: Schema.Literal("upsert") }),
});
export interface FanInCheckpointEvent extends Schema.Schema.Type<typeof FanInCheckpointEvent> {}

export const FanInMemberValue = Schema.Struct({
  format: Schema.Literal(FAN_IN_MEMBER_FORMAT),
  memberIdentity: Schema.NonEmptyString,
  /** Declared start position; `null` reads the member from its beginning. */
  from: Schema.NullOr(DurablePosition),
  /** Member position already incorporated into the target State. */
  through: Schema.NullOr(DurablePosition),
});
export interface FanInMemberValue extends Schema.Schema.Type<typeof FanInMemberValue> {}

export const FanInMemberEvent = Schema.Struct({
  type: Schema.Literal(FAN_IN_MEMBER_TYPE),
  key: Schema.NonEmptyString,
  value: FanInMemberValue,
  headers: Schema.Struct({ operation: Schema.Literal("upsert") }),
});
export interface FanInMemberEvent extends Schema.Schema.Type<typeof FanInMemberEvent> {}

/** Durable identity and cursors for one active fan-in member. */
export interface FanInMember {
  readonly identity: StreamIdentity;
  readonly from: string | null;
  readonly through: string | null;
}

export const MEMBER_KEY_PREFIX = "member:";

export function memberRowKey(identity: StreamIdentity): string {
  return `${MEMBER_KEY_PREFIX}${encodeStreamIdentity(identity)}`;
}

export function createMemberRow(member: FanInMember): FanInMemberEvent {
  return FanInMemberEvent.make({
    type: FAN_IN_MEMBER_TYPE,
    key: memberRowKey(member.identity),
    value: {
      format: FAN_IN_MEMBER_FORMAT,
      memberIdentity: encodeStreamIdentity(member.identity),
      from: member.from,
      through: member.through,
    },
    headers: { operation: "upsert" },
  });
}

export function removeMemberRow(identity: StreamIdentity): JsonValue {
  return {
    type: FAN_IN_MEMBER_TYPE,
    key: memberRowKey(identity),
    headers: { operation: "delete" },
  };
}

export function createFanInCheckpoint(
  lane: ProducerLane,
  checkpoint: { readonly membershipThrough: string | null; readonly nextProducerSeq: number },
): FanInCheckpointEvent {
  return FanInCheckpointEvent.make({
    type: FAN_IN_CHECKPOINT_TYPE,
    key: FAN_IN_CHECKPOINT_KEY,
    value: {
      format: FAN_IN_CHECKPOINT_FORMAT,
      kind: FAN_IN_STATE_KIND,
      processorId: lane.processorId,
      processorVersion: lane.processorVersion,
      outputGeneration: lane.outputGeneration,
      membershipIdentity: encodeStreamIdentity(lane.source),
      targetIdentity: encodeStreamIdentity(lane.target),
      producerId: lane.producerId,
      producerEpoch: lane.producerEpoch,
      nextProducerSeq: checkpoint.nextProducerSeq,
      membershipThrough: checkpoint.membershipThrough,
    },
    headers: { operation: "upsert" },
  });
}

export const decodeFanInCheckpoint = Effect.fn("MeshFanIn.decodeCheckpoint")(function* (
  value: unknown,
) {
  return yield* Schema.decodeUnknownEffect(FanInCheckpointEvent)(value).pipe(
    Effect.mapError(
      (cause) => new MalformedLineage({ message: "Malformed fan-in checkpoint row", cause }),
    ),
  );
});

export const decodeFanInMember = Effect.fn("MeshFanIn.decodeMember")(function* (value: unknown) {
  const event = yield* Schema.decodeUnknownEffect(FanInMemberEvent)(value).pipe(
    Effect.mapError(
      (cause) => new MalformedLineage({ message: "Malformed fan-in member row", cause }),
    ),
  );
  if (event.key !== `${MEMBER_KEY_PREFIX}${event.value.memberIdentity}`) {
    return yield* new MalformedLineage({
      message: "Fan-in member row key does not match its member identity",
      cause: event,
    });
  }
  return event;
});

export const ensureFanInCheckpointCompatible = Effect.fn("MeshFanIn.ensureCompatible")(function* (
  event: FanInCheckpointEvent,
  lane: ProducerLane,
) {
  const expected = createFanInCheckpoint(lane, {
    membershipThrough: event.value.membershipThrough,
    nextProducerSeq: event.value.nextProducerSeq,
  }).value;
  for (const field of [
    "format",
    "kind",
    "processorId",
    "processorVersion",
    "outputGeneration",
    "membershipIdentity",
    "targetIdentity",
    "producerId",
    "producerEpoch",
  ] as const) {
    if (event.value[field] !== expected[field]) {
      return yield* new IncompatibleLineage({
        message: `Fan-in checkpoint ${field} is incompatible with the configured lane`,
      });
    }
  }
  return undefined;
});

export function isReservedType(type: string): boolean {
  return type.startsWith(MESH_RESERVED_TYPE_PREFIX);
}

/** Canonical member order: encoded identity ascending, then source position. */
export function compareMembers(a: FanInMember, b: FanInMember): -1 | 0 | 1 {
  const left = encodeStreamIdentity(a.identity);
  const right = encodeStreamIdentity(b.identity);
  return left === right ? 0 : left < right ? -1 : 1;
}
