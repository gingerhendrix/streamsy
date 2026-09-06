import { Schema } from "effect";
import {
  StreamId,
  Offset,
  ProducerId,
  StreamRecord,
  StoredMessage,
  ProducerState,
  RecordPatch,
} from "../schema/index.ts";
export const ProducerPrecondition = Schema.Struct({
  producerId: ProducerId,
  expected: Schema.Option(ProducerState), // None = insert only if absent
  next: ProducerState,
});

export const Operation = Schema.Union([
  Schema.TaggedStruct("Create", {
    record: StreamRecord,
    initialMessages: Schema.Array(StoredMessage),
    forkSource: Schema.optionalKey(Schema.Struct({ id: StreamId, liveAtOffset: Offset })),
  }),
  Schema.TaggedStruct("Append", {
    streamId: StreamId,
    messages: Schema.Array(StoredMessage),
    patch: RecordPatch,
    expectedOffset: Schema.optionalKey(Offset),
    expectedClosed: Schema.optionalKey(Schema.Boolean),
    producer: Schema.optionalKey(ProducerPrecondition),
  }),
  Schema.TaggedStruct("Delete", {
    streamId: StreamId,
    reason: Schema.Literals(["delete", "expiry"]),
    expectedExpiresAtMs: Schema.optionalKey(Schema.Number),
  }),
]);

export const Mutation = Schema.Struct({ operations: Schema.NonEmptyArray(Operation) });

export const OperationResult = Schema.Union([
  Schema.TaggedStruct("Created", { record: StreamRecord }),
  Schema.TaggedStruct("Appended", { record: StreamRecord }),
  Schema.TaggedStruct("Purged", { record: StreamRecord }),
  Schema.TaggedStruct("SoftDeleted", { record: StreamRecord }),
]);

export const MutationOutcome = Schema.Union([
  Schema.TaggedStruct("Applied", { results: Schema.NonEmptyArray(OperationResult) }),
  Schema.TaggedStruct("Rejected", {
    index: Schema.Number,
    reason: Schema.Literals([
      "offset",
      "closed",
      "producer",
      "exists",
      "not-found",
      "gone",
      "fork-source-gone",
      "expiry-mismatch",
    ]),
    record: Schema.Option(StreamRecord),
  }),
]);

export type ProducerPrecondition = typeof ProducerPrecondition.Type;

export type Operation = typeof Operation.Type;

export type Mutation = typeof Mutation.Type;

export type OperationResult = typeof OperationResult.Type;

export type MutationOutcome = typeof MutationOutcome.Type;
