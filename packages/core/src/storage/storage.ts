// The accepted storage contract explicitly names StorageShape.
/* oxlint-disable anti-slop/no-shape-in-symbol-names */
import { Context, type Effect, type Option, type Stream } from "effect";
import type { StorageFault } from "../fault.ts";
import type {
  StreamId,
  StreamRecord,
  StoredMessage,
  MessageWindow,
  ProducerId,
  ProducerState,
  ChangeSnapshot,
} from "../schema/index.ts";
import type { StorageCapabilities } from "./capabilities.ts";
import type { Mutation, MutationOutcome, MutationRejected } from "./mutation.ts";
export interface StorageShape {
  readonly capabilities: StorageCapabilities;
  readonly record: (id: StreamId) => Effect.Effect<Option.Option<StreamRecord>, StorageFault>;
  readonly messages: (
    id: StreamId,
    window: MessageWindow,
  ) => Effect.Effect<ReadonlyArray<StoredMessage>, StorageFault>;
  readonly producer: (
    id: StreamId,
    producerId: ProducerId,
  ) => Effect.Effect<Option.Option<ProducerState>, StorageFault>;
  readonly mutate: (
    mutation: Mutation,
  ) => Effect.Effect<MutationOutcome, MutationRejected | StorageFault>;
  readonly changes: (id: StreamId) => Stream.Stream<ChangeSnapshot, StorageFault>;
  readonly nextExpiry: Effect.Effect<
    Option.Option<{ readonly at: number; readonly streamId: StreamId }>,
    StorageFault
  >;
}
export class Storage extends Context.Service<Storage, StorageShape>()("@streamsy/core/Storage") {}
