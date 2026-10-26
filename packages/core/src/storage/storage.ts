// The accepted storage contract explicitly names StorageShape.
/* oxlint-disable anti-slop/no-shape-in-symbol-names */
import { Context, type Effect, type Option, type PubSub, type Scope, type Stream } from "effect";
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
import type { Mutation, MutationApplied, MutationRejected } from "./mutation.ts";

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
  ) => Effect.Effect<MutationApplied, MutationRejected | StorageFault>;
  readonly changes: (id: StreamId) => Stream.Stream<ChangeSnapshot, StorageFault>;
  /**
   * Subscribe to the stream ids of later local commits. The subscription exists when
   * this effect returns, so a commit after that point always lands in it. Each item
   * holds the ids that one commit created, appended to or deleted, after that commit.
   * A rolled-back mutation reports nothing. In process and best effort: a commit by
   * another process on the same database does not show, and nothing replays after a
   * restart. The scope releases the subscription.
   */
  readonly committed: Effect.Effect<
    PubSub.Subscription<ReadonlyArray<StreamId>>,
    never,
    Scope.Scope
  >;
  readonly nextExpiry: Effect.Effect<
    Option.Option<{ readonly at: number; readonly streamId: StreamId }>,
    StorageFault
  >;
}

export class Storage extends Context.Service<Storage, StorageShape>()("@streamsy/core/Storage") {}
