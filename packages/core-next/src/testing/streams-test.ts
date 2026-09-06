import { Context, Effect, Layer, Queue, Ref, Stream } from "effect";
import { Storage } from "../storage/storage.ts";
import * as Memory from "../storage/memory/layer.ts";
import * as Protocol from "../protocol/layer.ts";
import type { ChangeSnapshot, StreamId } from "../schema/index.ts";

export class StreamsTest extends Context.Service<
  StreamsTest,
  {
    readonly storage: typeof Storage.Service;
    readonly subscribers: Effect.Effect<number>;
    readonly snapshot: Effect.Effect<{ readonly id: StreamId; readonly value: ChangeSnapshot }>;
  }
>()("@streamsy/core/StreamsTest") {}

/** Real protocol and memory, with deterministic observation at the scoped changes boundary. */
export const layerTest = (options: Memory.MemoryOptions & Protocol.ProtocolOptions = {}) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const storage = yield* Storage;
      const count = yield* Ref.make(0);
      const snapshots = yield* Effect.acquireRelease(
        Queue.unbounded<{ id: StreamId; value: ChangeSnapshot }>(),
        Queue.shutdown,
      );
      const observed = Storage.of({
        ...storage,
        changes: (id) =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* Effect.acquireRelease(
                Ref.update(count, (n) => n + 1),
                () => Ref.update(count, (n) => n - 1),
              );
              return storage
                .changes(id)
                .pipe(Stream.tap((value) => Queue.offer(snapshots, { id, value })));
            }),
          ),
      });
      const context = yield* Layer.build(
        Protocol.layer(options).pipe(Layer.provide(Layer.succeed(Storage, observed))),
      );
      return Context.add(
        context,
        StreamsTest,
        StreamsTest.of({
          storage: observed,
          subscribers: Ref.get(count),
          snapshot: Queue.take(snapshots),
        }),
      );
    }),
  ).pipe(Layer.provide(Memory.layer(options)));
