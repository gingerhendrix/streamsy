import { Context, Deferred, Effect, Exit, Layer } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";

/**
 * A protocol Layer over memory storage that records a parked long-poll read and
 * holds it open until `hold` completes. The drain cases use it so the parked
 * read is signalled, rather than racing a client request that may not have
 * reached the server yet.
 */
export const parkableLayer = (
  readStarted: Deferred.Deferred<void>,
  hold: Deferred.Deferred<void>,
) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const reader = yield* StreamsReader;
      const writer = yield* StreamsWriter;

      return Context.make(StreamsWriter, writer).pipe(
        Context.add(
          StreamsReader,
          StreamsReader.of({
            ...reader,
            readNext: (id, readOptions) =>
              Effect.suspend(() => {
                Deferred.doneUnsafe(readStarted, Exit.void);

                return Deferred.await(hold).pipe(Effect.andThen(reader.readNext(id, readOptions)));
              }),
          }),
        ),
      );
    }),
  ).pipe(Layer.provide(MemoryStorage.layerProtocol({ longPollTimeoutMs: 30_000 })));

export const memoryLayer = MemoryStorage.layerProtocol({ longPollTimeoutMs: 30_000 });
