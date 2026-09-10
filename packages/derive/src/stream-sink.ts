import { encodeKey } from "./identity.ts";
import { Effect } from "effect";
import { Streams, StreamsWriter, ZERO_OFFSET, type StreamRef } from "@streamsy/core";
import { Commit } from "./commit.ts";
import { DeriveFault } from "./fault.ts";
import type { Sink } from "./sink.ts";

/** The caller provisions a dedicated, initially empty stream on this exact host. */
export const make = Effect.fn("Derive.StreamSink.make")(function* <A>(ref: StreamRef.StreamRef<A>) {
  const owner = yield* Commit;
  const writer = yield* StreamsWriter;
  return {
    identity: encodeKey([ref.id, ref.contentType]),
    initialPosition: ZERO_OFFSET,
    owner,
    write: Effect.fn("Derive.StreamSink.write")(function* (outputs, previousPosition) {
      if (outputs.length === 0) return previousPosition;
      const result = yield* Streams.append(ref, outputs, { expectedOffset: previousPosition }).pipe(
        Effect.provideService(StreamsWriter, writer),
        Effect.mapError(
          () => new DeriveFault({ reason: "storage-failure", message: `Cannot append ${ref.id}` }),
        ),
      );
      if (result.status !== "appended")
        return yield* new DeriveFault({
          reason: "sink-conflict",
          message: `Sink ${ref.id}: ${result.status}`,
        });
      return result.offset;
    }),
  } satisfies Sink<A>;
});
