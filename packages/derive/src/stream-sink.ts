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
  const conflict = (error: {
    readonly _tag: string;
    readonly id: string;
    readonly message?: string;
  }) =>
    new DeriveFault({
      reason: "sink-conflict",
      message: `Sink ${error.id}: ${error.message || error._tag}`,
    });
  return {
    identity: encodeKey([ref.id, ref.contentType]),
    initialPosition: ZERO_OFFSET,
    owner,
    write: Effect.fn("Derive.StreamSink.write")(function* (outputs, previousPosition) {
      if (outputs.length === 0) return previousPosition;
      const result = yield* Streams.append(ref, outputs, { expectedOffset: previousPosition }).pipe(
        Effect.provideService(StreamsWriter, writer),
        Effect.catchTags({
          OffsetMismatch: (error) =>
            new DeriveFault({
              reason: "sink-conflict",
              message: `Sink ${error.id}: expected ${error.expected}, actual ${error.actual}`,
            }),
          StreamNotFound: conflict,
          StreamGone: conflict,
          StreamClosed: conflict,
          AppendConflict: conflict,
          StreamBusy: conflict,
          StaleEpoch: conflict,
          ProducerGap: conflict,
          InvalidEpochSeq: conflict,
          InvalidAppendRequest: conflict,
          NotSupported: conflict,
          EncodeFault: () =>
            new DeriveFault({ reason: "storage-failure", message: `Cannot append ${ref.id}` }),
          StorageFault: () =>
            new DeriveFault({ reason: "storage-failure", message: `Cannot append ${ref.id}` }),
          TransportFault: () =>
            new DeriveFault({ reason: "storage-failure", message: `Cannot append ${ref.id}` }),
        }),
      );
      return result.offset;
    }),
  } satisfies Sink<A>;
});
