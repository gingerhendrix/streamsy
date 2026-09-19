/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console -- Executable Bun process boundary writes its JSON protocol to stdout. */
import { Clock, Effect, Layer, ManagedRuntime, Option } from "effect";
import {
  ProducerId,
  Protocol,
  Storage,
  StreamId,
  StreamsReader,
  StreamsWriter,
  ZERO_OFFSET,
  type ProtocolError,
} from "@streamsy/core";
import { layer } from "@streamsy/storage/bun";

const argument = (index: number, name: string): string => {
  const value = Bun.argv[index + 2];
  if (value === undefined) throw new Error(`Missing ${name}`);
  return value;
};

const optional = (index: number): string | undefined => {
  const value = Bun.argv[index + 2];
  return value === "-" ? undefined : value;
};

const integer = (index: number, name: string): number => {
  const value = Number(argument(index, name));
  if (!Number.isSafeInteger(value)) throw new Error(`Invalid ${name}`);
  return value;
};

const storageRecord = (id: StreamId) => ({
  id,
  config: { contentType: "text/plain", createdAt: 0 },
  lifecycle: { closed: false, softDeleted: false },
  currentOffset: ZERO_OFFSET,
});

const mode = argument(0, "mode");
const filename = argument(1, "filename");
const storageLayer = layer({ client: { filename } });
const applicationLayer = Protocol.layer({ longPollTimeoutMs: 500 }).pipe(
  Layer.provideMerge(storageLayer),
);
const runtime = ManagedRuntime.make(applicationLayer);

const operation = Effect.gen(function* () {
  const reader = yield* StreamsReader;
  const writer = yield* StreamsWriter;
  switch (mode) {
    case "create": {
      const initial = optional(3);
      return yield* writer.create(StreamId.make(argument(2, "id")), {
        contentType: "text/plain",
        initialData: initial === undefined ? undefined : new TextEncoder().encode(initial),
        ttlSeconds: optional(4) === undefined ? undefined : integer(4, "ttlSeconds"),
        expiresAt: optional(5),
      });
    }
    case "fork":
      return yield* writer.fork(
        StreamId.make(argument(2, "id")),
        StreamId.make(argument(3, "source")),
        {
          forkOffset: optional(4),
          forkSubOffset: optional(5) === undefined ? undefined : integer(5, "forkSubOffset"),
        },
      );
    case "append": {
      const waitUntil = integer(9, "waitUntil");
      const now = yield* Clock.currentTimeMillis;
      if (waitUntil > now) yield* Effect.sleep(waitUntil - now);
      const producerId = optional(5);
      return yield* writer.append(StreamId.make(argument(2, "id")), {
        data: new TextEncoder().encode(argument(3, "data")),
        contentType: "text/plain",
        expectedOffset: optional(4),
        producer:
          producerId === undefined
            ? undefined
            : {
                producerId,
                producerEpoch: integer(6, "producerEpoch"),
                producerSeq: integer(7, "producerSeq"),
              },
      });
    }
    case "read": {
      const result = yield* reader.read(StreamId.make(argument(2, "id")));
      return {
        ...result,
        messages: result.messages.map(({ data }) => ({
          text: new TextDecoder().decode(data),
        })),
      };
    }
    case "head":
      return yield* reader.head(StreamId.make(argument(2, "id")));
    case "producer":
      return yield* (yield* Storage).producer(
        StreamId.make(argument(2, "id")),
        ProducerId.make(argument(3, "producerId")),
      );
    case "multi-fail": {
      const storage = yield* Storage;
      const fresh = StreamId.make(argument(2, "freshId"));
      const existing = StreamId.make(argument(3, "existingId"));
      const result = yield* Effect.flip(
        storage.mutate({
          operations: [
            { _tag: "Create", record: storageRecord(fresh), initialMessages: [] },
            { _tag: "Create", record: storageRecord(existing), initialMessages: [] },
          ],
        }),
      );
      return { result, freshAbsent: Option.isNone(yield* storage.record(fresh)) };
    }
    default:
      return yield* Effect.die(new Error(`Unknown mode: ${mode}`));
  }
});

try {
  console.log(
    JSON.stringify(
      await runtime.runPromise(
        operation.pipe(
          Effect.catchTags({
            StreamNotFound: (error: ProtocolError) => Effect.succeed(error),
            StreamGone: (error: ProtocolError) => Effect.succeed(error),
            StreamBusy: (error: ProtocolError) => Effect.succeed(error),
            StreamClosed: (error: ProtocolError) => Effect.succeed(error),
            OffsetMismatch: (error: ProtocolError) => Effect.succeed(error),
            AppendConflict: (error: ProtocolError) => Effect.succeed(error),
            StaleEpoch: (error: ProtocolError) => Effect.succeed(error),
            ProducerGap: (error: ProtocolError) => Effect.succeed(error),
            InvalidEpochSeq: (error: ProtocolError) => Effect.succeed(error),
            InvalidAppendRequest: (error: ProtocolError) => Effect.succeed(error),
            CreateConflict: (error: ProtocolError) => Effect.succeed(error),
            ForkSourceNotFound: (error: ProtocolError) => Effect.succeed(error),
            InvalidForkRequest: (error: ProtocolError) => Effect.succeed(error),
            NotSupported: (error: ProtocolError) => Effect.succeed(error),
          }),
        ),
      ),
    ),
  );
} finally {
  await runtime.dispose();
}
