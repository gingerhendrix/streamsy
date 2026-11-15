import { Array, Context, Effect, Layer, Schema } from "effect";
import { Storage } from "@streamsy/core";
import { Protocol, Streams, StreamsReader } from "@streamsy/core/protocol";
import { Commit, Projection, type Entry, type Slice } from "@streamsy/projection";
import { json, provideReader } from "./families.ts";

const input = json("entry-offsets", { schema: Schema.Finite });

const idle = json("entry-offsets-idle", { schema: Schema.Finite });

/** Run unchanged on memory, Bun SQLite and same-object Durable Object SQLite. */
export const entryOffsets = Effect.gen(function* () {
  yield* Streams.create(input);
  yield* Streams.create(idle);
  yield* Streams.append(input, [10, 20, 30, 40, 50]);
  const storage = yield* Storage;
  const stored = yield* storage.messages(input.id, {});
  const expected = Array.map(stored, (message) => message.offset);
  const slices: Array<Slice<number>> = [];
  const entries: Array<Entry<{ input: typeof input; idle: typeof idle }>> = [];
  const keys: Array<string> = [];
  const skippedOffsets: Array<ReadonlyArray<string> | undefined> = [];
  let fail = true;

  const projection = Projection.process({
    id: "entry-offsets",
    params: {},
    inputs: { input, idle },
    commit: Commit.atLeastOnce,
    process: (batch, unit) =>
      Effect.gen(function* () {
        slices.push(batch.input);
        skippedOffsets.push(batch.idle.offsets);
        entries.push(...Projection.items(batch));
        keys.push(unit.key);

        if (fail) return yield* Effect.fail("crash after pin");

        return undefined;
      }),
  }).at({});

  const first = yield* Projection.pass(projection).pipe(Effect.result);
  fail = false;
  // Replay the three-item pin using two-item pages: two pages, with an over-read.
  const context = yield* Layer.build(Protocol.layer({ readLimit: 2 }));
  const reader = Context.get(context, StreamsReader);
  const replay = yield* Projection.pass(projection).pipe(provideReader(reader));
  const rest = yield* Projection.run(projection);

  return {
    expected,
    slices,
    entries,
    keys,
    skippedOffsets,
    first: first._tag,
    replay: replay.items,
    rest: rest.items,
  };
});
