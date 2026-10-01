import { Commit } from "@streamsy/projection";
import { Protocol } from "@streamsy/core/protocol";
import { expect, test } from "bun:test";
import { Array, Cause, Context, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import { Storage, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import * as BunStorage from "@streamsy/storage/bun";
import { CheckpointStore, Projection } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as ProjectionSql from "@streamsy/projection/sql";

class Source extends Streamsy.json<Source>()("OwnerComposition/Source", "memory/source/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}
class Output extends Streamsy.json<Output>()("OwnerComposition/Output", "sqlite/output/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}
const memory = Memory.layerMemory();
const sqlite = ProjectionSql.layerStorage.pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
);
const host = Layer.merge(
  Protocol.bind(memory, Streamsy.group(Source)),
  Protocol.bind(sqlite, Streamsy.group(Output)),
).pipe(Layer.provideMerge(sqlite));

test("an transactional projection reads remote input and writes on its SQLite checkpoint owner", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const source = yield* Source;
      const output = yield* Output;
      const params = { name: "orders" };
      yield* source.create(params);
      yield* output.create(params);
      yield* source.append(params, [1, 2, 3]);
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "mixed-double",
        params: {},
        inputs: { input: Source.at(params) },
        process: (batch) =>
          Effect.gen(function* () {
            const doubled = batch.input.items.map((n) => n * 2);
            if (Array.isReadonlyArrayNonEmpty(doubled)) yield* output.append(params, doubled);
          }),
      }).at({});
      const first = yield* Projection.run(projection);
      expect(first.items).toBe(3);
      expect(
        (yield* Stream.runCollect(output.read(params))).flatMap((batch) => batch.items),
      ).toEqual([2, 4, 6]);
      expect((yield* Projection.run(projection)).items).toBe(0);
    }).pipe(Effect.provide(host)),
  );
});

test("watch watches the input's feed even when checkpoints have a different owner", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const source = yield* Source;
      const params = { name: "watch" };
      yield* source.create(params);
      yield* source.append(params, [1], { close: true });
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "mixed-watch",
        params: {},
        inputs: { input: Source.at(params) },
        process: () => Effect.void,
      }).at({});
      const fiber = yield* Projection.watch(projection);
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
    }).pipe(Effect.provide(host), Effect.scoped),
  );
});

test("watch rejects a stream rebound without its matching feed", async () => {
  const alternate = Protocol.bind(Memory.layerMemory(), Streamsy.group(Source)).pipe(
    Layer.flatMap((context) => Layer.succeed(Source, Context.get(context, Source))),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "rebound-feed",
        params: {},
        inputs: { input: Source.at({ name: "watch" }) },
        process: () => Effect.void,
      }).at({});
      const fiber = yield* Projection.watch(projection).pipe(Effect.provide(alternate));
      const result = yield* Fiber.join(fiber).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure")
        expect(result.failure).toMatchObject({ reason: "unsupported-composition" });
    }).pipe(Effect.provide(host), Effect.scoped),
  );
});

const memoryGraph = Memory.layerMemory();
// A custom adapter that copies protocol methods loses private storage-owner metadata.
const unowned = Layer.effectContext(
  Effect.gen(function* () {
    const reader = yield* StreamsReader;
    const writer = yield* StreamsWriter;
    return Context.make(StreamsReader, { ...reader }).pipe(
      Context.add(StreamsWriter, { ...writer }),
    );
  }),
).pipe(Layer.provide(memoryGraph));

test("a custom stream wire cannot establish the owner of an transactional write", async () => {
  const unownedHost = Protocol.bind(unowned, Streamsy.group(Source, Output)).pipe(
    Layer.provideMerge(sqlite),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const source = yield* Source;
      const output = yield* Output;
      const params = { name: "orders" };
      yield* source.create(params);
      yield* source.append(params, [1]);
      yield* output.create(params);
      const projection = Projection.process({
        commit: Commit.transaction,
        id: "unowned-write",
        params: {},
        inputs: { input: Source.at(params) },
        process: () => output.append(params, [2]),
      }).at({});
      const result = yield* Projection.run(projection).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure).toMatchObject({ reason: "unsupported-composition" });
        expect(result.failure.message).toContain("Cannot establish a storage owner");
      }
      expect(
        (yield* Stream.runCollect(output.read(params))).flatMap((batch) => batch.items),
      ).toEqual([]);
      expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("0");
    }).pipe(Effect.provide(unownedHost)),
  );
});

test("a custom wire graph exposing ambient Storage is rejected during binding", async () => {
  const storageOnly = memoryGraph.pipe(
    Layer.flatMap((context) => Layer.succeed(Storage, Context.get(context, Storage))),
  );
  const mixed = Layer.merge(unowned, storageOnly);
  const result = await Effect.runPromiseExit(
    Effect.void.pipe(Effect.provide(Protocol.bind(mixed, Streamsy.group(Source, Output)))),
  );
  expect(Exit.isFailure(result)).toBe(true);
  if (Exit.isFailure(result))
    expect(String(Cause.squash(result.cause))).toContain(
      "The exposed Storage does not own the protocol wires",
    );
});
