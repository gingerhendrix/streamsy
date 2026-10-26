import { it as baseIt, layer } from "@effect/vitest";
import { Commit, Transaction } from "@streamsy/projection";
import { Protocol } from "@streamsy/core/protocol";
import { expect } from "vitest";
import { Array, Context, Deferred, Effect, Fiber, Layer, Option, Schema, Stream } from "effect";
import { Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { memoryHost } from "./support/memory-host.ts";

class Source extends Streamsy.json<Source>()("OwnerComposition/Source", "memory/source/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}

class Output extends Streamsy.json<Output>()("OwnerComposition/Output", "sqlite/output/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}

const memory = memoryHost();

const sqlite = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
);

const host = Layer.merge(
  Protocol.bind(memory, Streamsy.group(Source)),
  Protocol.bind(sqlite, Streamsy.group(Output)),
).pipe(Layer.provideMerge(sqlite));

layer(host)((it) => {
  it.effect(
    "an transactional projection reads remote input and writes on its SQLite checkpoint owner",
    () =>
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
              const doubled = Array.map(batch.input.items, (n) => n * 2);

              if (Array.isReadonlyArrayNonEmpty(doubled)) yield* output.append(params, doubled);
            }),
        }).at({});

        const first = yield* Projection.run(projection);
        expect(first.items).toBe(3);
        expect(
          Array.flatMap(yield* Stream.runCollect(output.read(params)), (batch) => batch.items),
        ).toEqual([2, 4, 6]);
        expect((yield* Projection.run(projection)).items).toBe(0);
      }),
  );
});

layer(host)((it) => {
  it.effect("follow wakes from an input with a different checkpoint owner", () =>
    Effect.gen(function* () {
      const source = yield* Source;
      const params = { name: "follow" };
      yield* source.create(params);

      const projection = Projection.process({
        commit: Commit.transaction,
        id: "mixed-follow",
        params: {},
        inputs: { input: Source.at(params) },
        process: () => Effect.void,
      }).at({});

      const parked = yield* Deferred.make<void>();

      const fiber = yield* Stream.runLast(Projection.follow(projection)).pipe(
        Effect.provideService(Source, {
          ...source,
          wire: {
            ...source.wire,
            readNext: (id, options) =>
              Deferred.succeed(parked, undefined).pipe(
                Effect.andThen(source.wire.readNext(id, options)),
              ),
          },
        }),
        Effect.forkScoped,
      );

      yield* Deferred.await(parked);
      yield* source.append(params, [1], { close: true });
      expect(Option.getOrThrow(yield* Fiber.join(fiber)).status).toBe("source-closed");
    }),
  );
});

const memoryGraph = memoryHost();

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

baseIt.effect("a custom stream wire cannot establish the owner of an transactional write", () =>
  Effect.gen(function* () {
    const unownedHost = Protocol.bind(unowned, Streamsy.group(Source, Output)).pipe(
      Layer.provideMerge(sqlite),
    );

    yield* Effect.gen(function* () {
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
        Array.flatMap(yield* Stream.runCollect(output.read(params)), (batch) => batch.items),
      ).toEqual([]);
      expect((yield* (yield* CheckpointStore).load(projection)).token).toBe("0");
    }).pipe(Effect.provideContext(yield* Layer.build(unownedHost)));
  }),
);
