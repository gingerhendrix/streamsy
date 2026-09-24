import { expect, test } from "bun:test";
import { Context, Effect, Fiber, Layer, Schema, Stream } from "effect";
import { provides, StreamFamily } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Projection } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";

class Source extends StreamFamily.json<Source>()("routed.test/Source", "memory/source/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}
class Output extends StreamFamily.json<Output>()("routed.test/Output", "sqlite/output/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
}) {}
const memory = Memory.layerMemory();
const sqlite = Sqlite.layer.pipe(
  Layer.provideMerge(BunStorage.layerProtocol({ client: { filename: ":memory:" } })),
);
const host = Layer.merge(provides(memory, Source), provides(sqlite, Output)).pipe(
  Layer.provideMerge(sqlite),
);

test("a fused projection reads remote input and writes on its SQLite checkpoint owner", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const source = yield* Source;
      const output = yield* Output;
      const params = { name: "orders" };
      yield* source.create(params);
      yield* output.create(params);
      yield* source.append(params, [1, 2, 3]);
      const projection = Projection.make({
        id: "routed-double",
        input: Source.at(params),
        process: (batch) =>
          output.append(
            params,
            batch.input.items.map((n) => n * 2),
          ),
      });
      const first = yield* Projection.run(projection);
      expect(first.items).toBe(3);
      expect(
        (yield* Stream.runCollect(output.read(params))).flatMap((batch) => batch.items),
      ).toEqual([2, 4, 6]);
      expect((yield* Projection.run(projection)).items).toBe(0);
    }).pipe(Effect.provide(host)),
  );
});

test("onChange watches the input's feed even when checkpoints have a different owner", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const source = yield* Source;
      const params = { name: "watch" };
      yield* source.create(params);
      yield* source.append(params, [1], { close: true });
      const projection = Projection.make({
        id: "routed-watch",
        input: Source.at(params),
        process: () => Effect.void,
      });
      const fiber = yield* Projection.onChange(projection);
      expect((yield* Fiber.join(fiber)).status).toBe("source-closed");
    }).pipe(Effect.provide(host), Effect.scoped),
  );
});

test("onChange rejects a family rebound without its matching feed", async () => {
  const alternate = provides(Memory.layerMemory(), Source).pipe(
    Layer.flatMap((context) => Layer.succeed(Source, Context.get(context, Source))),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const projection = Projection.make({
        id: "rebound-feed",
        input: Source.at({ name: "watch" }),
        process: () => Effect.void,
      });
      const fiber = yield* Projection.onChange(projection).pipe(Effect.provide(alternate));
      const result = yield* Fiber.join(fiber).pipe(Effect.result);
      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") expect(result.failure.reason).toBe("unsupported-composition");
    }).pipe(Effect.provide(host), Effect.scoped),
  );
});
