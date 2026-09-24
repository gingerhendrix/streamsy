import { expect, it } from "bun:test";
import { Cause, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { AnyStream, Backend, provides, StreamFamily, StreamId, Streams } from "../../src/index.ts";

class Notes extends StreamFamily.json<Notes>()("notes/:user", {
  params: { user: Schema.String },
  schema: Schema.Struct({ text: Schema.String }),
}) {}
class Copies extends StreamFamily.json<Copies>()("notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
class Overlap extends StreamFamily.bytes<Overlap>()("notes/:id", {
  params: { id: Schema.String },
}) {}
class Counts extends StreamFamily.state<Counts>()("counts/:id", {
  params: { id: Schema.NumberFromString },
  collections: {
    count: { schema: Schema.Struct({ id: Schema.String, count: Schema.Number }), key: "id" },
  },
}) {}

it("family statics retain their route and typed service over a protocol graph", async () => {
  expect(Notes.template).toBe("notes/:user");
  expect(Notes.route.ref).toBe(Notes.ref);
  expect(Option.getOrThrow(Counts.parse("counts/4"))).toEqual({ id: 4 });
  await Effect.runPromise(
    Effect.gen(function* () {
      const notes = yield* Notes;
      yield* notes.create({ user: "ann" });
      yield* notes.append({ user: "ann" }, [{ text: "hello" }]);
      yield* notes.fork({ user: "copy" }, { user: "ann" });
      const batches = yield* Stream.runCollect(notes.read({ user: "copy" }));
      expect(batches.flatMap((batch) => batch.items)).toEqual([{ text: "hello" }]);
      expect((yield* notes.head({ user: "ann" })).contentType).toBe("application/json");
      const counts = yield* Counts;
      yield* counts.create({ id: 4 });
      yield* counts.append({ id: 4 }, [
        { type: "count", key: "a", value: { id: "a", count: 3 }, headers: { operation: "upsert" } },
      ]);
      expect((yield* Stream.runCollect(counts.read({ id: 4 })))[0]?.items[0]).toMatchObject({
        value: { count: 3 },
      });
    }).pipe(Effect.provide(provides(Streams.layerMemory(), Notes, Counts))),
  );
});

it("every wire operation rejects foreign ids including both fork forms", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const { wire } = yield* Notes;
      const own = StreamId.make("notes/ann");
      const foreign = StreamId.make("private/secret");
      const attempts: ReadonlyArray<
        Effect.Effect<
          unknown,
          import("../../src/index.ts").StreamsFault | import("../../src/index.ts").ProtocolError
        >
      > = [
        wire.head(foreign),
        wire.read(foreign),
        wire.readNext(foreign, { offset: "-1" }),
        wire.create(foreign),
        wire.append(foreign, { data: new Uint8Array(), contentType: "application/octet-stream" }),
        wire.remove(foreign),
        wire.fork(own, foreign),
        wire.fork(foreign, own),
        wire.create(own, { forkedFrom: foreign }),
      ];
      for (const attempt of attempts) {
        const exit = yield* Effect.exit(attempt);
        expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
      }
    }).pipe(Effect.provide(provides(Streams.layerMemory(), Notes))),
  );
});

it("bindings refuse duplicate and overlapping templates and enforce AnyStream last", () => {
  const graph = Streams.layerMemory();
  expect(() => provides(graph, Notes, Copies)).toThrow("duplicate family template");
  expect(() => provides(graph, Notes, Overlap)).toThrow("overlap");
  expect(() => provides(graph, AnyStream, Notes)).toThrow("AnyStream must be last");
  expect(() => provides(graph, Notes, AnyStream)).not.toThrow();
});

it("named backends isolate families and AnyStream handles arbitrary ids", async () => {
  const durable = Backend.make("durable");
  const layer = provides(durable, AnyStream).pipe(
    Layer.provide(durable.layer(Streams.layerMemory())),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const any = yield* AnyStream;
      yield* any.create({ id: "a/b/c" });
      yield* any.append({ id: "a/b/c" }, [new TextEncoder().encode("hello")]);
      expect((yield* Stream.runCollect(any.read({ id: "a/b/c" })))[0]?.items[0]).toEqual(
        new TextEncoder().encode("hello"),
      );
    }).pipe(Effect.provide(layer)),
  );
});

// These are compiled, never executed: params and items cannot widen at the family boundary.
const typeChecks = (notes: Notes["Service"]) => {
  // @ts-expect-error family params are decoded and named
  const invalidParams = notes.append({ other: "ann" }, [{ text: "hello" }]);
  // @ts-expect-error item codec fixes the application type
  const invalidItem = notes.append({ user: "ann" }, [42]);
  void invalidParams;
  void invalidItem;
  const template: "notes/:user" = Notes.template;
  return template;
};
void typeChecks;

const missingFamily = Effect.gen(function* () {
  yield* Notes;
  yield* Counts;
}).pipe(Effect.provide(provides(Streams.layerMemory(), Notes)));
// oxlint-disable effecttsgo/missing-effect-context -- This negative type test deliberately omits Counts.
// @ts-expect-error providing Notes must leave Counts in the requirement
const missing: Effect.Effect<void> = missingFamily;
void missing;
// oxlint-enable effecttsgo/missing-effect-context
