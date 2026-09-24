import { expect, it } from "bun:test";
import { Cause, Context, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { AnyStream, provides, StreamFamily, StreamId, StreamRef } from "../../src/index.ts";
import { Backend, Streams } from "../../src/protocol/index.ts";

class Notes extends StreamFamily.json<Notes>()("Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.Struct({ text: Schema.String }),
}) {}
class Copies extends StreamFamily.json<Copies>()("Copies", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
class Overlap extends StreamFamily.bytes<Overlap>()("Overlap", "notes/:id", {
  params: { id: Schema.String },
}) {}
class Counts extends StreamFamily.state<Counts>()("Counts", "counts/:id", {
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

        wire.fork(foreign, own),
      ];
      for (const attempt of [wire.fork(own, foreign), wire.create(own, { forkedFrom: foreign })]) {
        const result = yield* Effect.result(attempt);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") expect(result.failure._tag).toBe("ForkSourceNotFound");
      }
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

class Drafts extends StreamFamily.json<Drafts>()("Drafts", "drafts/:user", {
  params: { user: Schema.String },
  schema: Schema.Struct({ text: Schema.String }),
}) {}
const distinctFamilies: Notes extends Drafts ? false : true = true;
void distinctFamilies;

it("separate providers cannot alias classes declared with the same template", async () => {
  expect(Notes.key).not.toBe(Copies.key);
  const graph = Streams.layerMemory();
  await Effect.runPromise(
    Effect.gen(function* () {
      const notes = yield* Notes;
      const copies = yield* Copies;
      expect(notes.route).toBe(Notes.route);
      expect(copies.route).toBe(Copies.route);
    }).pipe(Effect.provide(Layer.mergeAll(provides(graph, Notes), provides(graph, Copies)))),
  );
});

class SameName extends StreamFamily.bytes<SameName>()("Notes", "other/:user", {
  params: { user: Schema.String },
}) {}
class CustomA extends StreamFamily.custom<CustomA>()("CustomA", {
  parse: (id: string) => (id.startsWith("custom-a/") ? Option.some({ id }) : Option.none()),
  ref: ({ id }: { readonly id: string }) => StreamRef.bytes(id),
}) {}
class CustomB extends StreamFamily.custom<CustomB>()("CustomB", {
  parse: (id: string) => (id.startsWith("custom-b/") ? Option.some({ id }) : Option.none()),
  ref: ({ id }: { readonly id: string }) => StreamRef.bytes(id),
}) {}

it("stable names define service identity and duplicate names are refused", () => {
  expect(Notes.key).toBe("@streamsy/core/Family/Notes");
  expect(SameName.key).toBe(Notes.key);
  expect(() => provides(Streams.layerMemory(), Notes, SameName)).toThrow("duplicate family name");
  expect(() => provides(Streams.layerMemory(), CustomA, CustomB)).not.toThrow();
  expect(() => provides(Streams.layerMemory(), CustomA, AnyStream)).not.toThrow();
  expect(Notes.at({ user: "ann" }).family.key).toBe(Notes.key);
  expect(Notes.at({ user: "ann" }).id).toBe(Notes.ref({ user: "ann" }).id);
});

it("AnyStream ordering follows stable identity across class aliases", () => {
  class AnyAgain extends AnyStream {}
  expect(() => provides(Streams.layerMemory(), AnyAgain, Notes)).toThrow("AnyStream must be last");
});

it("provides a scoped feed with the family's owner", async () => {
  const { Changes } = await import("../../src/index.ts");
  await Effect.runPromise(
    Effect.gen(function* () {
      const notes = yield* Notes;
      const feed = yield* Changes(Notes);
      expect(feed.owner).toBe(notes.owner);
      yield* notes.create({ user: "ann" });
      const snapshots = yield* feed
        .changes(Notes.at({ user: "ann" }).id)
        .pipe(Stream.take(1), Stream.runCollect);
      expect(snapshots[0]?.present).toBe(true);
      const foreign = yield* feed
        .changes(StreamId.make("foreign/id"))
        .pipe(Stream.runDrain, Effect.exit);
      expect(Exit.isFailure(foreign)).toBe(true);
    }).pipe(Effect.provide(provides(Streams.layerMemory(), Notes))),
  );
});

it("does not expose a feed when the graph hides Storage", async () => {
  const { Changes } = await import("../../src/index.ts");
  const { StreamsReader, StreamsWriter } = await import("../../src/protocol/index.ts");
  const protocol = Layer.effectContext(
    Effect.context<
      | import("../../src/protocol/index.ts").StreamsReader
      | import("../../src/protocol/index.ts").StreamsWriter
    >().pipe(Effect.map(Context.pick(StreamsReader, StreamsWriter))),
  ).pipe(Layer.provide(Streams.layerMemory()));
  await Effect.runPromise(
    Effect.gen(function* () {
      expect(Option.isNone(yield* Effect.serviceOption(Changes(Notes)))).toBe(true);
      yield* (yield* Notes).create({ user: "ann" });
    }).pipe(Effect.provide(provides(protocol, Notes))),
  );
});

it("rejects a graph whose exposed Storage does not own the protocol wires", async () => {
  const { Storage } = await import("../../src/index.ts");
  const first = Streams.layerMemory();
  const other = Streams.layerMemory();
  const mixed = Layer.merge(first, Layer.effect(Storage, Storage).pipe(Layer.provide(other)));
  const result = await Effect.runPromiseExit(
    Effect.void.pipe(Effect.provide(provides(mixed, Notes))),
  );
  expect(Exit.isFailure(result)).toBe(true);
  if (Exit.isFailure(result))
    expect(String(Cause.squash(result.cause))).toContain("routed or mixed graph");
});

it("spreading a protocol writer does not copy its ownership claim", async () => {
  const { Storage } = await import("../../src/index.ts");
  const { StreamsReader, StreamsWriter } = await import("../../src/protocol/index.ts");
  const graph = Streams.layerMemory().pipe(
    Layer.flatMap((context) =>
      Layer.succeedContext(
        Context.make(StreamsReader, Context.get(context, StreamsReader)).pipe(
          Context.add(StreamsWriter, { ...Context.get(context, StreamsWriter) }),
          Context.add(Storage, Context.get(context, Storage)),
        ),
      ),
    ),
  );
  const result = await Effect.runPromiseExit(
    Effect.void.pipe(Effect.provide(provides(graph, Notes))),
  );
  expect(Exit.isFailure(result)).toBe(true);
});

it("the private guard sentinel is recognized by its stable brand", async () => {
  const { ForeignOwner, isForeignOwner } = await import("../../src/internal/owner.ts");
  const foreignCopy = new ForeignOwner("notes/ann", true);
  Object.setPrototypeOf(foreignCopy, Error.prototype);
  expect(foreignCopy instanceof ForeignOwner).toBe(false);
  expect(isForeignOwner(foreignCopy)).toBe(true);
  expect(isForeignOwner(new Error("unrelated"))).toBe(false);
});
