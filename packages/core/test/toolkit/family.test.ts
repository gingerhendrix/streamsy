import { Protocol } from "@streamsy/core/protocol";
import { expect, it } from "bun:test";
import { Cause, Context, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { Streamsy, StreamId, StreamAddress } from "../../src/index.ts";
import { Streams } from "../../src/protocol/index.ts";

class Notes extends Streamsy.json<Notes>()("Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.Struct({ text: Schema.String }),
}) {}
class Copies extends Streamsy.json<Copies>()("Copies", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}
class Overlap extends Streamsy.bytes<Overlap>()("Overlap", "notes/:id", {
  params: { id: Schema.String },
}) {}
class Counts extends Streamsy.state<Counts>()("Counts", "counts/:id", {
  params: { id: Schema.NumberFromString },
  collections: {
    count: { schema: Schema.Struct({ id: Schema.String, count: Schema.Number }), key: "id" },
  },
}) {}

it("stream statics retain their route and typed service over a protocol graph", async () => {
  expect(Notes.template).toBe("notes/:user");
  expect(Notes.at({ user: "ann" }).stream.key).toBe(Notes.key);
  expect(Notes).not.toHaveProperty("route");
  expect(Notes).not.toHaveProperty("family");
  expect(Notes).not.toHaveProperty("make");
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
    }).pipe(Effect.provide(Protocol.bind(Streams.layerMemory(), Streamsy.group(Notes, Counts)))),
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
    }).pipe(Effect.provide(Protocol.bind(Streams.layerMemory(), Streamsy.group(Notes)))),
  );
});

it("bindings refuse duplicate and overlapping templates and enforce Streamsy.Wildcard last", () => {
  const graph = Streams.layerMemory();
  expect(() => Protocol.bind(graph, Streamsy.group(Notes, Copies))).toThrow(
    "duplicate stream template",
  );
  expect(() => Protocol.bind(graph, Streamsy.group(Notes, Overlap))).toThrow("overlap");
  expect(() => Protocol.bind(graph, Streamsy.group(Streamsy.Wildcard, Notes))).toThrow(
    "Wildcard must be last",
  );
  expect(() => Protocol.bind(graph, Streamsy.group(Notes, Streamsy.Wildcard))).not.toThrow();
});

it("Streamsy.Wildcard handles arbitrary ids", async () => {
  const layer = Protocol.bind(Streams.layerMemory(), Streamsy.group(Streamsy.Wildcard));
  await Effect.runPromise(
    Effect.gen(function* () {
      const any = yield* Streamsy.Wildcard;
      yield* any.create({ id: "a/b/c" });
      yield* any.append({ id: "a/b/c" }, [new TextEncoder().encode("hello")]);
      expect((yield* Stream.runCollect(any.read({ id: "a/b/c" })))[0]?.items[0]).toEqual(
        new TextEncoder().encode("hello"),
      );
    }).pipe(Effect.provide(layer)),
  );
});

// These are compiled, never executed: params and items cannot widen at the stream boundary.
const typeChecks = (notes: Notes["Service"]) => {
  // @ts-expect-error stream params are decoded and named
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
}).pipe(Effect.provide(Protocol.bind(Streams.layerMemory(), Streamsy.group(Notes))));
// oxlint-disable effecttsgo/missing-effect-context -- This negative type test deliberately omits Counts.
// @ts-expect-error providing Notes must leave Counts in the requirement
const missing: Effect.Effect<void> = missingFamily;
void missing;
// oxlint-enable effecttsgo/missing-effect-context

class Drafts extends Streamsy.json<Drafts>()("Drafts", "drafts/:user", {
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
      expect(notes.route.template).toBe(Notes.template);
      expect(copies.route.template).toBe(Copies.template);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Protocol.bind(graph, Streamsy.group(Notes)),
          Protocol.bind(graph, Streamsy.group(Copies)),
        ),
      ),
    ),
  );
});

class SameName extends Streamsy.bytes<SameName>()("Notes", "other/:user", {
  params: { user: Schema.String },
}) {}
class CustomA extends Streamsy.custom<CustomA>()("CustomA", {
  parse: (id: string) => (id.startsWith("custom-a/") ? Option.some({ id }) : Option.none()),
  address: ({ id }: { readonly id: string }) => StreamAddress.bytes(id),
}) {}
class CustomB extends Streamsy.custom<CustomB>()("CustomB", {
  parse: (id: string) => (id.startsWith("custom-b/") ? Option.some({ id }) : Option.none()),
  address: ({ id }: { readonly id: string }) => StreamAddress.bytes(id),
}) {}

it("stable names define service identity and duplicate names are refused", () => {
  expect(Notes.key).toBe("@streamsy/core/Stream/Notes");
  expect(SameName.key).toBe(Notes.key);
  expect(() => Protocol.bind(Streams.layerMemory(), Streamsy.group(Notes, SameName))).toThrow(
    "duplicate stream name",
  );
  expect(() =>
    Protocol.bind(Streams.layerMemory(), Streamsy.group(CustomA, CustomB)),
  ).not.toThrow();
  expect(() =>
    Protocol.bind(Streams.layerMemory(), Streamsy.group(CustomA, Streamsy.Wildcard)),
  ).not.toThrow();
  expect(Notes.at({ user: "ann" }).stream.key).toBe(Notes.key);
  expect(Notes.at({ user: "ann" }).id).toBe(Notes.address({ user: "ann" }).id);
});

it("Streamsy.Wildcard ordering follows stable identity across class aliases", () => {
  class AnyAgain extends Streamsy.Wildcard {}
  expect(() => Protocol.bind(Streams.layerMemory(), Streamsy.group(AnyAgain, Notes))).toThrow(
    "Wildcard must be last",
  );
});

it("provides a scoped feed with the stream's owner", async () => {
  const { Streamsy } = await import("../../src/index.ts");
  await Effect.runPromise(
    Effect.gen(function* () {
      const notes = yield* Notes;
      const feed = yield* Streamsy.ChangeFeed(Notes);
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
    }).pipe(Effect.provide(Protocol.bind(Streams.layerMemory(), Streamsy.group(Notes)))),
  );
});

it("does not expose a feed when the graph hides Storage", async () => {
  const { Streamsy } = await import("../../src/index.ts");
  const { StreamsReader, StreamsWriter } = await import("../../src/protocol/index.ts");
  const protocol = Layer.effectContext(
    Effect.context<
      | import("../../src/protocol/index.ts").StreamsReader
      | import("../../src/protocol/index.ts").StreamsWriter
    >().pipe(Effect.map(Context.pick(StreamsReader, StreamsWriter))),
  ).pipe(Layer.provide(Streams.layerMemory()));
  await Effect.runPromise(
    Effect.gen(function* () {
      expect(Option.isNone(yield* Effect.serviceOption(Streamsy.ChangeFeed(Notes)))).toBe(true);
      yield* (yield* Notes).create({ user: "ann" });
    }).pipe(Effect.provide(Protocol.bind(protocol, Streamsy.group(Notes)))),
  );
});

it("rejects a graph whose exposed Storage does not own the protocol wires", async () => {
  const { Storage } = await import("../../src/index.ts");
  const first = Streams.layerMemory();
  const other = Streams.layerMemory();
  const mixed = Layer.merge(first, Layer.effect(Storage, Storage).pipe(Layer.provide(other)));
  const result = await Effect.runPromiseExit(
    Effect.void.pipe(Effect.provide(Protocol.bind(mixed, Streamsy.group(Notes)))),
  );
  expect(Exit.isFailure(result)).toBe(true);
  if (Exit.isFailure(result))
    expect(String(Cause.squash(result.cause))).toContain(
      "The exposed Storage does not own the protocol wires",
    );
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
    Effect.void.pipe(Effect.provide(Protocol.bind(graph, Streamsy.group(Notes)))),
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
