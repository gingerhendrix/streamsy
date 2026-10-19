import { Protocol } from "@streamsy/core/protocol";
import { expect, it as baseIt, layer } from "@effect/vitest";
import { Storage } from "../../src/index.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import { ForeignOwner, isForeignOwner } from "../../src/internal/owner.ts";
import { Array, Cause, Context, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import { Streamsy, StreamId, StreamAddress, MemoryStorage } from "../../src/index.ts";

class Notes extends Streamsy.json<Notes>()("Notes", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.Struct({ text: Schema.String }),
}) {}

class Copies extends Streamsy.json<Copies>()("Copies", "notes/:user", {
  params: { user: Schema.String },
  schema: Schema.String,
}) {}

class Counts extends Streamsy.state<Counts>()("Counts", "counts/:id", {
  params: { id: Schema.FiniteFromString },
  collections: {
    count: { schema: Schema.Struct({ id: Schema.String, count: Schema.Finite }), key: "id" },
  },
}) {}

layer(Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Notes, Counts)))((it) => {
  it.effect("stream statics retain their route and typed service over a protocol graph", () =>
    Effect.gen(function* () {
      expect(Notes.template).toBe("notes/:user");
      expect(Notes.at({ user: "ann" }).stream.key).toBe(Notes.key);
      expect(Notes).not.toHaveProperty("route");
      expect(Notes).not.toHaveProperty("family");
      expect(Notes).not.toHaveProperty("make");
      expect(Option.getOrThrow(Counts.parse("counts/4"))).toEqual({ id: 4 });

      const notes = yield* Notes;
      yield* notes.create({ user: "ann" });
      yield* notes.append({ user: "ann" }, [{ text: "hello" }]);
      yield* notes.fork({ user: "copy" }, { user: "ann" });
      const batches = yield* Stream.runCollect(notes.read({ user: "copy" }));
      expect(Array.flatMap(batches, (batch) => batch.items)).toEqual([{ text: "hello" }]);
      expect((yield* notes.head({ user: "ann" })).contentType).toBe("application/json");
      const counts = yield* Counts;
      yield* counts.create({ id: 4 });
      yield* counts.append({ id: 4 }, [
        { type: "count", key: "a", value: { id: "a", count: 3 }, headers: { operation: "upsert" } },
      ]);
      expect((yield* Stream.runCollect(counts.read({ id: 4 })))[0]?.items[0]).toMatchObject({
        value: { count: 3 },
      });
    }),
  );
});

layer(Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Notes)))((it) => {
  it.effect("every wire operation rejects foreign ids including both fork forms", () =>
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
    }),
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

const missingFamily = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* Notes;
    yield* Counts;
  }),
).pipe(Layer.provide(Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Notes))));

// oxlint-disable effecttsgo/missing-layer-context -- This negative type test deliberately omits Counts.
// @ts-expect-error providing Notes must leave Counts in the requirement
const missing: Layer.Layer<never> = missingFamily;

void missing;
// oxlint-enable effecttsgo/missing-layer-context

class Drafts extends Streamsy.json<Drafts>()("Drafts", "drafts/:user", {
  params: { user: Schema.String },
  schema: Schema.Struct({ text: Schema.String }),
}) {}

const distinctFamilies: Notes extends Drafts ? false : true = true;

void distinctFamilies;

const sharedGraph = MemoryStorage.layerProtocol();

layer(
  Layer.mergeAll(
    Protocol.bind(sharedGraph, Streamsy.group(Notes)),
    Protocol.bind(sharedGraph, Streamsy.group(Copies)),
  ),
)((it) => {
  it.effect("separate providers cannot alias classes declared with the same template", () =>
    Effect.gen(function* () {
      expect(Notes.key).not.toBe(Copies.key);

      const notes = yield* Notes;
      const copies = yield* Copies;
      expect(notes.route.template).toBe(Notes.template);
      expect(copies.route.template).toBe(Copies.template);
    }),
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

baseIt("stable names define service identity and duplicate names are refused", () => {
  expect(Notes.key).toBe("@streamsy/core/Stream/Notes");
  expect(SameName.key).toBe(Notes.key);
  expect(() =>
    Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(Notes, SameName)),
  ).toThrow("duplicate stream name");
  expect(() =>
    Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(CustomA, CustomB)),
  ).not.toThrow();
  expect(() =>
    Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(CustomA, Streamsy.Wildcard)),
  ).not.toThrow();
  expect(Notes.at({ user: "ann" }).stream.key).toBe(Notes.key);
  expect(Notes.at({ user: "ann" }).id).toBe(Notes.address({ user: "ann" }).id);
});

baseIt("Streamsy.Wildcard ordering follows stable identity across class aliases", () => {
  class AnyAgain extends Streamsy.Wildcard {}

  expect(() =>
    Protocol.bind(MemoryStorage.layerProtocol(), Streamsy.group(AnyAgain, Notes)),
  ).toThrow("Wildcard must be last");
});

const protocol = Layer.effectContext(
  Effect.context<
    | import("../../src/protocol/index.ts").StreamsReader
    | import("../../src/protocol/index.ts").StreamsWriter
  >().pipe(Effect.map(Context.pick(StreamsReader, StreamsWriter))),
).pipe(Layer.provide(MemoryStorage.layerProtocol()));

layer(Protocol.bind(protocol, Streamsy.group(Notes)))((it) => {
  it.effect("binds streams when the graph hides Storage", () =>
    Effect.gen(function* () {
      yield* (yield* Notes).create({ user: "ann" });
    }),
  );
});

baseIt.effect("rejects a graph whose exposed Storage does not own the protocol wires", () =>
  Effect.gen(function* () {
    const first = MemoryStorage.layerProtocol();
    const other = MemoryStorage.layerProtocol();
    const mixed = Layer.merge(first, Layer.effect(Storage, Storage).pipe(Layer.provide(other)));

    const result = yield* Layer.build(Protocol.bind(mixed, Streamsy.group(Notes))).pipe(
      Effect.exit,
    );

    expect(Exit.isFailure(result)).toBe(true);

    if (Exit.isFailure(result))
      expect(String(Cause.squash(result.cause))).toContain(
        "The exposed Storage does not own the protocol wires",
      );
  }),
);

baseIt.effect("spreading a protocol writer does not copy its ownership claim", () =>
  Effect.gen(function* () {
    const graph = MemoryStorage.layerProtocol().pipe(
      Layer.flatMap((context) =>
        Layer.succeedContext(
          Context.make(StreamsReader, Context.get(context, StreamsReader)).pipe(
            Context.add(StreamsWriter, { ...Context.get(context, StreamsWriter) }),
            Context.add(Storage, Context.get(context, Storage)),
          ),
        ),
      ),
    );

    const result = yield* Layer.build(Protocol.bind(graph, Streamsy.group(Notes))).pipe(
      Effect.exit,
    );

    expect(Exit.isFailure(result)).toBe(true);
  }),
);

baseIt("the private guard sentinel is recognized by its stable brand", () => {
  const foreignCopy = new ForeignOwner("notes/ann", true);
  Object.setPrototypeOf(foreignCopy, Error.prototype);
  expect(ForeignOwner.prototype.isPrototypeOf(foreignCopy)).toBe(false);
  expect(isForeignOwner(foreignCopy)).toBe(true);
  expect(isForeignOwner(new Error("unrelated"))).toBe(false);
});
