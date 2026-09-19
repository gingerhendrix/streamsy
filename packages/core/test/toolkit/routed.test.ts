import { expect, it } from "bun:test";
import { Cause, Effect, Exit, Fiber, Layer, Option, Result, Schema, Stream } from "effect";
import { NotSupported } from "../../src/protocol/errors.ts";
import { StreamsWriter, type StreamsReader } from "../../src/protocol/tags.ts";
import { ZERO_OFFSET } from "../../src/offset/index.ts";
import * as Backend from "../../src/toolkit/backend.ts";
import * as StreamRoute from "../../src/toolkit/route.ts";
import * as Streams from "../../src/toolkit/streams.ts";

const Note = Schema.Struct({ n: Schema.Finite });
const Draft = StreamRoute.json("scratch/drafts/:user", {
  params: { user: Schema.String },
  schema: Note,
});
const Journal = StreamRoute.json("journal/:user", {
  params: { user: Schema.String },
  schema: Note,
});
const Totals = StreamRoute.json("totals/:user", {
  params: { user: Schema.String },
  schema: Note,
});

const memory = Backend.make("memory");
const sqlite = Backend.make("sqlite");

const memoryGraph = Streams.layerMemory({ longPollTimeoutMs: 100 });
const routed: RoutedLayer = Streams.layerRouted([
  memory.serves(Draft),
  sqlite.serves(Journal, Totals),
]).pipe(Layer.provide(memory.layer(memoryGraph)), Layer.provide(sqlite.layer(memoryGraph)));

type RoutedServices = StreamsReader | StreamsWriter;

type RoutedLayer = Layer.Layer<RoutedServices>;

/**
 * Build the routed Layer, then provide only `TestClock` from the outside. The
 * backend keys stay inside this scope, so a test never needs to name them.
 */
const run = <A, E>(
  program: Effect.Effect<A, E, RoutedServices>,
  layer: RoutedLayer = routed,
): Promise<Exit.Exit<A, E>> =>
  Effect.runPromiseExit(
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(layer);
        return yield* Effect.provideContext(program, context);
      }),
    ),
  );

/** Run a program whose own exit is the value under test. */
const runExit = async <A, E>(
  program: Effect.Effect<A, E, RoutedServices>,
  layer: RoutedLayer = routed,
): Promise<Exit.Exit<A, E>> => {
  const outer = await run(Effect.exit(program), layer);
  if (Exit.isFailure(outer)) throw new Error("Expected the routed Layer to build");
  return outer.value;
};

const dieDefect = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isFailure(exit) ? Result.getOrUndefined(Cause.findDefect(exit.cause)) : undefined;

it("each family lands in its own backend", async () => {
  const exit = await run(
    Effect.gen(function* () {
      yield* Streams.create(Draft.ref({ user: "ann" }));
      yield* Streams.append(Draft.ref({ user: "ann" }), [{ n: 1 }]);
      yield* Streams.create(Journal.ref({ user: "ann" }));
      yield* Streams.append(Journal.ref({ user: "ann" }), [{ n: 2 }]);
      yield* Streams.create(Totals.ref({ user: "ann" }));
      yield* Streams.append(Totals.ref({ user: "ann" }), [{ n: 3 }]);

      expect(
        yield* Streams.read(Draft.ref({ user: "ann" })).pipe(Streams.items, Stream.runCollect),
      ).toEqual([{ n: 1 }]);
      expect(
        yield* Streams.read(Journal.ref({ user: "ann" })).pipe(Streams.items, Stream.runCollect),
      ).toEqual([{ n: 2 }]);
      expect(
        yield* Streams.read(Totals.ref({ user: "ann" })).pipe(Streams.items, Stream.runCollect),
      ).toEqual([{ n: 3 }]);
      expect(yield* Streams.head(Draft.ref({ user: "ann" }))).toMatchObject({
        contentType: "application/json",
      });

      yield* Streams.remove(Draft.ref({ user: "ann" }));
      expect(
        Exit.isFailure(yield* Streams.head(Draft.ref({ user: "ann" })).pipe(Effect.exit)),
      ).toBe(true);
    }),
  );
  expect(exit).toEqual(Exit.succeed(undefined));
});

it("an id that no route or fallback serves dies with RangeError", async () => {
  const unrouted = StreamRoute.json("unrouted/:user", {
    params: { user: Schema.String },
    schema: Note,
  });
  const exit = await runExit(Streams.head(unrouted.ref({ user: "ann" })));
  expect(Exit.isFailure(exit)).toBe(true);
  expect(dieDefect(exit)).toBeInstanceOf(RangeError);
});

it("follow waits through readNext on the routed reader", async () => {
  const exit = await run(
    Effect.gen(function* () {
      yield* Streams.create(Draft.ref({ user: "bo" }));
      yield* Streams.append(Draft.ref({ user: "bo" }), [{ n: 1 }]);
      const fiber = yield* Streams.follow(Draft.ref({ user: "bo" })).pipe(
        Streams.items,
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Streams.append(Draft.ref({ user: "bo" }), [{ n: 2 }]);
      expect(yield* Fiber.join(fiber)).toEqual([{ n: 1 }, { n: 2 }]);
    }),
  );
  expect(exit).toEqual(Exit.succeed(undefined));
});

it("a same-backend fork succeeds and a cross-backend fork is NotSupported", async () => {
  const exit = await run(
    Effect.gen(function* () {
      const writer = yield* StreamsWriter;
      yield* Streams.create(Journal.ref({ user: "s" }));
      yield* Streams.append(Journal.ref({ user: "s" }), [{ n: 1 }]);
      const same = yield* writer.fork(Journal.ref({ user: "t" }).id, Journal.ref({ user: "s" }).id);
      expect(same._tag).toBe("Created");
      expect(
        yield* Streams.read(Journal.ref({ user: "t" })).pipe(Streams.items, Stream.runCollect),
      ).toEqual([{ n: 1 }]);

      const cross = yield* writer
        .fork(Draft.ref({ user: "t" }).id, Journal.ref({ user: "s" }).id)
        .pipe(Effect.exit);
      expect(Exit.isFailure(cross)).toBe(true);
      if (Exit.isFailure(cross)) {
        const failure = Cause.squash(cross.cause);
        expect(failure).toBeInstanceOf(NotSupported);
        expect(failure).toMatchObject({ feature: "cross-backend-fork" });
      }
    }),
  );
  expect(exit).toEqual(Exit.succeed(undefined));
});

it("a fallback serves an id that no route matches", async () => {
  const other = Backend.make("other");
  const withFallback: RoutedLayer = Streams.layerRouted([memory.serves(Draft)], {
    fallback: other,
  }).pipe(Layer.provide(memory.layer(memoryGraph)), Layer.provide(other.layer(memoryGraph)));
  const exit = await run(
    Effect.gen(function* () {
      yield* Streams.create(Journal.ref({ user: "any" }));
      expect(
        yield* Streams.read(Journal.ref({ user: "any" })).pipe(Streams.items, Stream.runCollect),
      ).toEqual([]);
    }),
    withFallback,
  );
  expect(exit).toEqual(Exit.succeed(undefined));
});

it("two overlapping templates make the Layer build die", async () => {
  const byId = StreamRoute.json("orders/:id", { params: { id: Schema.String }, schema: Note });
  const literal = StreamRoute.json("orders/latest", { params: {}, schema: Note });
  const overlap = Streams.layerRouted([memory.serves(byId, literal)]).pipe(
    Layer.provide(memory.layer(memoryGraph)),
  );
  const exit = await Effect.runPromiseExit(Effect.scoped(Layer.build(overlap)));
  expect(Exit.isFailure(exit)).toBe(true);
  expect(dieDefect(exit)).toBeInstanceOf(RangeError);
});

it("custom routes are ordered and the first match wins", async () => {
  const fold = StreamRoute.json("fold/:x", { params: { x: Schema.String }, schema: Note });
  const custom = (label: string) =>
    StreamRoute.custom({
      parse: (id) => (id.startsWith("fold/") ? Option.some({ label }) : Option.none()),
      ref: () => fold.ref({ x: label }),
    });
  const other = Backend.make("other");
  const layer: RoutedLayer = Streams.layerRouted([
    memory.serves(custom("one")),
    other.serves(custom("two")),
  ]).pipe(Layer.provide(memory.layer(memoryGraph)), Layer.provide(other.layer(memoryGraph)));
  const exit = await run(
    Effect.gen(function* () {
      yield* Streams.create(fold.ref({ x: "one" }));
      expect(yield* Streams.head(fold.ref({ x: "one" }))).toMatchObject({
        nextOffset: ZERO_OFFSET,
      });
      // The second custom route never sees an id, so its backend stays empty.
      expect(Exit.isFailure(yield* Streams.head(fold.ref({ x: "two" })).pipe(Effect.exit))).toBe(
        true,
      );
    }),
    layer,
  );
  expect(exit).toEqual(Exit.succeed(undefined));
});

it("a backend re-tag shares one graph with a sibling Layer in either merge order", async () => {
  // A nested `Layer.build` shares a storage Layer with a sibling only by build
  // order. Re-tagging shares regardless, which is what lets a derive `Commit`
  // owner share one `SqlClient` with the SQLite protocol graph.
  for (const tableFirst of [true, false]) {
    let builds = 0;
    const counted = Layer.effectDiscard(
      Effect.sync(() => {
        builds += 1;
      }),
    );
    const graph = Streams.layerMemory().pipe(Layer.provideMerge(counted));
    const first = Backend.make("first");
    const second = Backend.make("second");
    const sibling = Layer.effectDiscard(Effect.void).pipe(Layer.provide(counted));
    const table = Streams.layerRouted([first.serves(Draft), second.serves(Journal)]).pipe(
      Layer.provide(first.layer(graph)),
      Layer.provide(second.layer(graph)),
    );
    const merged = tableFirst ? Layer.merge(table, sibling) : Layer.merge(sibling, table);
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(merged);
          yield* Streams.create(Draft.ref({ user: "x" })).pipe(Effect.provide(context));
        }),
      ),
    );
    expect(exit).toEqual(Exit.succeed(undefined));
    // The routed Layer never calls `Layer.build`, so the one graph builds once.
    expect(builds).toBe(1);
  }
});
