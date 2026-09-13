/* oxlint-disable eslint/no-underscore-dangle -- Effect results use public tagged variants. */
/* oxlint-disable effecttsgo/async-function, effecttsgo/strict-effect-provide -- Bun owns the runtime boundary; the test assembles the complete routed host graph. */
import { expect, test } from "bun:test";
import { Effect, Layer, Schema, Stream } from "effect";
import { Backend, Protocol, StreamRoute, Streams } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Projection, StreamSink, StreamSource } from "../src/index.ts";
import * as Sqlite from "../src/sqlite.ts";

const Source = StreamRoute.json("memory/source/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
});
const Sink = StreamRoute.json("sqlite/sink/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
});

const memory = Backend.make("projection-memory");
const sqlite = Backend.make("projection-sqlite");

const routed = Streams.layerRouted([memory.serves(Source), sqlite.serves(Sink)]).pipe(
  Layer.provide(memory.layer(Streams.layerMemory())),
  Layer.provide(sqlite.layer(Protocol.layer())),
);

const host = Layer.merge(routed, Sqlite.layer).pipe(
  Layer.provideMerge(BunStorage.layer({ client: { filename: ":memory:" } })),
);

test("a routed projection keeps its source in memory and its sink plus Commit in SQLite", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const sourceRef = Source.ref({ name: "orders" });
      const sinkRef = Sink.ref({ name: "orders" });
      yield* Streams.create(sourceRef);
      yield* Streams.create(sinkRef);
      yield* Streams.append(sourceRef, [1, 2, 3]);

      const projection = Projection.make({
        id: "routed-sum",
        version: 1,
        source: yield* StreamSource.make(sourceRef),
        sink: yield* StreamSink.make(sinkRef),
        initial: 0,
        stateSchema: Schema.fromJsonString(Schema.Finite),
        step: (state, item) => {
          const next = state + item;
          return { state: next, outputs: [next] };
        },
      });

      const first = yield* Projection.catchUp(projection);
      expect(first.status).toBe("caught-up");
      expect(first.items).toBe(3);
      expect(yield* Streams.read(sinkRef).pipe(Streams.items, Stream.runCollect)).toEqual([
        1, 3, 6,
      ]);

      const second = yield* Projection.catchUp(projection);
      expect(second.status).toBe("caught-up");
      expect(second.items).toBe(0);
      expect(second.boundaries).toBe(0);

      yield* Streams.remove(sourceRef);
      const third = yield* Projection.catchUp(projection).pipe(Effect.result);
      expect(third._tag).toBe("Failure");
      if (third._tag === "Failure") expect(third.failure.reason).toBe("history-unavailable");
    }).pipe(Effect.provide(host), Effect.scoped),
  );
});
