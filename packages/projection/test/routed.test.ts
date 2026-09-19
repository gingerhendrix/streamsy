import { expect, test } from "bun:test";
import { Effect, Layer, Schema, Stream } from "effect";
import { Backend, Protocol, StreamRoute, Streams } from "@streamsy/core";
import * as BunStorage from "@streamsy/storage/bun";
import { Projection, ProjectionFault } from "@streamsy/projection";
import * as Sqlite from "@streamsy/projection/sqlite";

const Source = StreamRoute.json("memory/source/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
});
const Output = StreamRoute.json("sqlite/output/:name", {
  params: { name: Schema.String },
  schema: Schema.Finite,
});

const memory = Backend.make("projection-memory");
const sqlite = Backend.make("projection-sqlite");

const routed = Streams.layerRouted([memory.serves(Source), sqlite.serves(Output)]).pipe(
  Layer.provide(memory.layer(Streams.layerMemory())),
  Layer.provide(sqlite.layer(Protocol.layer())),
);

/** The checkpoint owner and the output share the SQLite boundary; the input lives elsewhere. */
const host = Layer.merge(routed, Sqlite.layer).pipe(
  Layer.provideMerge(BunStorage.layer({ client: { filename: ":memory:" } })),
);

test("a routed projection reads its input from memory and commits output with the SQLite checkpoint", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const sourceRef = Source.ref({ name: "orders" });
      const outputRef = Output.ref({ name: "orders" });
      yield* Streams.create(sourceRef);
      yield* Streams.create(outputRef);
      yield* Streams.append(sourceRef, [1, 2, 3]);

      const projection = Projection.make({
        id: "routed-double",
        input: sourceRef,
        process: (batch) =>
          Streams.append(
            outputRef,
            batch.input.items.map((item) => item * 2),
          ),
      });

      const first = yield* Projection.run(projection);
      expect(first.status).toBe("caught-up");
      expect(first.items).toBe(3);
      expect(first.record.inputs).toEqual({ input: (yield* Streams.head(sourceRef)).nextOffset });
      expect(yield* Streams.read(outputRef).pipe(Streams.items, Stream.runCollect)).toEqual([
        2, 4, 6,
      ]);

      const second = yield* Projection.run(projection);
      expect(second.status).toBe("caught-up");
      expect(second.items).toBe(0);
      expect(second.units).toBe(0);

      yield* Streams.remove(sourceRef);
      const third = yield* Projection.run(projection).pipe(Effect.result);
      expect(third._tag).toBe("Failure");
      if (third._tag === "Failure" && third.failure instanceof ProjectionFault) {
        expect(third.failure.phase).toBe("read");
        expect(third.failure.reason).toBe("history-unavailable");
        expect(third.failure.input).toBe("input");
      } else throw new Error("Expected a ProjectionFault");
    }).pipe(Effect.provide(host), Effect.scoped),
  );
});
