import { layer } from "@effect/vitest";
import { expect, expectTypeOf } from "vitest";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { OwnerRecords } from "@streamsy/core/internal";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { recordKey } from "@streamsy/projection/checkpoint";
import { withFamilies } from "./support/families.ts";
import { memoryHost } from "./support/memory-host.ts";
import { layerSqlite } from "./support/sqlite.ts";

const params = { label: Schema.String, bucket: Schema.FiniteFromString };

class Events extends Streamsy.json<Events>()("Bindings/Events", "bindings/:bucket", {
  params: { bucket: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

const declaration = (id: string, generation = 1, version = 1) =>
  Projection.fold({
    id,
    generation,
    version,
    params,
    inputs: { events: Events },
    state: { schema: Schema.Finite, initial: 0 },
    fold: (sum, { item }) => sum + item,
  });

const seed = (binding: Parameters<typeof Events.at>[0]) =>
  Effect.gen(function* () {
    const events = yield* Events;
    yield* events.create(binding);
    yield* events.append(binding, [1]);
  });

for (const [name, host] of [
  ["memory", withFamilies(memoryHost(), Events)],
  ["SQLite", withFamilies(layerSqlite(), Events)],
] as const) {
  layer(host)((it) => {
    it.effect(`${name}: lists typed bindings across generations and checkpoint versions`, () =>
      Effect.gen(function* () {
        const id = 'totals_%!"\\';
        const previous = declaration(id);
        const current = declaration(id, 3, 2);
        const shared = { label: 'é%_!"\\', bucket: 1 };
        const olderOnly = { label: "older", bucket: 2 };
        const currentOnly = { label: "current", bucket: 3 };
        const differentVersion = { label: "earlier-version-at-generation-three", bucket: 4 };
        const otherProjection = declaration('totalsX%!"\\', 3, 2);

        for (const binding of [shared, olderOnly, currentOnly, differentVersion])
          yield* seed(binding);
        yield* Projection.run(previous.at(shared));
        yield* Projection.run(previous.at(olderOnly));
        yield* Projection.run(current.at(shared));
        yield* Projection.run(current.at(currentOnly));
        yield* Projection.run(declaration(id, 3).at(differentVersion));
        yield* Projection.run(otherProjection.at(olderOnly));
        yield* Projection.run(declaration(`${id}-suffix`).at(olderOnly));
        yield* Projection.run(declaration(id.toUpperCase()).at(olderOnly));

        const records = yield* OwnerRecords;
        const slot = recordKey(current.at(shared));
        yield* records.write(
          slot.replace("streamsy.projection.v1", "streamsy.projection.trigger.v1"),
          "cursor",
        );
        yield* records.write(recordKey(otherProjection.at(currentOnly)), "unrelated owner record");
        yield* records.write(
          recordKey(declaration(id.toUpperCase()).at(currentOnly)),
          "unrelated owner record",
        );

        const all = yield* Projection.bindings(current);
        expectTypeOf(all).toEqualTypeOf<
          ReadonlyArray<{ readonly label: string; readonly bucket: number }>
        >();
        expect(all).toHaveLength(4);
        expect(all).toEqual(
          expect.arrayContaining([shared, olderOnly, currentOnly, differentVersion]),
        );
        const atThree = yield* Projection.bindings(current, { generation: 3 });
        expect(atThree).toHaveLength(3);
        expect(atThree).toEqual(expect.arrayContaining([shared, currentOnly, differentVersion]));
        const atOne = yield* Projection.bindings(current, { generation: 1 });
        expect(atOne).toHaveLength(2);
        expect(atOne).toEqual(expect.arrayContaining([shared, olderOnly]));
        expect(yield* Projection.bindings(current, { generation: 2 })).toEqual([]);
      }),
    );

    it.effect(`${name}: excludes retired, forgotten and ended rows within the selected owner`, () =>
      Effect.gen(function* () {
        const previous = declaration("lifecycle");
        const current = declaration("lifecycle", 2);
        const live = { label: "live", bucket: 11 };
        const retired = { label: "retired", bucket: 12 };
        const forgotten = { label: "forgotten", bucket: 13 };
        const ended = { label: "ended", bucket: 14 };
        const neverRun = { label: "never-ran", bucket: 15 };

        for (const binding of [live, retired, forgotten, ended, neverRun]) yield* seed(binding);
        expect(yield* Projection.bindings(current)).toEqual([]);

        for (const binding of [live, retired, forgotten, ended])
          yield* Projection.run(previous.at(binding));
        yield* Projection.run(current.at(live));
        yield* Projection.retire(current, live, { generation: 1 });
        yield* Projection.retire(current, retired, { generation: 1 });
        yield* Projection.forget(previous.at(forgotten));

        // Model a checkpoint after its final gone unit settled and before its forget ran.
        const store = yield* CheckpointStore;
        const bound = previous.at(ended);
        const loaded = yield* store.load(bound);
        const { identity, inputs, adapters } = Option.getOrThrow(loaded.record);
        yield* store.compareAndSet(bound, loaded.token, {
          identity,
          inputs,
          adapters,
          ended: true,
        });

        expect(yield* Projection.bindings(current)).toEqual([live]);
        expect(yield* Projection.bindings(current, { generation: 1 })).toEqual([]);
        expect(yield* Projection.bindings(current, { generation: 2 })).toEqual([live]);

        const foreign = Context.get(yield* Layer.build(Layer.fresh(host)), CheckpointStore);
        expect(
          yield* Projection.bindings(current).pipe(Effect.provideService(CheckpointStore, foreign)),
        ).toEqual([]);
      }),
    );

    it.effect(`${name}: fails explicitly when historical params do not round-trip`, () =>
      Effect.gen(function* () {
        const spec = declaration("changed-codec", 2, 2);
        const store = yield* CheckpointStore;
        const historical = { label: "old", bucket: "01" };
        yield* store.compareAndSet(
          { id: spec.id, version: 1, generation: 1, params: historical },
          "0",
          {
            identity: { inputs: {} },
            inputs: {},
            adapters: {},
          },
        );

        expect(yield* Projection.bindings(spec).pipe(Effect.flip)).toMatchObject({
          _tag: "InvalidBinding",
          projection: spec.id,
          params: historical,
        });
        expect(yield* Projection.bindings(spec, { generation: 2 })).toEqual([]);
      }),
    );
  });
}
