import { Effect, Schema } from "effect";
import { StorageFault, Streamsy } from "@streamsy/core";
import { Commit, InvalidBinding, NotAttached, Projection } from "@streamsy/projection";

class Source extends Streamsy.json<Source>()("FamilyTypes/Source", "family-types/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("FamilyTypes/Wake", "family-types-wake", { params: {} }) {}

const declaration = Projection.process({
  id: "family-types",
  params: { id: Schema.String },
  inputs: { source: Source },
  commit: Commit.atLeastOnce,
  process: () => Effect.void,
});

export const requests = Effect.gen(function* () {
  const registry = yield* Projection.Registry;
  const bindings = [declaration.at({ id: "a" }), declaration.at({ id: "b" })] as const;

  const scheduled: Effect.Effect<void, InvalidBinding | NotAttached | StorageFault> =
    registry.schedule(bindings);

  const rescan: Effect.Effect<void, NotAttached | StorageFault> = registry.rescan(
    declaration,
    Wake,
  );

  yield* scheduled;
  yield* registry.schedule(bindings[0]);
  yield* registry.schedule([]);
  yield* rescan;
  yield* registry.rescan(declaration.id, Wake.address({}).id);
  // @ts-expect-error Rescan targets a declaration or id, never a binding.
  yield* registry.rescan(bindings[0], Wake);
  // @ts-expect-error Encoded binding params are strings.
  yield* registry.schedule([{ id: declaration.id, params: { id: 1 } }]);
});
