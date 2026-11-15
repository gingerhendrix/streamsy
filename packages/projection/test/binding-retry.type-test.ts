/* oxlint-disable effecttsgo/missing-layer-context -- Negative type tests prove that hook services cannot be omitted. */
import { Array, Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { Commit, Projection, ProjectionWakeup, type Unit } from "@streamsy/projection";

class Source extends Streamsy.json<Source>()("BindingRetryTypes/Source", "retry-types/:id", {
  params: { id: Schema.FiniteFromString },
  schema: Schema.String,
}) {}

class FailureSink extends Context.Service<
  FailureSink,
  { readonly record: (unit: Unit<{ readonly id: number }>) => Effect.Effect<void, "sink-failure"> }
>()("BindingRetryTypes/Sink") {}

const common = {
  id: "typed-hook",
  params: { id: Schema.FiniteFromString },
  inputs: { source: Source },
  retry: {
    onExhausted: (unit: Unit<{ readonly id: number }>) =>
      Effect.flatMap(FailureSink, (sink) => sink.record(unit)),
  },
};

const process = Projection.process({
  ...common,
  commit: Commit.transaction,
  process: (_, unit, context) => {
    const attempt: number = context.attempt;
    const id: number = unit.params.id;
    void attempt;
    void id;

    return Effect.void;
  },
});

const fold = Projection.fold({
  ...common,
  state: { schema: Schema.Finite, initial: 0 },
  fold: (state) => state,
});

const emit = Projection.emit({ ...common, commit: Commit.producer, outputs: {}, emit: () => ({}) });

const mapAccum = Projection.mapAccum({
  ...common,
  commit: Commit.producer,
  outputs: {},
  state: { schema: Schema.Finite, initial: 0 },
  mapAccum: (state) => [state, {}] as const,
});

const mapAccumBatch = Projection.mapAccumBatch({
  ...common,
  commit: Commit.producer,
  outputs: {},
  state: { schema: Schema.Finite, initial: 0 },
  mapAccumBatch: (state) => [state, {}] as const,
});

export const hooks = Array.map(
  [process, fold, emit, mapAccum, mapAccumBatch],
  (declaration) => declaration.retry,
);

// Hook services reach both hosts and are independent of handler/Commit requirements.
export const attached = Projection.attach(process);

export const alarm = ProjectionWakeup.attach(process);

// @ts-expect-error FailureSink remains required by the attach layer.
const noHookService: Layer.Layer<
  never,
  never,
  Exclude<Layer.Services<typeof attached>, FailureSink>
> = attached;

void noHookService;

// Even a service supplied to the handler by Commit.transaction stays required by the hook.
const sqlHook = Projection.process({
  ...common,
  retry: { onExhausted: () => Effect.asVoid(SqlClient.SqlClient) },
  commit: Commit.transaction,
  process: () => Effect.asVoid(SqlClient.SqlClient),
});

export const sqlAttached = Projection.attach(sqlHook);

// @ts-expect-error The hook runs outside the transaction and needs a caller-provided SqlClient.
const noSql: Layer.Layer<
  never,
  never,
  Exclude<Layer.Services<typeof sqlAttached>, SqlClient.SqlClient>
> = sqlAttached;

void noSql;

Projection.attach(process, {
  // @ts-expect-error The attach retry name was removed.
  retry: undefined,
});
