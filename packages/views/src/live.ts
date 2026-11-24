import { MemoryStorage, Offset, Streamsy, type StreamAddress } from "@streamsy/core";
import { enlist } from "@streamsy/core/internal";
import type { StreamRoute } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionRetry,
  Transaction,
} from "@streamsy/projection";
import {
  Array,
  Cause,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Order,
  Predicate,
  Record,
  type Schedule,
  type Scope,
  Stream,
} from "effect";
import {
  namespaceOf,
  type CircuitError,
  type InputOf,
  type ParameterExpressions,
  type ValidInputs,
} from "./circuit.ts";
import { circuitErrors, normalizeUnit } from "./circuit-engine.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- Each scoped reader owns these private output relations.
import { makeLiveOutputs } from "./live-output.ts";
import { compilePlan } from "./relation.ts";
import { parameterReference } from "./expression.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- makeEngine compiles an inert plan.
import { makeEngine } from "./sql/engine.ts";
import { currentBackend, ViewBackend } from "./view-backend.ts";
import type { Input, Relation } from "./view.ts";

type LiveView = Pick<Relation<unknown>, "expression" | "inputs" | "rowType"> & {
  readonly routing?: never;
};

export type Views = Readonly<Record<string, LiveView>>;

type InputServices<V extends Views> = ReturnType<InputOf<V[keyof V]>["at"]>["stream"]["Identifier"];

type LiveInput<V extends Views> = Omit<Input, "at"> & {
  readonly at: (
    params: Readonly<Record<string, unknown>>,
  ) => Streamsy.Address<StreamAddress.CollectionsEvent<Input["collections"]>, InputServices<V>>;
};

/** The rows of every view and the input offsets committed with them. */
export interface LiveSnapshot<V extends Views> {
  readonly rows: {
    readonly [K in keyof V]: ReadonlyArray<V[K] extends { readonly rowType?: infer A } ? A : never>;
  };
  /** Committed offsets indexed by the declared State input's key. */
  readonly offsets: Readonly<Record<string, Offset>>;
  /** Whether the reader has reached the input tails captured at startup. */
  readonly caughtUp: boolean;
}

/** A scoped reader's current snapshot and its stream of committed snapshots. */
export interface LiveReader<V extends Views> {
  readonly snapshot: Effect.Effect<LiveSnapshot<V>>;
  readonly snapshots: Stream.Stream<LiveSnapshot<V>, CircuitError>;
  readonly caughtUp: Effect.Effect<void, CircuitError>;
}

type LiveDefinition<C extends StreamRoute.ParamCodecs, V extends Views> = {
  readonly id: string;
  readonly params: C;
  readonly views:
    | (V & ValidInputs<NoInfer<C>, V>)
    | ((params: ParameterExpressions<NoInfer<C>>) => V & ValidInputs<NoInfer<C>, V>);
  readonly maxAffectedKeys?: number;
  readonly retry?: Schedule.Schedule<unknown>;
};

const memoryHost = Layer.mergeAll(
  ViewBackend.layerMemory,
  CheckpointStore.layer,
  Transaction.layer,
).pipe(Layer.provide(MemoryStorage.layer(Streamsy.group())));

/** Run views in a scoped follower. Open the reader outside any transaction. */
export function live<C extends StreamRoute.ParamCodecs, const V extends Views>(
  definition: LiveDefinition<C, V>,
) {
  const { views, params, id, maxAffectedKeys, retry } = definition;
  const refs = Record.map(params, (_codec, name) => parameterReference(name));
  const parameterized = Predicate.isFunction(views);
  // SAFETY: Each selector's phantom type follows the decoded codec for its name.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Record.map keeps the names.
  const relations = parameterized ? views(refs as ParameterExpressions<C>) : views;
  const parameters = parameterized ? Array.sort(Record.keys(params), Order.String) : [];
  const sources = new Map<string, { readonly state: Input; readonly collection: string }>();
  const names = Record.keys(relations);

  const plans = Record.fromEntries(
    Array.map(names, (name) => {
      const relation = relations[name]!;

      if (relation.routing !== undefined)
        throw new RangeError("View.routed requires a routed output");

      for (const [sourceId, input] of relation.inputs) sources.set(sourceId, input);

      return [name, compilePlan(name, relation.expression, parameters)] as const;
    }),
  );

  const inputs: Readonly<Record<string, Input | LiveInput<V>>> = Record.fromEntries(
    Array.map([...sources.values()], ({ state }) => [state.key, state] as const),
  );

  for (const state of Record.values(inputs))
    for (const name of Record.keys(state.paramCodecs))
      if (!Record.has(params, name)) throw new RangeError(`Missing circuit parameter ${name}`);

  const engine = Effect.runSync(makeEngine(plans, false, maxAffectedKeys));

  type ProjectionInputs = Parameters<
    typeof Projection.process<C, Record<string, LiveInput<V>>>
  >[0]["inputs"];

  // SAFETY: the declaration checks input parameter types and the runtime walk checks their names.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Restore the checked input map at the Projection boundary.
  const checkedInputs = inputs as ProjectionInputs;

  const at = (
    values: StreamRoute.Params<C>,
  ): Effect.Effect<LiveReader<V>, CircuitError, Scope.Scope | InputServices<V>> =>
    Effect.gen(function* () {
      const host = yield* Layer.build(Layer.fresh(memoryHost));
      const backend = yield* currentBackend.pipe(Effect.provideContext(host));
      const { owner } = Context.get(host, Transaction);
      const outputs = yield* makeLiveOutputs<V>(names, Record.keys(inputs));

      const binding = Projection.process<
        C,
        Record<string, LiveInput<V>>,
        CircuitError,
        never,
        never,
        CheckpointStore | Transaction
      >({
        id,
        params,
        inputs: checkedInputs,
        commit: Commit.transaction,
        process: (batch, unit) =>
          Effect.gen(function* () {
            const { changes } = yield* normalizeUnit(
              engine,
              backend,
              Effect.succeed(namespace),
              sources,
              batch,
            );

            const updated = yield* engine.process(backend, namespace, changes, unit.params);
            const offsets = Record.map(batch, (slice) => Offset.make(slice.nextOffset));

            yield* enlist(owner, {
              onCommit: outputs.stage(updated, offsets),
              onRollback: () => {},
            });
          }).pipe(circuitErrors),
      }).at(values);

      const namespace = namespaceOf(binding);

      const follow = Effect.gen(function* () {
        const schedule = retry ?? (yield* ProjectionRetry);

        const captureTails = Projection.tails(binding).pipe(
          Effect.retry({ while: (fault) => fault.retryable, schedule }),
          Effect.flatMap(outputs.captureTails),
        );

        yield* Effect.all(
          [Projection.follow(binding, { retry: schedule }).pipe(Stream.runDrain), captureTails],
          { concurrency: "unbounded", discard: true },
        );
      }).pipe(Effect.provideContext(host));

      yield* follow.pipe(
        Effect.onExit((exit) =>
          outputs.end(
            Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause) ? Exit.void : exit,
          ),
        ),
        Effect.exit,
        Effect.forkScoped({ startImmediately: true }),
      );

      const { snapshot, snapshots } = outputs;

      return {
        snapshot,
        snapshots,
        caughtUp: snapshots.pipe(
          Stream.filter((value) => value.caughtUp),
          Stream.runHead,
          Effect.flatMap((value) => (Option.isSome(value) ? Effect.void : Effect.interrupt)),
        ),
      };
    });

  return { at };
}
