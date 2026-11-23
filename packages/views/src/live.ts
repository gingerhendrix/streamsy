import type { StreamAddress, Streamsy } from "@streamsy/core";
import { enlist, ownerOf } from "@streamsy/core/internal";
import type { StreamRoute } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  OwnerTransaction,
  Projection,
  ProjectionFault,
  ProjectionRetry,
  type ProjectionKey,
  type Slices,
  type Unit,
} from "@streamsy/projection";
import {
  Array,
  Context,
  Effect,
  Exit,
  Order,
  Predicate,
  Record,
  Schedule,
  Scope,
  Stream,
} from "effect";
import type { Backend } from "./backend.ts";
import {
  namespaceOf,
  type CircuitError,
  type InputOf,
  type ParameterExpressions,
  type ValidInputs,
} from "./circuit.ts";
import { circuitErrors, normalizeUnit } from "./circuit-engine.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- Each scoped reader owns these private output relations.
import { makeLiveOutputs, type LiveSnapshot } from "./live-output.ts";
import { compilePlan } from "./relation.ts";
import { parameterReference } from "./expression.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- makeEngine compiles an inert plan.
import { makeEngine, type OutputChange } from "./sql/engine.ts";
import { memoryBackendOf, ViewBackend } from "./view-backend.ts";
import { ViewFault } from "./view-fault.ts";
import type { Input, Relation } from "./view.ts";

type LiveView = Pick<Relation<unknown>, "expression" | "inputs" | "rowType"> & {
  readonly routing?: never;
};

type Views = Readonly<Record<string, LiveView>>;

type InputServices<V extends Views> = ReturnType<InputOf<V[keyof V]>["at"]>["stream"]["Identifier"];

type LiveInput<V extends Views> = Omit<Input, "at"> & {
  readonly at: (
    params: Readonly<Record<string, unknown>>,
  ) => Streamsy.Address<StreamAddress.CollectionsEvent<Input["collections"]>, InputServices<V>>;
};

type Name<V> = Extract<keyof V, string>;

type Row<V extends Views, K extends Name<V>> = V[K] extends { readonly rowType?: infer A }
  ? A
  : never;

interface LiveReader<V extends Views> {
  readonly rows: <K extends Name<V>>(name: K) => Effect.Effect<LiveSnapshot<Row<V, K>>>;
  readonly changes: <K extends Name<V>>(
    name: K,
  ) => Stream.Stream<LiveSnapshot<Row<V, K>>, CircuitError>;
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

class LiveUnit extends Context.Service<LiveUnit, (changes: readonly OutputChange[]) => void>()(
  "@streamsy/views/LiveUnit",
) {}

type LiveAddresses<V extends Views> = Record<string, ReturnType<LiveInput<V>["at"]>>;

interface PreparedBinding<V extends Views> {
  readonly binding: LiveBinding<V>;
  readonly namespace: string;
}

interface LiveBinding<V extends Views> extends ProjectionKey {
  readonly inputs: LiveAddresses<V>;
  readonly state: undefined;
  readonly commit: Commit.Commit<void, LiveUnit, CheckpointStore | OwnerTransaction>;
  readonly process: (
    batch: Slices<LiveAddresses<V>>,
    unit: Unit,
  ) => Effect.Effect<void, CircuitError, LiveUnit>;
}

/** Drop heap writes and staged output even if an enclosing owner catches this unit's failure. */
function liveCommit(
  backend: Backend,
  tx: typeof OwnerTransaction.Service,
  stage: (changes: readonly OutputChange[]) => () => void,
) {
  return Commit.make<void, LiveUnit, CheckpointStore | OwnerTransaction>({
    ...Commit.ownerTransaction,
    apply: (pending) =>
      tx
        .withTransaction(
          backend.atomic(
            Effect.gen(function* () {
              let publish: (() => void) | undefined;

              const discard = () => {
                publish = undefined;
              };

              yield* enlist(tx.owner, {
                onCommit: () => publish?.(),
                onRollback: discard,
              });

              // This declaration has no prepare, accepted stream or outputs.
              const run = pending.run.pipe(
                Effect.provideService(LiveUnit, (changes) => {
                  publish = stage(changes);
                }),
              );

              yield* Commit.ownerTransaction
                .apply({ ...pending, run })
                .pipe(
                  Effect.onExit((exit) =>
                    Exit.isFailure(exit) ? Effect.sync(discard) : Effect.void,
                  ),
                );
            }),
          ),
        )
        .pipe(
          Effect.catchTag("SqlError", (cause) =>
            Effect.fail(
              new ProjectionFault({
                phase: "process",
                reason: "storage-failure",
                message: "Cannot update live circuit state",
                cause,
              }),
            ),
          ),
        ),
  });
}

export function prepareLive<C extends StreamRoute.ParamCodecs, const V extends Views>(
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

  const bind = (
    values: StreamRoute.Params<C>,
    backend: Backend,
    tx: typeof OwnerTransaction.Service,
    stage: (changes: readonly OutputChange[]) => () => void,
  ): PreparedBinding<V> => {
    type ProjectionInputs = Parameters<
      typeof Projection.process<C, Record<string, LiveInput<V>>>
    >[0]["inputs"];

    // SAFETY: the declaration checks input parameter types and the runtime walk checks their names.
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Restore the checked input map at the Projection boundary.
    const checkedInputs = inputs as ProjectionInputs;

    const binding = Projection.process<
      C,
      Record<string, LiveInput<V>>,
      CircuitError,
      LiveUnit,
      LiveUnit,
      CheckpointStore | OwnerTransaction
    >({
      id,
      params,
      inputs: checkedInputs,
      commit: liveCommit(backend, tx, stage),
      process: (batch, unit) =>
        Effect.gen(function* () {
          const publish = yield* LiveUnit;
          const changes = yield* normalizeUnit(engine, backend, namespace, sources, batch);

          publish(yield* engine.process(backend, namespace, changes, unit.params));
        }).pipe(circuitErrors),
    }).at(values);

    const namespace = namespaceOf(binding);

    return { binding, namespace };
  };

  return { bind, names, id, retry };
}

const retryable = (fault: ProjectionFault) => fault.retryable;

/** Run views in a scoped follower and read committed heap snapshots. */
export function live<C extends StreamRoute.ParamCodecs, const V extends Views>(
  definition: LiveDefinition<C, V>,
) {
  const plan = prepareLive(definition);

  const at = (
    params: StreamRoute.Params<C>,
  ): Effect.Effect<
    LiveReader<V>,
    CircuitError,
    Scope.Scope | CheckpointStore | OwnerTransaction | ViewBackend | InputServices<V>
  > =>
    Effect.suspend(() => {
      let lifetime: Scope.Closeable | undefined;

      return Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const memory = memoryBackendOf(yield* ViewBackend);

          if (memory === undefined)
            return yield* new ViewFault({ message: "View.live requires a memory ViewBackend" });

          const tx = yield* OwnerTransaction;
          const store = yield* CheckpointStore;

          if (memory.owner !== tx.owner || ownerOf(store.load) !== tx.owner)
            return yield* new ViewFault({
              message:
                "View.live requires the memory backend and checkpoint store to share the transaction owner",
            });

          const outputs = yield* makeLiveOutputs(plan.names);
          const { binding, namespace } = plan.bind(params, memory.backend, tx, outputs.stage);

          const duplicate = () =>
            new ViewFault({
              message: `View.live ${plan.id} is already active for these parameters`,
            });

          if (memory.live.has(namespace)) return yield* duplicate();

          const checkpoint = yield* restore(store.load(binding));

          if (memory.live.has(namespace)) return yield* duplicate();

          if (checkpoint.token !== "0")
            return yield* new ViewFault({
              message: `View.live ${plan.id} requires an unused checkpoint for these parameters`,
            });

          const scope = yield* Scope.fork(yield* Effect.scope);
          lifetime = scope;
          memory.live.add(namespace);

          yield* Scope.addFinalizer(
            scope,
            tx
              .withTransaction(
                store.remove(binding).pipe(
                  Effect.andThen(memory.backend.clear(namespace)),
                  Effect.andThen(
                    enlist(tx.owner, {
                      onCommit: () => {
                        memory.live.delete(namespace);
                        outputs.close();
                      },
                      onRollback: () => {},
                    }),
                  ),
                ),
              )
              .pipe(Effect.orDie),
          );

          const follow = Effect.gen(function* () {
            const schedule = plan.retry ?? (yield* ProjectionRetry);

            const retryCheck = <A, R>(check: Effect.Effect<A, ProjectionFault, R>) =>
              check.pipe(
                Effect.retry({ while: retryable, schedule }),
                // A finite policy can run out while the follower is still live.
                // Start another check at the normal catch-up polling cadence.
                Effect.retry({ while: retryable, schedule: Schedule.spaced("250 millis") }),
              );

            const catchUp = retryCheck(Projection.tails(binding)).pipe(
              Effect.flatMap((tails) => retryCheck(Projection.caughtUp(binding, tails))),
              Effect.matchEffect({
                onSuccess: () => outputs.finishCatchUp(Exit.void),
                onFailure: (fault) => outputs.finishCatchUp(Exit.fail(fault)),
              }),
            );

            yield* Effect.all(
              [Projection.follow(binding, { retry: schedule }).pipe(Stream.runDrain), catchUp],
              { concurrency: "unbounded", discard: true },
            );
          }).pipe(Effect.exit, Effect.flatMap(outputs.finishFollow));

          yield* follow.pipe(Effect.forkIn(scope));

          return {
            rows: <K extends Name<V>>(name: K) => outputs.rows<Row<V, K>>(name),
            changes: <K extends Name<V>>(name: K) => outputs.changes<Row<V, K>>(name),
            caughtUp: outputs.caughtUp,
          } satisfies LiveReader<V>;
        }),
      ).pipe(
        Effect.onInterrupt(() =>
          lifetime === undefined ? Effect.void : Scope.close(lifetime, Exit.void),
        ),
      );
    });

  return { at };
}
