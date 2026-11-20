import { Array, Effect, Option, Order, Predicate, Record, Schema } from "effect";
import { DurableState, isStreamsFault } from "@streamsy/core";
import { Streams, StreamsWriter, type StreamRoute } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  NonRetryable,
  Projection,
  ProjectionFault,
  Transaction,
  type ProjectionKey,
  type Declaration,
  type Slices,
  type Unit,
  type WakeAt,
} from "@streamsy/projection";
import { SqlClient } from "effect/sql";
import { compilePlan } from "./relation.ts";
import { keyExpression, parameterReference, type TypedExpression } from "./expression.ts";
import { circuitPlanHash, orderPlanFields } from "./plan.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- makeEngine compiles an inert plan; it neither constructs nor acquires an Effect service.
import { makeEngine, type StateChange } from "./sql/engine.ts";
import {
  circuitState,
  clearNamespace,
  migrate as migrateEngine,
  migrateCircuits,
  storeFor,
} from "./sql/store.ts";
import type { Input, Relation } from "./view.ts";
import { dataFault, ViewDataFault } from "./sql/data-fault.ts";
import { normalize } from "./sql/normalize.ts";
import { encodeRowKey } from "./runtime/key.ts";
import type { JsonValue, RelationPlan } from "./ir/contracts.ts";

type Views = Readonly<Record<string, Pick<Relation<unknown>, "expression" | "inputs" | "rowType">>>;

type Output = Projection.PerGeneration<Input, string>;

type OutputMap = Readonly<Record<string, Output>>;

type ViewMap = Readonly<Record<string, Views>>;

type InputOf<R> = R extends {
  readonly inputs: ReadonlyMap<string, { readonly state: infer I extends Input }>;
}
  ? I
  : never;

type ViewInputs<V> = V extends Views ? InputOf<V[keyof V]> : never;

type InputsOf<V extends ViewMap> = Readonly<Record<string, ViewInputs<V[keyof V]>>>;

type OutputState<O extends Output> =
  O extends Projection.PerGeneration<infer S, string> ? S : never;

type CollectionsOf<O extends Output> = OutputState<O>["collections"];

type Fits<V extends Views, O extends Output> = {
  readonly [K in keyof V]: K extends keyof CollectionsOf<O>
    ? V[K] extends { readonly rowType?: infer Row }
      ? [Row] extends [CollectionsOf<O>[K]["schema"]["Type"]]
        ? V[K]
        : never
      : never
    : never;
};

type InvalidStateParams<
  C extends StreamRoute.ParamCodecs,
  I extends Input,
  Exempt extends string = never,
> = {
  [K in Exclude<keyof I["paramCodecs"], Exempt>]: K extends keyof C
    ? [StreamRoute.Params<I["paramCodecs"]>[K]] extends [StreamRoute.Params<C>[K]]
      ? [StreamRoute.Params<C>[K]] extends [StreamRoute.Params<I["paramCodecs"]>[K]]
        ? never
        : K
      : K
    : K;
}[Exclude<keyof I["paramCodecs"], Exempt>];

type InvalidParams<C extends StreamRoute.ParamCodecs, I extends Input | Output> =
  I extends Projection.PerGeneration<infer S extends Input, infer N>
    ? Extract<N, keyof C> | InvalidStateParams<C, S, N>
    : I extends Input
      ? InvalidStateParams<C, I>
      : never;

type ValidInputs<C extends StreamRoute.ParamCodecs, V extends Views> = {
  readonly [K in keyof V]: V[K]["inputs"] extends ReadonlyMap<
    string,
    { readonly state: infer I extends Input }
  >
    ? InvalidParams<C, I> extends never
      ? V[K]
      : never
    : never;
};

export type CircuitError = PlanChanged | ViewFault | ViewDataFault | ProjectionFault;

export type ParameterExpressions<C extends StreamRoute.ParamCodecs> = {
  readonly [
    K in keyof C as C[K]["Type"] extends JsonValue | undefined ? K : never
  ]: TypedExpression<C[K]["Type"]>;
};

type WithParameters<C extends StreamRoute.ParamCodecs, V> =
  | V
  | ((params: ParameterExpressions<NoInfer<C>>) => V);

type Common<C extends StreamRoute.ParamCodecs, V extends ViewMap, O extends OutputMap, HE, HR> = {
  readonly id: string;
  readonly version?: number;
  readonly generation?: number;
  readonly params: C;
  readonly retry?: CircuitDeclaration<C, V, O, HE, HR>["retry"];
  /** Maximum distinct affected relation keys per unit, including intermediate changes. */
  readonly maxAffectedKeys?: number;
  /** Return a wake request after the unit, or void to clear the deadline. */
  readonly wakeAt?: (unit: Unit<StreamRoute.Params<NoInfer<C>>>) => void | WakeAt;
};

type SingleDefinition<
  C extends StreamRoute.ParamCodecs,
  V extends Views,
  O extends Output,
  HE,
  HR,
> = Common<C, { output: V }, { output: O }, HE, HR> & {
  readonly output: O & (InvalidParams<NoInfer<C>, O> extends never ? unknown : never);
  readonly outputs?: never;
  readonly views: WithParameters<C, V & Fits<V, NoInfer<O>> & ValidInputs<NoInfer<C>, V>>;
};

type MultiDefinition<
  C extends StreamRoute.ParamCodecs,
  V extends ViewMap,
  O extends OutputMap,
  HE,
  HR,
> = Common<C, V, O, HE, HR> & {
  readonly output?: never;
  readonly outputs: O & {
    readonly [K in keyof O]: InvalidParams<NoInfer<C>, O[K]> extends never ? O[K] : never;
  };
  readonly views: WithParameters<
    C,
    V & {
      readonly [K in keyof O]: K extends keyof V
        ? V[K] & Fits<V[K], NoInfer<O[K]>> & ValidInputs<NoInfer<C>, V[K]>
        : never;
    } & { readonly [K in Exclude<keyof V, keyof O>]: never }
  >;
};

export class PlanChanged extends Schema.TaggedError<
  PlanChanged,
  { readonly brand: unique symbol }
>()("ViewPlanChanged", {
  namespace: Schema.String,
  stored: Schema.String,
  requested: Schema.String,
}) {
  readonly [NonRetryable] = true;

  override get message() {
    return `Circuit plan changed for ${this.namespace} (stored ${this.stored}, requested ${this.requested})`;
  }
}

export class ViewFault extends Schema.TaggedError<ViewFault, { readonly brand: unique symbol }>()(
  "ViewFault",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Same projection binding and generation, independent of the unit's ranges. */
function namespaceOf(key: ProjectionKey) {
  const params = Record.fromEntries(
    Array.map(Array.sort(Record.keys(key.params), Order.String), (name) => [
      name,
      key.params[name],
    ]),
  );

  const projectionKey = JSON.stringify([key.id, key.version, params]);

  return JSON.stringify([projectionKey, key.generation]);
}

/** Create operator tables at host startup, like StateTable.migrate(sql). */
export const migrate = Effect.fn("View.migrate")(function* (sql: SqlClient.SqlClient) {
  yield* sql.onDialectOrElse({
    sqlite: () => Effect.void,
    orElse: () => Effect.fail(new ViewFault({ message: "View requires SQLite" })),
  });
  yield* migrateEngine().pipe(Effect.provideService(SqlClient.SqlClient, sql));
  yield* migrateCircuits(sql);
});

const guard = Effect.fn("View.guard")(function* (
  sql: SqlClient.SqlClient,
  key: ProjectionKey,
  hash: string,
) {
  const namespace = namespaceOf(key);
  const state = circuitState(sql, namespace);
  const [previous] = yield* state.load();

  if (previous === undefined) yield* state.save(hash);
  else if (previous.plan_hash !== hash)
    return yield* new PlanChanged({ namespace, stored: previous.plan_hash, requested: hash });

  return namespace;
});

const commit = Commit.transactionWithCleanup((key, sql) =>
  clearNamespace(sql, namespaceOf(key)).pipe(
    Effect.mapError(
      (cause) =>
        new ProjectionFault({
          phase: "checkpoint",
          reason: "storage-failure",
          message: "Cannot clear circuit state",
          cause,
        }),
    ),
  ),
);

export interface CircuitBinding<V extends ViewMap, O extends OutputMap> extends ProjectionKey {
  readonly inputs: { readonly [name: string]: ReturnType<InputsOf<V>[string]["at"]> };
  readonly state: undefined;
  readonly streams: ReturnType<
    ReturnType<typeof Projection.process<StreamRoute.ParamCodecs, InputsOf<V>, O>>["at"]
  >["streams"];
  readonly commit: Commit.Commit<void | WakeAt, SqlClient.SqlClient, CheckpointStore | Transaction>;
  readonly process: (
    batch: Slices<CircuitBinding<V, O>["inputs"]>,
    unit: Unit,
    encoded?: string,
  ) => Effect.Effect<
    void | WakeAt,
    CircuitError,
    SqlClient.SqlClient | OutputState<O[keyof O]>["Identifier"]
  >;
}

export interface CircuitDeclaration<
  C extends StreamRoute.ParamCodecs,
  V extends ViewMap,
  O extends OutputMap,
  HE = never,
  HR = never,
> extends Declaration<C, CircuitBinding<V, O>, HE, HR> {}

/** A group of views is one transactional Projection.process declaration. */
export function circuit<
  C extends StreamRoute.ParamCodecs,
  const V extends Views,
  O extends Output,
  HE = never,
  HR = never,
>(
  definition: SingleDefinition<C, V, O, HE, HR>,
): CircuitDeclaration<C, { output: V }, { output: O }, HE, HR>;

export function circuit<
  C extends StreamRoute.ParamCodecs,
  const V extends ViewMap,
  const O extends OutputMap,
  HE = never,
  HR = never,
>(definition: MultiDefinition<C, V, O, HE, HR>): CircuitDeclaration<C, V, O, HE, HR>;

export function circuit<C extends StreamRoute.ParamCodecs, HE, HR>(
  definition: Common<C, ViewMap, OutputMap, HE, HR> &
    (
      | {
          readonly output: Output;
          readonly views: WithParameters<C, Views>;
          readonly outputs?: never;
        }
      | {
          readonly output?: never;
          readonly views: WithParameters<C, ViewMap>;
          readonly outputs: OutputMap;
        }
    ),
): CircuitDeclaration<C, ViewMap, OutputMap, HE, HR> {
  const parameterized = Predicate.isFunction(definition.views);

  const refs = Record.map(definition.params, (_codec, name) => parameterReference(name));
  // SAFETY: each selector has the decoded type of the matching parameter codec.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Record.map preserves the named keys; their expression types are phantom.
  const params = refs as ParameterExpressions<C>;

  const parameters = parameterized ? Array.sort(Record.keys(definition.params), Order.String) : [];

  const { output: singleOutput } = definition;
  const routedOutputs = singleOutput === undefined ? definition.outputs : { output: singleOutput };

  const resolveViews = <V extends Views | ViewMap>(value: WithParameters<C, V>): V =>
    Predicate.isFunction(value) ? value(params) : value;

  const views =
    singleOutput === undefined
      ? resolveViews(definition.views)
      : { output: resolveViews(definition.views) };

  const plans: Record<string, RelationPlan> = {};
  const targets = new Map<string, { readonly route: string; readonly collection: string }>();
  const sources = new Map<string, { readonly state: Input; readonly collection: string }>();

  const owned = new Set<string>();

  const checkParams = (state: Input, generationParam?: string) => {
    for (const name of Record.keys(state.paramCodecs))
      if (name !== generationParam && !Record.has(definition.params, name))
        throw new RangeError(`Missing circuit parameter ${name}`);
  };

  for (const route of Array.sort(Record.keys(routedOutputs), Order.String)) {
    const output = routedOutputs[route];

    if (output === undefined || !Projection.isPerGeneration(output))
      throw new ViewFault({ message: `Circuit output ${route} requires Projection.perGeneration` });

    const { route: state, param } = Projection.generationBinding(output);
    checkParams(state, param);

    if (owned.has(state.key)) throw new RangeError(`Duplicate output State class ${state.key}`);
    owned.add(state.key);
    const relations = views[route];

    if (relations === undefined) throw new RangeError(`Missing views for output ${route}`);

    for (const collection of Record.keys(state.collections))
      if (!Record.has(relations, collection))
        throw new RangeError(`Missing view for output collection ${state.key}/${collection}`);

    for (const name of Array.sort(Record.keys(relations), Order.String)) {
      const relation = relations[name];

      if (relation === undefined) throw new RangeError(`Missing view ${name}`);
      const collection = state.collections[name];

      if (!Record.has(state.collections, name) || collection === undefined)
        throw new RangeError(`Unknown output collection ${state.key}/${name}`);

      const expression = {
        kind: "key",
        input: relation.expression,
        key: keyExpression(collection.key),
      } as const;

      const planName = JSON.stringify([route, name]);
      plans[planName] = orderPlanFields(compilePlan(planName, expression, parameters));
      targets.set(planName, { route, collection: name });

      for (const [id, input] of relation.inputs) sources.set(id, input);
    }
  }

  if (owned.size === 0) throw new RangeError("A circuit needs an output State class");

  for (const route of Record.keys(views))
    if (!Record.has(routedOutputs, route)) throw new RangeError(`Unknown circuit output ${route}`);

  const inputs: Record<string, Input> = {};

  for (const { state } of sources.values()) inputs[state.key] = state;

  for (const state of Record.values(inputs)) checkParams(state);

  const engine = Effect.runSync(makeEngine(plans, true, definition.maxAffectedKeys));
  const hash = circuitPlanHash(plans);

  type ProjectionRoutes = Parameters<
    typeof Projection.process<C, Record<string, Input>, OutputMap>
  >[0];

  // SAFETY: route parameters were checked by the public overloads and the runtime walk.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Restore the checked input map at the Projection boundary.
  const checkedInputs = inputs as ProjectionRoutes["inputs"];
  // SAFETY: route parameters were checked by the public overloads and the runtime walk.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Restore the checked output map at the Projection boundary.
  const checkedOutputs = routedOutputs as ProjectionRoutes["outputs"];

  return Projection.process<
    C,
    Record<string, Input>,
    OutputMap,
    CircuitError,
    SqlClient.SqlClient | Input["Identifier"],
    CheckpointStore | Transaction,
    void,
    never,
    never,
    undefined,
    HE,
    HR,
    void | WakeAt
  >({
    ...definition,
    inputs: checkedInputs,
    outputs: checkedOutputs,
    commit,
    process: (batch, unit, { outputs }) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        // Unit params are decoded. Re-encode through the declaration to obtain the
        // exact checkpoint identity, including transformed parameter codecs.
        const boundParams: Record<string, string> = {};

        for (const [name, codec] of Record.toEntries(definition.params))
          boundParams[name] = yield* Schema.encodeEffect(codec)(unit.params[name]);

        const namespace = yield* guard(
          sql,
          {
            id: unit.projectionId,
            version: unit.version,
            generation: unit.generation,
            params: boundParams,
          },
          hash,
        );

        const changes: StateChange[] = [];
        const store = storeFor(sql, namespace);

        const active = Array.flatMap([...sources], ([sourceId, input]) => {
          const slice = batch[input.state.key];

          return slice === undefined
            ? []
            : [{ sourceId, input, slice, markerKey: encodeRowKey(sourceId) }];
        });

        const snapshots = yield* store.getMany(
          "@snapshots",
          Array.map(active, ({ markerKey }) => markerKey),
        );

        for (const { sourceId, input, slice, markerKey } of active) {
          const source = Array.findFirst(
            engine.program.nodes,
            (node) => node.kind === "source" && node.sourceId === sourceId,
          );

          if (Option.isNone(source)) throw new TypeError(`Missing compiled source ${sourceId}`);
          const location = engine.program.locations.get(source.value.id);

          if (location === undefined) throw new Error(`Missing source location ${sourceId}`);
          changes.push(
            ...(yield* normalize(
              store,
              sourceId,
              source.value.id,
              input.collection,
              dataFault(location),
              slice.items,
              snapshots.has(markerKey),
            )),
          );
        }

        const changesOut = yield* engine.process(namespace, changes, unit.params);
        const entriesByRoute = new Map<string, DurableState.Entry<Input["collections"]>[]>();

        for (const change of changesOut) {
          const target = targets.get(change.view);

          if (target === undefined) throw new Error(`Missing output for view ${change.view}`);
          const { collection, route } = target;
          const options = { headers: { txid: unit.key } };

          const entry =
            change.kind === "upsert"
              ? DurableState.upsert(collection, change.value, options)
              : DurableState.remove(collection, String(change.key), options);

          const entries = entriesByRoute.get(route) ?? [];
          entries.push(entry);
          entriesByRoute.set(route, entries);
        }

        for (const [route, output] of Record.toEntries(outputs)) {
          const entries = entriesByRoute.get(route) ?? [];

          if (!Array.isReadonlyArrayNonEmpty(entries)) continue;

          const service: Effect.Effect<
            Effect.Success<typeof output.stream>,
            never,
            Input["Identifier"]
          > = output.stream;

          const { wire } = yield* service;
          yield* Streams.append(output, DurableState.changes(output, {}, entries)).pipe(
            Effect.provideService(StreamsWriter, wire),
            Effect.mapError(
              (cause) =>
                new ProjectionFault({
                  phase: "process",
                  reason: isStreamsFault(cause)
                    ? "storage-failure"
                    : Predicate.isTagged(cause, "StreamBusy")
                      ? "output-busy"
                      : "invalid-output",
                  message: `Cannot append circuit output ${output.id}`,
                  cause,
                }),
            ),
          );
        }

        return definition.wakeAt?.(unit);
      }).pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            new ProjectionFault({
              phase: "process",
              reason: "storage-failure",
              message: "Cannot update circuit state",
              cause,
            }),
          SchemaError: (cause) => new ViewFault({ message: cause.message }),
          SqlEngineFault: (cause) => new ViewFault({ message: "Circuit operator failed", cause }),
        }),
      ),
  });
}
