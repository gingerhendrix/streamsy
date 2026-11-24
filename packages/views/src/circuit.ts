import { Array, Effect, Order, Predicate, Record, Schema } from "effect";
import { DurableState, isStreamsFault } from "@streamsy/core";
import { Streams, StreamsWriter, type StreamRoute } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  NonRetryable,
  Transaction,
  Projection,
  ProjectionFault,
  type ProjectionKey,
  type Declaration,
  type Slices,
  type Unit,
  type WakeAt,
} from "@streamsy/projection";
import type { SqlClient } from "effect/sql";
import { compilePlan, type RelationExpression } from "./relation.ts";
import {
  keyExpression,
  parameterReference,
  type ExpressionValue,
  type TypedExpression,
} from "./expression.ts";
import { circuitPlanHash, orderPlanFields } from "./plan.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- makeEngine compiles an inert plan; it neither constructs nor acquires an Effect service.
import { makeEngine } from "./sql/engine.ts";
import type { Backend } from "./backend.ts";
import { migrateBackend } from "./sql/backend.ts";
import { currentBackend, memoryEnlistFault, type ViewBackend } from "./view-backend.ts";
import { ViewFault } from "./view-fault.ts";
import type { Input, Relation } from "./view.ts";
import { dataFault, ViewDataFault } from "./sql/data-fault.ts";
import { circuitErrors, normalizeUnit } from "./circuit-engine.ts";
import { encodeRowKey } from "./runtime/key.ts";
import {
  isJsonObject,
  type Expression,
  type JsonValue,
  type RelationPlan,
} from "./ir/contracts.ts";

export { ViewFault } from "./view-fault.ts";

interface CircuitView extends Pick<Relation<unknown>, "expression" | "inputs" | "rowType"> {
  readonly routing?: {
    readonly by: Readonly<Record<string, Expression>>;
    readonly row: Expression;
  };
}

type Views = Readonly<Record<string, CircuitView>>;

type Output = Projection.PerGeneration<Input, string, string>;

type OutputMap = Readonly<Record<string, Output>>;

type ViewMap = Readonly<Record<string, Views>>;

export type InputOf<R> = R extends {
  readonly inputs: ReadonlyMap<string, { readonly state: infer I extends Input }>;
}
  ? I
  : never;

type ViewInputs<V> = V extends Views ? InputOf<V[keyof V]> : never;

type InputsOf<V extends ViewMap> = Readonly<Record<string, ViewInputs<V[keyof V]>>>;

type OutputState<O extends Output> =
  O extends Projection.PerGeneration<infer S, string, string> ? S : never;

type CollectionsOf<O extends Output> = OutputState<O>["collections"];

type RouteFits<V extends CircuitView, O extends Output> =
  O extends Projection.PerGeneration<infer S extends Input, string, infer By>
    ? [By] extends [never]
      ? V extends { readonly routing: object }
        ? never
        : V
      : V extends { readonly routing: { readonly by: infer Fields } }
        ? [keyof Fields, By] extends [By, keyof Fields]
          ? {
              [K in By]: K extends keyof Fields & keyof S["paramCodecs"]
                ? ExpressionValue<Fields[K]> extends S["paramCodecs"][K]["Type"]
                  ? never
                  : K
                : K;
            }[By] extends never
            ? V
            : never
          : never
        : never
    : never;

type Fits<V extends Views, O extends Output> = {
  readonly [K in keyof V]: K extends keyof CollectionsOf<O>
    ? V[K] extends { readonly rowType?: infer Row }
      ? [Row] extends [CollectionsOf<O>[K]["schema"]["Type"]]
        ? RouteFits<V[K], O>
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
  I extends Projection.PerGeneration<infer S extends Input, infer N, infer By>
    ? Extract<N | By, keyof C> | InvalidStateParams<C, S, N | By>
    : I extends Input
      ? InvalidStateParams<C, I>
      : never;

export type ValidInputs<C extends StreamRoute.ParamCodecs, V extends Views> = {
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

const rowReference = (...path: string[]): Expression => ({ kind: "reference", scope: "row", path });

const outputExpression = (
  view: CircuitView,
  collectionKey: string,
  routeBy: readonly string[] | undefined,
): RelationExpression => {
  const { routing, expression } = view;

  if (routeBy === undefined) {
    if (routing !== undefined) throw new RangeError("View.routed requires a routed output");

    return { kind: "key", input: expression, key: keyExpression(collectionKey) };
  }

  if (
    routing === undefined ||
    Record.keys(routing.by).length !== routeBy.length ||
    !Array.every(routeBy, (name) => Record.has(routing.by, name))
  )
    throw new RangeError(
      `Each routed view must supply route expressions for ${Array.join(routeBy, ", ")}`,
    );

  const routedFields = Record.fromEntries(
    Array.map(Record.toEntries(routing.by), ([name, value]) => [`route:${name}`, value]),
  );

  // Keep the route expressions beside the projected row until the output key is assigned.
  return {
    kind: "key",
    input: { kind: "project", input: expression, fields: { ...routedFields, value: routing.row } },
    key: {
      kind: "variadic",
      operator: "key",
      operands: [
        ...Array.map(routeBy, (name) => rowReference(`route:${name}`)),
        rowReference("value", collectionKey),
      ],
    },
  };
};

/** Same projection binding and generation, independent of the unit's ranges. */
export function namespaceOf(key: ProjectionKey) {
  const params = Record.fromEntries(
    Array.map(Array.sort(Record.keys(key.params), Order.String), (name) => [
      name,
      key.params[name],
    ]),
  );

  // Keep the numeric 1 so existing operator namespaces still resolve.
  const projectionKey = JSON.stringify([key.id, 1, params]);

  return JSON.stringify([projectionKey, key.generation]);
}

/** Create operator tables at host startup, like StateTable.migrate(sql). */
export const migrate = Effect.fn("View.migrate")(function* (sql: SqlClient.SqlClient) {
  yield* migrateBackend(sql);
});

const guard = Effect.fn("View.guard")(function* (
  backend: Backend,
  key: ProjectionKey,
  hash: string,
) {
  const namespace = namespaceOf(key);
  const state = backend.circuit(namespace);
  const previous = yield* state.load;

  if (previous === undefined) yield* state.save(hash);
  else if (previous !== hash)
    return yield* new PlanChanged({ namespace, stored: previous, requested: hash });

  return namespace;
});

const commit = Commit.transactionWithCleanup((key) =>
  currentBackend.pipe(
    Effect.flatMap((backend) => backend.clear(namespaceOf(key))),
    Effect.mapError((error) => {
      const cause = Predicate.isTagged(error, "MemoryEnlistFault") ? memoryEnlistFault() : error;
      const wrongOwner = Predicate.isTagged(cause, "ViewFault");

      return new ProjectionFault({
        phase: "checkpoint",
        reason: wrongOwner ? "unsupported-composition" : "storage-failure",
        message: wrongOwner ? cause.message : "Cannot clear circuit state",
        cause,
      });
    }),
  ),
);

export interface CircuitBinding<V extends ViewMap, O extends OutputMap> extends ProjectionKey {
  readonly inputs: { readonly [name: string]: ReturnType<InputsOf<V>[string]["at"]> };
  readonly state: undefined;
  readonly streams: ReturnType<
    ReturnType<typeof Projection.process<StreamRoute.ParamCodecs, InputsOf<V>, O>>["at"]
  >["streams"];
  readonly commit: Commit.TransactionCommit<
    void | WakeAt,
    CheckpointStore | Transaction | ViewBackend
  >;
  readonly process: (
    batch: Slices<CircuitBinding<V, O>["inputs"]>,
    unit: Unit,
    encoded?: string,
  ) => Effect.Effect<
    void | WakeAt,
    CircuitError,
    ViewBackend | OutputState<O[keyof O]>["Identifier"]
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

  const targets = new Map<
    string,
    {
      readonly route: string;
      readonly collection: string;
      readonly routeBy: readonly string[] | undefined;
    }
  >();

  const sources = new Map<string, { readonly state: Input; readonly collection: string }>();

  const owned = new Set<string>();

  const checkParams = (state: Input, generationParam?: string, routeBy?: readonly string[]) => {
    for (const name of Record.keys(state.paramCodecs))
      if (
        name !== generationParam &&
        !routeBy?.includes(name) &&
        !Record.has(definition.params, name)
      )
        throw new RangeError(`Missing circuit parameter ${name}`);
  };

  for (const route of Array.sort(Record.keys(routedOutputs), Order.String)) {
    const output = routedOutputs[route];

    if (output === undefined || !Projection.isPerGeneration(output))
      throw new ViewFault({ message: `Circuit output ${route} requires Projection.perGeneration` });

    const { route: state, param, routeBy } = Projection.generationBinding(output);
    checkParams(state, param, routeBy);

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

      const planName = JSON.stringify([route, name]);
      const expression = outputExpression(relation, collection.key, routeBy);
      const plan = compilePlan(planName, expression, parameters);
      plans[planName] = orderPlanFields(
        routeBy === undefined ? plan : { ...plan, routeArity: routeBy.length },
      );
      targets.set(planName, { route, collection: name, routeBy });

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
    ViewBackend | Input["Identifier"],
    CheckpointStore | Transaction | ViewBackend,
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
        const backend = yield* currentBackend;

        // Unit params are decoded. Re-encode through the declaration to obtain the
        // exact checkpoint identity, including transformed parameter codecs. Source
        // validation still runs before encoding or reading/writing the plan hash.
        const prepareNamespace = Effect.gen(function* () {
          const boundParams: Record<string, string> = {};

          for (const [name, codec] of Record.toEntries(definition.params))
            boundParams[name] = yield* Schema.encodeEffect(codec)(unit.params[name]);

          return yield* guard(
            backend,
            {
              id: unit.projectionId,
              generation: unit.generation,
              params: boundParams,
            },
            hash,
          );
        });

        const { namespace, changes } = yield* normalizeUnit(
          engine,
          backend,
          prepareNamespace,
          sources,
          batch,
        );

        const changesOut = yield* engine.process(backend, namespace, changes, unit.params);

        type HandlerOutput = (typeof outputs)[string];

        type Address = Exclude<HandlerOutput, { readonly open: unknown }>;

        const entriesByStream = new Map<
          string,
          {
            readonly route: string;
            readonly address: Address;
            readonly entries: DurableState.Entry<Input["collections"]>[];
          }
        >();

        const opened = new Map<string, Address>();

        for (const change of changesOut) {
          const target = targets.get(change.view);
          const plan = plans[change.view];

          if (target === undefined || plan === undefined)
            throw new Error(`Missing output for view ${change.view}`);
          const { collection, route, routeBy } = target;
          const output = outputs[route];
          const options = { headers: { txid: unit.key } };
          let key = String(change.key);
          let value = change.kind === "upsert" ? change.value : undefined;
          let address: Address;

          if (output === undefined) throw new Error(`Missing circuit output ${route}`);

          if ("open" in output) {
            if (routeBy === undefined || !Array.isArray<typeof change.key>(change.key))
              throw new Error(`Missing route key for view ${change.view}`);

            const parts = change.key.slice(0, plan.routeArity);
            key = String(change.key[routeBy.length]);
            const identity = encodeRowKey([route, ...parts]);
            const known = opened.get(identity);

            if (known === undefined) {
              const values = Record.fromEntries(Array.zip(routeBy, parts));
              address = yield* output.open(values).pipe(
                Effect.catchIf(
                  (fault) => fault.reason === "invalid-output" && fault.cause instanceof RangeError,
                  (fault) =>
                    Effect.fail(
                      dataFault({ view: change.view, node: plan.output })(
                        key,
                        `Cannot route row ${key}. ${fault.message}`,
                      ),
                    ),
                ),
              );
              opened.set(identity, address);
            } else address = known;

            if (value !== undefined) {
              const row = value.value;

              if (row === undefined || !isJsonObject(row))
                return yield* dataFault({ view: change.view, node: plan.output })(
                  key,
                  "row projection must produce a JSON object",
                );
              value = row;
            }
          } else address = output;

          const entry =
            value === undefined
              ? DurableState.remove(collection, key, options)
              : DurableState.upsert(collection, value, options);

          const pending = entriesByStream.get(address.id);

          if (pending === undefined)
            entriesByStream.set(address.id, { route, address, entries: [entry] });
          else pending.entries.push(entry);
        }

        // Keep declaration order for fixed outputs and group each routed stream once.
        const ordered = Array.groupBy(
          Array.fromIterable(entriesByStream.values()),
          ({ route }) => route,
        );

        for (const { address: output, entries } of Array.flatMap(
          Record.keys(outputs),
          (route) => ordered[route] ?? [],
        )) {
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
      }).pipe(circuitErrors),
  });
}
