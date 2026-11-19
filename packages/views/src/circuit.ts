import { Array, Effect, Option, Order, Predicate, Record, Schema } from "effect";
import { DurableState, isStreamsFault } from "@streamsy/core";
import { Streams, StreamsWriter, type StreamRoute } from "@streamsy/core/protocol";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionFault,
  Transaction,
  type ProjectionKey,
  type Declaration,
  type Slices,
  type Unit,
} from "@streamsy/projection";
import { SqlClient } from "effect/sql";
import { compilePlan } from "./relation.ts";
import { keyExpression } from "./expression.ts";
import { circuitPlanHash, encodePlan } from "./plan.ts";
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
import { normalize } from "./sql/normalize.ts";
import type { SqlRelationPlan } from "./ir/contracts.ts";

type Views = Readonly<Record<string, Pick<Relation<unknown>, "expression" | "inputs" | "rowType">>>;

type InputsOf<V extends Views> = {
  readonly [name: string]: V[keyof V]["inputs"] extends ReadonlyMap<
    string,
    { readonly state: infer I extends Input }
  >
    ? I
    : never;
};

type CollectionsOf<O extends Input> = O["collections"];

type Fits<V extends Views, O extends Input> = {
  readonly [K in keyof V]: K extends keyof CollectionsOf<O>
    ? V[K] extends { readonly rowType?: infer Row }
      ? [Row] extends [CollectionsOf<O>[K]["schema"]["Type"]]
        ? V[K]
        : never
      : never
    : never;
};

type InvalidParams<C extends StreamRoute.ParamCodecs, I extends Input> = I extends Input
  ? {
      [K in keyof I["paramCodecs"]]: K extends keyof C
        ? [StreamRoute.Params<I["paramCodecs"]>[K]] extends [StreamRoute.Params<C>[K]]
          ? [StreamRoute.Params<C>[K]] extends [StreamRoute.Params<I["paramCodecs"]>[K]]
            ? never
            : K
          : K
        : K;
    }[keyof I["paramCodecs"]]
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

type CircuitError = PlanChanged | ViewFault | ProjectionFault;

type Definition<C extends StreamRoute.ParamCodecs, V extends Views, O extends Input, HE, HR> = {
  readonly id: string;
  readonly version?: number;
  readonly generation?: number;
  readonly params: C;
  readonly output: O & (InvalidParams<NoInfer<C>, O> extends never ? unknown : never);
  readonly views: V & Fits<V, NoInfer<O>> & ValidInputs<NoInfer<C>, V>;
  readonly retry?: Declaration<C, CircuitBinding<V, O>, HE, HR>["retry"];
};

export class PlanChanged extends Schema.TaggedError<
  PlanChanged,
  { readonly brand: unique symbol }
>()("ViewPlanChanged", {
  namespace: Schema.String,
  stored: Schema.String,
  requested: Schema.String,
}) {}

export class ViewFault extends Schema.TaggedError<ViewFault, { readonly brand: unique symbol }>()(
  "ViewFault",
  {
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** Same projection binding and generation, independent of the unit's ranges. */
function identity(key: ProjectionKey) {
  const params = Record.fromEntries(
    Array.map(Array.sort(Record.keys(key.params), Order.String), (name) => [
      name,
      key.params[name],
    ]),
  );

  const projectionKey = JSON.stringify([key.id, key.version, params]);

  return { projectionKey, namespace: JSON.stringify([projectionKey, key.generation]) };
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
  const { projectionKey, namespace } = identity(key);
  const state = circuitState(sql, projectionKey);
  const generations = yield* state.generations();

  for (const previous of generations) {
    if (previous.generation > key.generation)
      return yield* new ViewFault({ message: "A newer circuit generation has already run" });

    if (previous.namespace === namespace && previous.plan_hash !== hash)
      return yield* new PlanChanged({ namespace, stored: previous.plan_hash, requested: hash });
  }

  for (const previous of generations)
    if (previous.generation < key.generation) yield* clearNamespace(sql, previous.namespace);

  if (!Array.some(generations, (previous) => previous.namespace === namespace))
    yield* state.save(namespace, key.generation, hash);

  return namespace;
});

/** The public Commit.remove hook keeps Projection.forget fully compatible. */
const commit = Commit.make<void, SqlClient.SqlClient, CheckpointStore | Transaction>({
  name: "views-transaction",
  guarantee: Commit.transaction.guarantee,
  load: Commit.transaction.load,
  apply: Commit.transaction.apply,
  remove: (key) =>
    Effect.gen(function* () {
      const tx = yield* Transaction;
      const remove = Commit.transaction.remove;

      if (remove === undefined) throw new TypeError("Commit.transaction must support removal");
      yield* tx.withTransaction(
        Effect.gen(function* () {
          yield* remove(key);
          yield* clearNamespace(tx.sql, identity(key).namespace).pipe(
            Effect.mapError(
              (cause) =>
                new ProjectionFault({
                  phase: "checkpoint",
                  reason: "storage-failure",
                  message: "Cannot forget circuit state",
                  cause,
                }),
            ),
          );
        }),
      );
    }),
});

/** Alias for Projection.forget; circuit commits also remove operator state. */
export const forget = Projection.forget;

export interface CircuitBinding<V extends Views, O extends Input> extends ProjectionKey {
  readonly inputs: { readonly [name: string]: ReturnType<InputsOf<V>[string]["at"]> };
  readonly state: undefined;
  readonly streams: ReturnType<
    ReturnType<typeof Projection.process<StreamRoute.ParamCodecs, InputsOf<V>, { output: O }>>["at"]
  >["streams"];
  readonly commit: Commit.Commit<void, SqlClient.SqlClient, CheckpointStore | Transaction>;
  readonly process: (
    batch: Slices<CircuitBinding<V, O>["inputs"]>,
    unit: Unit,
    encoded?: string,
  ) => Effect.Effect<void, CircuitError, SqlClient.SqlClient | O["Identifier"]>;
}

export interface CircuitDeclaration<
  C extends StreamRoute.ParamCodecs,
  V extends Views,
  O extends Input,
  HE = never,
  HR = never,
> extends Declaration<C, CircuitBinding<V, O>, HE, HR> {}

/** A group of views is one transactional Projection.process declaration. */
export function circuit<
  C extends StreamRoute.ParamCodecs,
  const V extends Views,
  O extends Input,
  HE = never,
  HR = never,
>(definition: Definition<C, V, O, HE, HR>): CircuitDeclaration<C, V, O, HE, HR> {
  const plans: Record<string, SqlRelationPlan> = {};
  const sources = new Map<string, { readonly state: Input; readonly collection: string }>();

  for (const name of Array.sort(Record.keys(definition.views), Order.String)) {
    const relation = definition.views[name];

    if (relation === undefined) throw new RangeError(`Missing view ${name}`);
    const collection = definition.output.collections[name];

    if (collection === undefined) throw new RangeError(`Unknown output collection ${name}`);

    const expression = {
      kind: "key",
      input: relation.expression,
      key: keyExpression(collection.key),
    } as const;

    // SAFETY: canonical JSON is a round trip of the typed compiler output.
    // Canonical field order keeps SQL arrangement identities stable when a
    // caller reorders object properties without changing the plan hash.
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Only our validated JSON plan is deserialized here, never external input.
    plans[name] = JSON.parse(
      encodePlan(
        compilePlan(name, expression, { name: `${definition.output.key}/${name}`, version: 1 }),
      ),
    ) as SqlRelationPlan;

    for (const [id, input] of relation.inputs) sources.set(id, input);
  }

  const inputs: Record<string, Input> = {};

  for (const { state } of sources.values()) inputs[state.key] = state;

  // The public signature retains the input union. Runtime checks cover params
  // before delegating to the ordinary Projection declaration builder.
  for (const state of [...Record.values(inputs), definition.output]) {
    for (const name of Record.keys(state.paramCodecs))
      if (!Record.has(definition.params, name))
        throw new RangeError(`Missing circuit parameter ${name}`);
  }

  // SAFETY: inputs were collected only from V's relation declarations; their
  // params are checked above. Projection binds and validates every output.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Runtime collection erases the mapped input union, restored here at the compiler adapter boundary.
  const checkedInputs = inputs as Parameters<
    typeof Projection.process<C, InputsOf<V>, { output: O }>
  >[0]["inputs"];

  // SAFETY: output is exactly O; its parameter names were checked above.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Projection's conditional mapped Within type cannot reduce for generic O.
  const routedOutputs = { output: definition.output } as Parameters<
    typeof Projection.process<C, InputsOf<V>, { output: O }>
  >[0]["outputs"];

  const hash = circuitPlanHash(plans);
  const engine = Effect.runSync(makeEngine(plans));

  return Projection.process<
    C,
    InputsOf<V>,
    { output: O },
    CircuitError,
    SqlClient.SqlClient | O["Identifier"],
    CheckpointStore | Transaction,
    void,
    never,
    never,
    undefined,
    HE,
    HR
  >({
    ...definition,
    inputs: checkedInputs,
    outputs: routedOutputs,
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

        for (const [sourceId, input] of sources) {
          const slice = batch[input.state.key];

          if (slice === undefined) continue;

          const source = Array.findFirst(
            engine.program.nodes,
            (node) => node.kind === "source" && node.sourceId === sourceId,
          );

          if (Option.isNone(source)) throw new TypeError(`Missing compiled source ${sourceId}`);
          changes.push(
            ...(yield* normalize(
              storeFor(sql, namespace),
              sourceId,
              source.value.id,
              input.collection,
              slice.items,
            )),
          );
        }

        const changesOut = yield* engine.process(namespace, changes);
        const output = outputs.output;

        const entries = Array.map(changesOut, (change) =>
          change.kind === "upsert"
            ? DurableState.upsert(change.view, change.value, { headers: { txid: unit.key } })
            : DurableState.remove(change.view, String(change.key), { headers: { txid: unit.key } }),
        );

        // A State class's stream requires its service identifier. The runtime
        // input abstraction erases that requirement; retain O at this boundary.
        const service: Effect.Effect<
          Effect.Success<typeof output.stream>,
          never,
          O["Identifier"]
        > = output.stream;

        const { wire } = yield* service;
        yield* wire.create(output.id, { contentType: output.contentType });

        if (Array.isReadonlyArrayNonEmpty(entries)) {
          const events = DurableState.changes(output, {}, entries);
          yield* Streams.append(output, events).pipe(Effect.provideService(StreamsWriter, wire));
        }
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
        Effect.mapError((cause) =>
          Schema.is(PlanChanged)(cause) ||
          Schema.is(ViewFault)(cause) ||
          Schema.is(ProjectionFault)(cause)
            ? cause
            : new ProjectionFault({
                phase: "process",
                reason: isStreamsFault(cause)
                  ? "storage-failure"
                  : Predicate.isTagged(cause, "StreamBusy")
                    ? "output-busy"
                    : "invalid-output",
                message: "Cannot append circuit output",
                cause,
              }),
        ),
      ),
  });
}
