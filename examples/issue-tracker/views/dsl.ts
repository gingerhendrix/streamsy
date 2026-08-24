/**
 * The declaration vocabulary: `source`, `reducer`, `view`, `from`,
 * `reduceByKey`, `stateSink`.
 *
 * Every constructor here returns frozen, inert data. Nothing runs, decodes,
 * appends, or reads a clock. That is the whole point: a declaration can be
 * compiled to a {@link RelationPlan}, hashed, and stored, and the same value
 * drives the memory host and the SQLite host without a second definition.
 *
 * Effect Schemas travel on the declaration because they *are* inert data too,
 * but the plan itself only carries a {@link DescriptorRef}, so a plan hash does
 * not move when an unrelated schema helper is refactored.
 */
import type { Schema } from "effect";
import type { DescriptorRef, Expression, RelationNode, RelationPlan } from "./contracts.ts";
import { selectors, type Reference } from "./expression.ts";

export interface SourceSpec<S extends Schema.Top> {
  readonly schema: S;
  readonly schemaRef: DescriptorRef;
  /** Source identity of one item. */
  readonly key: Expression;
  /** Deterministic order within one partition. */
  readonly order: Expression;
  readonly partitionBy: Expression;
}

export interface SourceDeclaration<S extends Schema.Top = Schema.Top> extends SourceSpec<S> {
  readonly kind: "source";
  readonly name: string;
}

export const source = <S extends Schema.Top>(
  name: string,
  spec: SourceSpec<S>,
): SourceDeclaration<S> => Object.freeze({ kind: "source", name, ...spec });

/**
 * One evolve branch: the fields this event writes onto the fold's state.
 *
 * A branch is a field patch, not a whole state, so `IssueStatusChanged` states
 * only what it changes. The engine merges it over the previous state and
 * decodes the result through the reducer's state schema, so a branch that
 * leaves a creation incomplete is a typed failure rather than a bad row.
 */
export type EvolveBranch = Readonly<Record<string, Expression>>;

/**
 * A branch is written as a function of that branch's own scopes.
 *
 * The function runs once, when the declaration is built, and its result is the
 * inert expression record that gets planned and hashed. Taking scopes as an
 * argument is what lets `IssueCreated` read `event.title` while
 * `IssueStatusChanged` — which has no title — cannot.
 */
export type EvolveBranchBuilder<Event, State> = (x: {
  readonly event: Reference<Event>;
  readonly state: Reference<State>;
}) => EvolveBranch;

type Tagged<Input, D extends string> = Extract<Input, Record<D, string>>;
type TagsOf<Input, D extends string> = Tagged<Input, D>[D] & string;

export interface ReducerSpec<State extends Schema.Top, Input extends Schema.Top, D extends string> {
  readonly state: State;
  readonly stateRef: DescriptorRef;
  readonly input: Input;
  /** Field of the input that selects the evolve branch. */
  readonly discriminator: D;
  readonly evolve: {
    readonly [Tag in TagsOf<Input["Type"], D>]: EvolveBranchBuilder<
      Extract<Input["Type"], Record<D, Tag>>,
      State["Type"]
    >;
  };
}

export interface ReducerDeclaration<
  State extends Schema.Top = Schema.Top,
  Input extends Schema.Top = Schema.Top,
> {
  readonly kind: "reducer";
  readonly ref: DescriptorRef;
  readonly state: State;
  readonly stateRef: DescriptorRef;
  readonly input: Input;
  readonly discriminator: string;
  /** The built branches: inert expression records, keyed by discriminator value. */
  readonly evolve: Readonly<Record<string, EvolveBranch>>;
}

/**
 * A named, deterministic fold.
 *
 * Determinism is what makes checkpointed recovery sound: replaying the same
 * event suffix must rebuild the same state on any host, in any process. The
 * built declaration therefore holds expressions only — no callbacks, no
 * `Effect`, no clock.
 */
export const reducer = <State extends Schema.Top, Input extends Schema.Top, D extends string>(
  ref: DescriptorRef,
  spec: ReducerSpec<State, Input, D>,
): ReducerDeclaration<State, Input> => {
  const evolve: Record<string, EvolveBranch> = {};
  for (const [tag, build] of Object.entries(spec.evolve)) {
    const x = selectors<unknown, unknown, unknown>();
    // SAFETY: `spec.evolve` is a mapped type whose every value is an
    // `EvolveBranchBuilder`; `Object.entries` erases the tag that relates a
    // builder to its own event type, and the builder only reads references, so
    // widening both scopes to `unknown` produces the same expression tree.
    const branch = build as EvolveBranchBuilder<unknown, unknown>;
    evolve[tag] = Object.freeze(branch({ event: x.event, state: x.state }));
  }
  return Object.freeze({
    kind: "reducer",
    ref,
    state: spec.state,
    stateRef: spec.stateRef,
    input: spec.input,
    discriminator: spec.discriminator,
    evolve: Object.freeze(evolve),
  });
};

export interface ReduceByKeySpec {
  readonly key: Expression;
  readonly order: Expression;
  readonly reducer: ReducerDeclaration;
}

export interface ReduceByKeyExpression {
  readonly kind: "reduce-by-key";
  readonly input: SourceDeclaration;
  readonly spec: ReduceByKeySpec;
}

export type RelationExpression = ReduceByKeyExpression;

export interface RelationBuilder {
  readonly reduceByKey: (spec: ReduceByKeySpec) => ReduceByKeyExpression;
}

/** Start a relation expression from a declared source. */
export const from = (input: SourceDeclaration): RelationBuilder =>
  Object.freeze({
    reduceByKey: (spec: ReduceByKeySpec): ReduceByKeyExpression =>
      Object.freeze({ kind: "reduce-by-key", input, spec }),
  });

export interface ViewSpec<S extends Schema.Top> {
  readonly schema: S;
  readonly schemaRef: DescriptorRef;
  readonly key: Expression;
}

export interface ViewDeclaration<S extends Schema.Top = Schema.Top> extends ViewSpec<S> {
  readonly kind: "view";
  readonly name: string;
  readonly expression: RelationExpression;
  readonly plan: RelationPlan;
}

export const view = <S extends Schema.Top>(
  name: string,
  spec: ViewSpec<S>,
  expression: RelationExpression,
): ViewDeclaration<S> =>
  Object.freeze({ kind: "view", name, ...spec, expression, plan: compilePlan(name, expression) });

export interface Scope {
  readonly kind: "scope";
  readonly value: string;
}

export const scope = (value: string): Scope => Object.freeze({ kind: "scope", value });

export interface StateSinkSpec {
  readonly from: ViewDeclaration;
  readonly key: Expression;
  /** Route template. `:name` segments must be listed in `params`. */
  readonly route: string;
  /**
   * Route parameter names.
   *
   * The draft wrote `params: { workspaceId: x.route.workspaceId }`. A `route`
   * expression scope would have exactly one legal shape here, so the slice
   * lists the names instead and keeps the expression vocabulary to the scopes
   * it can actually evaluate.
   */
  readonly params: readonly string[];
  readonly protocol: {
    readonly transport: "durable-state";
    readonly resume: boolean;
    readonly fallback: "snapshot-then-live";
  };
  readonly auth: Scope;
}

export interface StateSinkDeclaration extends StateSinkSpec {
  readonly kind: "state-sink";
  readonly name: string;
}

export const stateSink = (name: string, spec: StateSinkSpec): StateSinkDeclaration =>
  Object.freeze({ kind: "state-sink", name, ...spec });

/** Lower a relation expression into the serializable plan the engine executes. */
export function compilePlan(name: string, expression: RelationExpression): RelationPlan {
  const sourceNode: RelationNode = {
    kind: "source",
    id: expression.input.name,
    schema: expression.input.schemaRef,
    sourceId: expression.input.name,
    key: expression.input.key,
    order: expression.input.order,
    partitionBy: expression.input.partitionBy,
  };
  const reduceNode: RelationNode = {
    kind: "reduce-by-key",
    id: name,
    schema: expression.spec.reducer.stateRef,
    input: sourceNode.id,
    key: expression.spec.key,
    order: expression.spec.order,
    reducer: expression.spec.reducer.ref,
  };
  return Object.freeze({
    version: 1,
    name,
    nodes: Object.freeze([sourceNode, reduceNode]),
    output: reduceNode.id,
  });
}
