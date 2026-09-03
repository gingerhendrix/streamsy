import type { Schema } from "effect";
import type {
  AggregateExpression,
  DescriptorRef,
  Expression,
  ParameterDescriptor,
  RelationNode,
  RelationPlan,
  SortTerm,
} from "@streamsy/views-ir";
import {
  keyExpression,
  literal,
  parameterReference,
  selectors,
  type AggregateValue,
  type BooleanExpression,
  type DeclaredKey,
  type ExpressionValue,
  type KeyFieldsOf,
  type Reference,
  type TypedExpression,
  type TypedAggregateExpression,
} from "./expression.ts";

type SchemaType<S extends Schema.Top> = S["Type"];
type Selected<Fields extends Readonly<Record<string, Expression>>> = {
  readonly [K in keyof Fields]: ExpressionValue<Fields[K]>;
};

/**
 * The Durable State collection a source is published under.
 *
 * `name` is the collection a host reads and writes, `type` is the wire tag on
 * every message in it. Declaring them here keeps a catalog's schema, type and
 * primary-key metadata derived from the same declaration that keys the plan.
 */
export interface SourceCollection {
  readonly name: string;
  readonly type: string;
}

export interface SourceSpec<
  S extends Schema.Top,
  Mode extends "facts" | "state" = "facts" | "state",
  Key extends DeclaredKey = DeclaredKey,
> {
  readonly schema: S;
  readonly schemaRef: DescriptorRef;
  readonly partitionBy: Expression;
  /** The declared key: one row field name, or an ordered tuple of them. */
  readonly key: Key;
  readonly mode: Mode;
  readonly collection?: SourceCollection;
}

export type SourceDeclaration<
  S extends Schema.Top = Schema.Top,
  Spec extends SourceSpec<S> = SourceSpec<S>,
> = Spec & {
  readonly kind: "source";
  readonly name: string;
  /** The declared key lowered to the expression the plan carries. */
  readonly keyExpression: Expression;
};

export const source = <S extends Schema.Top, const Spec extends SourceSpec<S>>(
  name: string,
  spec: Spec & SourceSpec<S> & { readonly key: KeyFieldsOf<SchemaType<S>> },
): SourceDeclaration<S, Spec> =>
  deepFreeze({
    kind: "source" as const,
    name,
    ...spec,
    keyExpression: keyExpression(spec.key),
  });

export type EvolveBranch = Readonly<Record<string, Expression>>;
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
  readonly evolve: Readonly<Record<string, EvolveBranch>>;
}

export const reducer = <State extends Schema.Top, Input extends Schema.Top, D extends string>(
  ref: DescriptorRef,
  spec: ReducerSpec<State, Input, D>,
): ReducerDeclaration<State, Input> => {
  const evolve: Record<string, EvolveBranch> = {};
  for (const [tag, build] of Object.entries(spec.evolve)) {
    const x = selectors<unknown, unknown, unknown>();
    // SAFETY: Object.entries erases the mapped tag, but every value is a builder and builders only emit references.
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion
    const branch = build as EvolveBranchBuilder<unknown, unknown>;
    evolve[tag] = deepFreeze(branch({ event: x.event, state: x.state }));
  }
  return deepFreeze({
    kind: "reducer",
    ref,
    state: spec.state,
    stateRef: spec.stateRef,
    input: spec.input,
    discriminator: spec.discriminator,
    evolve,
  });
};

interface SourceRelation<Row> {
  readonly kind: "source-relation";
  readonly source: SourceDeclaration;
  readonly _row?: Row;
}
interface FilterRelation<Row> {
  readonly kind: "filter";
  readonly input: RelationExpression<Row>;
  readonly predicate: Expression;
}
interface ProjectRelation<Row> {
  readonly kind: "project";
  readonly input: RelationExpression;
  readonly fields: Readonly<Record<string, Expression>>;
  readonly _row?: Row;
}
interface KeyRelation<Row> {
  readonly kind: "key";
  readonly input: RelationExpression<Row>;
  readonly key: Expression;
}
interface JoinRelation<Row> {
  readonly kind: "inner-join" | "left-join";
  readonly left: RelationExpression;
  readonly right: RelationExpression;
  readonly on: Expression;
  readonly rightAlias: string;
  readonly _row?: Row;
}
interface AggregateRelation<Row> {
  readonly kind: "grouped-aggregate";
  readonly input: RelationExpression;
  readonly groupBy: Readonly<Record<string, Expression>>;
  readonly aggregates: Readonly<Record<string, AggregateExpression>>;
  readonly _row?: Row;
}
interface TopRelation<Row> {
  readonly kind: "top-n";
  readonly input: RelationExpression<Row>;
  readonly orderBy: readonly SortTerm[];
  readonly limit: Expression;
  readonly partitionBy?: readonly Expression[];
}
interface ReduceByKeyRelation<Row> {
  readonly kind: "reduce-by-key";
  readonly input: SourceRelation<unknown>;
  readonly key: Expression;
  readonly reducer: ReducerDeclaration;
  readonly _row?: Row;
}

export type RelationExpression<Row = unknown> =
  | SourceRelation<Row>
  | FilterRelation<Row>
  | ProjectRelation<Row>
  | KeyRelation<Row>
  | JoinRelation<Row>
  | AggregateRelation<Row>
  | TopRelation<Row>
  | ReduceByKeyRelation<Row>;

export interface TopSpec {
  readonly by: readonly SortTerm[];
  readonly limit: number | TypedExpression<number>;
  readonly partitionBy?: readonly Expression[];
}

export interface JoinSpec {
  readonly on: BooleanExpression;
  readonly as: string;
}

export interface RelationBuilder<Row> {
  readonly expression: RelationExpression<Row>;
  readonly where: (predicate: BooleanExpression) => RelationBuilder<Row>;
  readonly select: <Fields extends Readonly<Record<string, Expression>>>(
    fields: Fields,
  ) => RelationBuilder<Selected<Fields>>;
  readonly join: <S extends Schema.Top, Alias extends string>(
    other: SourceDeclaration<S> | RelationBuilder<S["Type"]>,
    spec: JoinSpec & { readonly as: Alias },
  ) => RelationBuilder<Row & Record<Alias, S["Type"]>>;
  readonly leftJoin: <S extends Schema.Top, Alias extends string>(
    other: SourceDeclaration<S> | RelationBuilder<S["Type"]>,
    spec: JoinSpec & { readonly as: Alias },
  ) => RelationBuilder<Row & Partial<Record<Alias, S["Type"]>>>;
  readonly groupBy: <Fields extends Readonly<Record<string, Expression>>>(
    fields: Fields,
  ) => GroupedBuilder<Selected<Fields>>;
  readonly top: (spec: TopSpec) => RelationBuilder<Row>;
  readonly reduceByKey: <State extends Schema.Top>(spec: {
    readonly key: KeyFieldsOf<Row>;
    readonly reducer: ReducerDeclaration<State>;
  }) => RelationBuilder<State["Type"]>;
}

export interface GroupedBuilder<Group> {
  readonly aggregate: <
    Aggregates extends Readonly<Record<string, TypedAggregateExpression<unknown>>>,
  >(
    aggregates: Aggregates,
  ) => RelationBuilder<Group & { readonly [K in keyof Aggregates]: AggregateValue<Aggregates[K]> }>;
}

const asExpression = <Row>(
  input: SourceDeclaration | RelationBuilder<Row>,
): RelationExpression<Row> =>
  "expression" in input ? input.expression : deepFreeze({ kind: "source-relation", source: input });

const builder = <Row>(expression: RelationExpression<Row>): RelationBuilder<Row> => {
  const result: RelationBuilder<Row> = {
    expression,
    where: (predicate) => builder(deepFreeze({ kind: "filter", input: expression, predicate })),
    select: <Fields extends Readonly<Record<string, Expression>>>(fields: Fields) =>
      builder<Selected<Fields>>(deepFreeze({ kind: "project", input: expression, fields })),
    join: <S extends Schema.Top, Alias extends string>(
      other: SourceDeclaration<S> | RelationBuilder<S["Type"]>,
      spec: JoinSpec & { readonly as: Alias },
    ) =>
      builder<Row & Record<Alias, S["Type"]>>(
        deepFreeze({
          kind: "inner-join",
          left: expression,
          right: asExpression(other),
          on: spec.on,
          rightAlias: spec.as,
        }),
      ),
    leftJoin: <S extends Schema.Top, Alias extends string>(
      other: SourceDeclaration<S> | RelationBuilder<S["Type"]>,
      spec: JoinSpec & { readonly as: Alias },
    ) =>
      builder<Row & Partial<Record<Alias, S["Type"]>>>(
        deepFreeze({
          kind: "left-join",
          left: expression,
          right: asExpression(other),
          on: spec.on,
          rightAlias: spec.as,
        }),
      ),
    groupBy: <Fields extends Readonly<Record<string, Expression>>>(fields: Fields) => ({
      aggregate: <Aggregates extends Readonly<Record<string, TypedAggregateExpression<unknown>>>>(
        aggregates: Aggregates,
      ) =>
        builder<
          Selected<Fields> & {
            readonly [K in keyof Aggregates]: AggregateValue<Aggregates[K]>;
          }
        >(
          deepFreeze({
            kind: "grouped-aggregate",
            input: expression,
            groupBy: fields,
            aggregates,
          }),
        ),
    }),
    top: (spec) => {
      // oxlint-disable-next-line anti-slop/no-runtime-typeof -- TopSpec is a parsed discriminated union of a numeric literal and TypedExpression.
      const limit = typeof spec.limit === "number" ? literal(spec.limit) : spec.limit;
      const top: TopRelation<Row> =
        spec.partitionBy === undefined
          ? { kind: "top-n", input: expression, orderBy: spec.by, limit }
          : {
              kind: "top-n",
              input: expression,
              orderBy: spec.by,
              limit,
              partitionBy: spec.partitionBy,
            };
      return builder(deepFreeze(top));
    },
    reduceByKey: <State extends Schema.Top>(spec: {
      readonly key: KeyFieldsOf<Row>;
      readonly reducer: ReducerDeclaration<State>;
    }) => {
      if (expression.kind !== "source-relation" || expression.source.mode !== "facts") {
        throw new TypeError("reduceByKey is only available directly on a fact source");
      }
      return builder<State["Type"]>(
        deepFreeze({
          kind: "reduce-by-key",
          input: expression,
          key: keyExpression(spec.key),
          reducer: spec.reducer,
        }),
      );
    },
  };
  return Object.freeze(result);
};

export const from = <S extends Schema.Top>(
  input: SourceDeclaration<S>,
): RelationBuilder<SchemaType<S>> =>
  builder(deepFreeze({ kind: "source-relation", source: input }));

export interface ViewSpec<S extends Schema.Top, Key extends DeclaredKey = DeclaredKey> {
  readonly schema: S;
  readonly schemaRef: DescriptorRef;
  /** The declared output key: one row field name, or an ordered tuple of them. */
  readonly key: Key;
}

export interface ViewDeclaration<
  S extends Schema.Top = Schema.Top,
  Key extends DeclaredKey = DeclaredKey,
> extends ViewSpec<S, Key> {
  readonly kind: "view";
  readonly name: string;
  /** The declared key lowered to the expression the plan's keying node carries. */
  readonly keyExpression: Expression;
  readonly expression: RelationExpression<SchemaType<S>>;
  readonly parameters: Readonly<Record<string, ParameterDeclaration<Schema.Top>>>;
  readonly plan: RelationPlan;
}

export const view = <S extends Schema.Top, const Key extends KeyFieldsOf<SchemaType<S>>>(
  name: string,
  spec: ViewSpec<S, Key>,
  expression: RelationExpression<SchemaType<S>> | RelationBuilder<SchemaType<S>>,
): ViewDeclaration<S, Key> => makeView(name, spec, asRelation(expression), {});

/** A relation that declares what identifies one of its rows. */
export interface KeyedRelation<Key extends DeclaredKey = DeclaredKey> {
  readonly name: string;
  readonly key: Key;
}

/**
 * The change stream of a keyed relation.
 *
 * A relation's rows and a relation's changes are two different published
 * things. `changes(issues)` names the second one, so a declaration can publish
 * transitions without publishing the relation, and a consumer of the feed can
 * see which relation it is derived from.
 *
 * `order` is part of the declaration because it is the only ordering the engine
 * can honestly promise: a fact fold observes Durable Stream arrival order, so a
 * change stream is in arrival order and is never re-sorted by a domain field.
 */
export interface ChangeStreamDeclaration<Relation extends KeyedRelation = KeyedRelation> {
  readonly kind: "change-stream";
  readonly name: string;
  readonly of: Relation;
  /** The key of the relation whose changes this stream carries. */
  readonly key: Relation["key"];
  readonly order: "arrival";
}

export const changes = <const Relation extends KeyedRelation>(
  relation: Relation,
): ChangeStreamDeclaration<Relation> =>
  deepFreeze({
    kind: "change-stream",
    name: `${relation.name}.changes`,
    of: relation,
    key: relation.key,
    order: "arrival",
  });

export interface ParameterDeclaration<S extends Schema.Top> {
  readonly kind: "parameter";
  readonly name: string;
  readonly schema: S;
  readonly schemaRef: DescriptorRef;
  readonly maximum?: number;
}

export const parameter = <S extends Schema.Top>(
  name: string,
  schema: S,
  options: { readonly schemaRef?: DescriptorRef; readonly maximum?: number } = {},
): ParameterDeclaration<S> => {
  const declaration = {
    kind: "parameter",
    name,
    schema,
    schemaRef: options.schemaRef ?? { name, version: 1 },
  } as const;
  return options.maximum === undefined
    ? deepFreeze(declaration)
    : deepFreeze({ ...declaration, maximum: options.maximum });
};

type ParameterValues<P extends Readonly<Record<string, ParameterDeclaration<Schema.Top>>>> = {
  readonly [K in keyof P]: TypedExpression<P[K]["schema"]["Type"]>;
};

export const defineView = <
  S extends Schema.Top,
  P extends Readonly<Record<string, ParameterDeclaration<Schema.Top>>>,
  const Key extends KeyFieldsOf<SchemaType<S>>,
>(spec: {
  readonly name: string;
  readonly params: P;
  readonly schema: S;
  readonly schemaRef: DescriptorRef;
  readonly key: Key;
  readonly query: (params: ParameterValues<P>) => RelationBuilder<SchemaType<S>>;
}): ViewDeclaration<S, Key> => {
  const values: Record<string, TypedExpression<unknown>> = {};
  for (const name of Object.keys(spec.params)) values[name] = parameterReference(name);
  // SAFETY: values is built from exactly the keys of P, and each reference carries that parameter's decoded type only at compile time.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-known-value-widening
  const expression = spec.query(values as ParameterValues<P>).expression;
  return makeView(spec.name, spec, expression, spec.params);
};

function asRelation<Row>(
  value: RelationExpression<Row> | RelationBuilder<Row>,
): RelationExpression<Row> {
  return "expression" in value ? value.expression : value;
}

function makeView<S extends Schema.Top, Key extends DeclaredKey>(
  name: string,
  spec: ViewSpec<S, Key>,
  query: RelationExpression<SchemaType<S>>,
  parameters: Readonly<Record<string, ParameterDeclaration<Schema.Top>>>,
): ViewDeclaration<S, Key> {
  const lowered = keyExpression(spec.key);
  const expression = keyOutput(query, lowered);
  const plan = compilePlan(name, expression, spec.schemaRef, parameters);
  return deepFreeze({
    kind: "view",
    name,
    ...spec,
    keyExpression: lowered,
    expression,
    parameters,
    plan,
  });
}

/**
 * Key a view's output with its declared key.
 *
 * Keying is a property of the output relation, not a step a query writes for
 * itself, so the declared key is lowered exactly once. It is placed beneath any
 * trailing bounded-order operators, which order and partition rows that are
 * already keyed, and it is skipped when the query is keyed by construction.
 */
function keyOutput<Row>(
  expression: RelationExpression<Row>,
  key: Expression,
): RelationExpression<Row> {
  if (expression.kind === "key" || expression.kind === "reduce-by-key") return expression;
  if (expression.kind === "top-n") {
    return deepFreeze({ ...expression, input: keyOutput(expression.input, key) });
  }
  return deepFreeze({ kind: "key", input: expression, key });
}

export function compilePlan(
  name: string,
  expression: RelationExpression,
  outputSchema?: DescriptorRef,
  parameters: Readonly<Record<string, ParameterDeclaration<Schema.Top>>> = {},
): RelationPlan {
  const nodes: RelationNode[] = [];
  const seen = new Set<string>();
  let index = 0;
  const fallbackSchema = outputSchema ?? inferSchema(expression);
  const add = (node: RelationNode): string => {
    if (!seen.has(node.id)) {
      nodes.push(node);
      seen.add(node.id);
    }
    return node.id;
  };
  const walk = (relation: RelationExpression): string => {
    if (relation.kind === "source-relation") {
      return add({
        kind: "source",
        id: relation.source.name,
        schema: relation.source.schemaRef,
        sourceId: relation.source.name,
        partitionBy: relation.source.partitionBy,
        key: relation.source.keyExpression,
        mode: relation.source.mode,
      });
    }
    if (relation.kind === "reduce-by-key") {
      const input = walk(relation.input);
      return add({
        kind: "reduce-by-key",
        id: name,
        schema: relation.reducer.stateRef,
        input,
        key: relation.key,
        reducer: relation.reducer.ref,
      });
    }
    if (relation.kind === "inner-join" || relation.kind === "left-join") {
      const left = walk(relation.left);
      const right = walk(relation.right);
      const current = index++;
      const id = `${name}/${relation.kind}/${current}`;
      return add({
        kind: relation.kind,
        id,
        schema: fallbackSchema,
        left,
        right,
        on: relation.on,
        rightAlias: relation.rightAlias,
      });
    }
    if (!("input" in relation)) throw new TypeError("unsupported relation expression");
    const input = walk(relation.input);
    const current = index++;
    const id = `${name}/${relation.kind === "top-n" ? "top" : relation.kind}/${current}`;
    switch (relation.kind) {
      case "filter":
        return add({
          kind: "filter",
          id,
          schema: fallbackSchema,
          input,
          predicate: relation.predicate,
        });
      case "project":
        return add({ kind: "project", id, schema: fallbackSchema, input, fields: relation.fields });
      case "key":
        return add({ kind: "key", id, schema: fallbackSchema, input, key: relation.key });
      case "grouped-aggregate":
        return add({
          kind: "grouped-aggregate",
          id,
          schema: fallbackSchema,
          input,
          groupBy: relation.groupBy,
          aggregates: relation.aggregates,
        });
      case "top-n": {
        const maximum = maximumFor(relation.limit, parameters);
        const top = {
          kind: "top-n",
          id,
          schema: fallbackSchema,
          input,
          orderBy: relation.orderBy,
          limit: relation.limit,
          maximum,
        } as const;
        return add(
          relation.partitionBy === undefined ? top : { ...top, partitionBy: relation.partitionBy },
        );
      }
    }
    return unreachable(relation);
  };
  const output = walk(expression);
  const parameterDescriptors: Record<string, ParameterDescriptor> = {};
  for (const [key, value] of Object.entries(parameters)) {
    parameterDescriptors[key] =
      value.maximum === undefined
        ? { schema: value.schemaRef }
        : { schema: value.schemaRef, maximum: value.maximum };
  }
  const plan = {
    version: 3,
    name,
    nodes,
    output,
  } as const;
  return Object.keys(parameterDescriptors).length === 0
    ? deepFreeze(plan)
    : deepFreeze({ ...plan, parameters: parameterDescriptors });
}

function maximumFor(
  limit: Expression,
  parameters: Readonly<Record<string, ParameterDeclaration<Schema.Top>>>,
): number {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- LiteralExpression.value is parsed JsonValue; a numeric literal supplies its own bound.
  if (limit.kind === "literal" && typeof limit.value === "number") return limit.value;
  if (limit.kind === "reference" && limit.scope === "parameter" && limit.path.length === 1) {
    return parameters[limit.path[0] ?? ""]?.maximum ?? 0;
  }
  return 0;
}

function inferSchema(expression: RelationExpression): DescriptorRef {
  if (expression.kind === "source-relation") return expression.source.schemaRef;
  if (expression.kind === "reduce-by-key") return expression.reducer.stateRef;
  if (expression.kind === "inner-join" || expression.kind === "left-join")
    return inferSchema(expression.left);
  if (!("input" in expression)) throw new TypeError("unsupported relation expression");
  return inferSchema(expression.input);
}

export interface Scope {
  readonly kind: "scope";
  readonly value: string;
}
export const scope = (value: string): Scope => Object.freeze({ kind: "scope", value });

/* oxlint-disable anti-slop/no-runtime-typeof -- This is the freezing boundary for inert declaration values; schema classes are already frozen externally and are intentionally not traversed. */
function deepFreeze<T>(value: T): T {
  if (value === null || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.freeze(value);
  for (const child of Object.values(value)) deepFreeze(child);
  return value;
}
/* oxlint-enable anti-slop/no-runtime-typeof */

function unreachable(value: never): never {
  throw new TypeError(`unreachable relation ${String(value)}`);
}
