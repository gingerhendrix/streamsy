import { Array } from "effect";
import type { StreamAddress, Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import {
  from as relationFrom,
  buildRelation,
  source,
  type RelationBuilder,
  type RelationExpression,
} from "./relation.ts";
import {
  aggregate,
  joinSelectors,
  literal,
  selectors,
  type AggregateValue,
  type BooleanExpression,
  type ExpressionValue,
  type Reference,
  type TypedAggregateExpression,
} from "./expression.ts";
import type { Expression, SortTerm } from "./ir/contracts.ts";

/** A State declaration, with the same structural contract as StateTable.define. */
export type State<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  R,
> = StreamRoute.StreamRoute<StreamRoute.Params<Codecs>, StreamAddress.CollectionsEvent<C>> &
  Streamsy.Any & {
    readonly collections: C;
    readonly paramCodecs: Codecs;
    readonly at: (
      params: StreamRoute.Params<Codecs>,
    ) => StreamAddress.StateAddress<C, never, never> &
      Streamsy.Address<StreamAddress.CollectionsEvent<C>, R>;
  };

// The existential declaration boundary mirrors Streamsy's Inputs type. Exact
// declarations survive in Relation's Inputs parameter and the circuit signature.
/* oxlint-disable typescript/no-explicit-any -- Existential State classes retain exact param, collection, and service types at the generic boundary. */
export type Input = StreamRoute.StreamRoute<any, any> &
  Streamsy.Any & {
    readonly collections: StreamAddress.Collections;
    readonly paramCodecs: StreamRoute.ParamCodecs;
    readonly at: (
      params: any,
    ) => StreamAddress.StateAddress<any, never, never> & Streamsy.Address<any, any>;
  };
/* oxlint-enable typescript/no-explicit-any */

type Selected<F extends Readonly<Record<string, Expression>>> = {
  readonly [K in keyof F]: ExpressionValue<F[K]>;
};

export interface Relation<Row, Inputs extends Input = Input> {
  readonly rowType?: Row;
  readonly expression: RelationExpression<Row>;
  readonly inputs: ReadonlyMap<string, { readonly state: Inputs; readonly collection: string }>;
  readonly where: (predicate: (row: Reference<Row>) => BooleanExpression) => Relation<Row, Inputs>;
  readonly select: <F extends Readonly<Record<string, Expression>>>(
    fields: (row: Reference<Row>) => F,
  ) => Relation<Selected<F>, Inputs>;
  readonly join: <Right, Other extends Input, Alias extends string>(
    other: Relation<Right, Other>,
    spec: {
      readonly as: Alias;
      readonly on: (left: Reference<Row>, right: Reference<Right>) => BooleanExpression;
    },
  ) => Relation<Row & Record<Alias, Right>, Inputs | Other>;
  readonly leftJoin: <Right, Other extends Input, Alias extends string>(
    other: Relation<Right, Other>,
    spec: {
      readonly as: Alias;
      readonly on: (left: Reference<Row>, right: Reference<Right>) => BooleanExpression;
    },
  ) => Relation<Row & Partial<Record<Alias, Right>>, Inputs | Other>;
  readonly groupBy: <F extends Readonly<Record<string, Expression>>>(
    fields: (row: Reference<Row>) => F,
  ) => {
    readonly aggregate: <A extends Readonly<Record<string, TypedAggregateExpression<unknown>>>>(
      aggregates: (row: Reference<Row>) => A,
    ) => Relation<Selected<F> & { readonly [K in keyof A]: AggregateValue<A[K]> }, Inputs>;
  };
  readonly top: (spec: {
    readonly partitionBy?: (row: Reference<Row>) => Expression | readonly Expression[];
    readonly by: (row: Reference<Row>) => readonly SortTerm[];
    readonly limit: number;
  }) => Relation<Row, Inputs>;
}

function wrap<Row, Inputs extends Input>(
  relation: RelationBuilder<Row>,
  inputs: Relation<Row, Inputs>["inputs"],
): Relation<Row, Inputs> {
  const row = selectors<Row>().row;

  const join = <Right, Other extends Input, Alias extends string>(
    other: Relation<Right, Other>,
    spec: {
      readonly as: Alias;
      readonly on: (left: Reference<Row>, right: Reference<Right>) => BooleanExpression;
    },
  ) => {
    const refs = joinSelectors<Row, Right>();
    const on = spec.on(refs.left, refs.right);

    const combined = new Map<
      string,
      { readonly state: Inputs | Other; readonly collection: string }
    >(inputs);

    for (const [id, input] of other.inputs) combined.set(id, input);

    return {
      left: relation.expression,
      right: other.expression,
      rightAlias: spec.as,
      on,
      combined,
    };
  };

  return {
    expression: relation.expression,
    inputs,
    where: (predicate) => wrap(relation.where(predicate(row)), inputs),
    select: (fields) => wrap(relation.select(fields(row)), inputs),
    join: (other, spec) => {
      const { combined, ...expression } = join(other, spec);

      return wrap(buildRelation({ ...expression, kind: "inner-join" }), combined);
    },
    leftJoin: (other, spec) => {
      const { combined, ...expression } = join(other, spec);

      return wrap(buildRelation({ ...expression, kind: "left-join" }), combined);
    },
    groupBy: (fields) => ({
      aggregate: (build) => wrap(relation.groupBy(fields(row)).aggregate(build(row)), inputs),
    }),
    top: (spec) => {
      const by = spec.by(row);

      if (spec.partitionBy === undefined)
        return wrap(relation.top({ by, limit: spec.limit }), inputs);
      const partition = spec.partitionBy(row);

      const partitionBy = Array.isArray<Expression | readonly Expression[]>(partition)
        ? partition
        : [partition];

      return wrap(relation.top({ by, limit: spec.limit, partitionBy }), inputs);
    },
  };
}

export function from<
  Codecs extends StreamRoute.ParamCodecs,
  C extends StreamAddress.Collections,
  R,
  K extends keyof C & string,
>(
  state: State<Codecs, C, R>,
  collection: K,
): Relation<C[K]["schema"]["Type"], State<Codecs, C, R>> {
  const entry = state.collections[collection];

  if (entry === undefined) throw new RangeError(`Unknown State collection ${collection}`);
  const id = JSON.stringify([state.key, collection]);

  const declaration = source(id, {
    schema: entry.schema,
    schemaRef: { name: id, version: 1 },
    key: entry.key,
    partitionBy: literal("all"),
    mode: "state",
    stateKey: true,
  });

  return wrap(relationFrom(declaration), new Map([[id, { state, collection }]]));
}

export const { count, sum, min, max } = aggregate;

export { circuit, forget, migrate, PlanChanged, ViewFault } from "./circuit.ts";

export { ViewDataFault as DataFault } from "./sql/data-fault.ts";

export type { CircuitDeclaration, CircuitBinding } from "./circuit.ts";

export * as View from "./view.ts";
