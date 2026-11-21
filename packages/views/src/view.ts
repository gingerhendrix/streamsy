import { Array } from "effect";
import type { StreamAddress, Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import { deepFreeze, type RelationExpression } from "./relation.ts";
import {
  aggregate,
  joinSelectors,
  keyExpression,
  selectors,
  type AggregateValue,
  type BooleanExpression,
  type Selected,
  type Reference,
  type TypedAggregateExpression,
  type TypedExpression,
  type ValueExpression,
} from "./expression.ts";
import type { Expression, JsonObject, JsonValue, RowKeyPart, SortTerm } from "./ir/contracts.ts";

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

export interface Relation<Row, Inputs extends Input = Input> {
  readonly rowType?: Row;
  readonly expression: RelationExpression<Row>;
  readonly inputs: ReadonlyMap<string, { readonly state: Inputs; readonly collection: string }>;
  readonly where: (predicate: (row: Reference<Row>) => BooleanExpression) => Relation<Row, Inputs>;
  readonly select: <F extends Readonly<Record<string, Expression>>>(
    fields: (row: Reference<Row>) => F,
  ) => Relation<Selected<F>, Inputs>;
  readonly selectRow: <Result extends JsonObject>(
    expression: (row: Reference<Row>) => TypedExpression<Result>,
  ) => Relation<Result, Inputs>;
  readonly concat: <Other, OtherInputs extends Input>(
    other: Relation<Other, OtherInputs>,
  ) => Relation<Row | Other, Inputs | OtherInputs>;
  readonly closure: <Part extends JsonValue | undefined = undefined>(spec: {
    readonly parent: (row: Reference<Row>) => ValueExpression<string | undefined>;
    readonly part?: (row: Reference<Row>) => ValueExpression<Part>;
    readonly maxDepth: number;
  }) => Relation<
    {
      readonly node: string;
      readonly ancestor: string;
      readonly depth: number;
      readonly path?: readonly Exclude<Part, undefined>[];
    },
    Inputs
  >;
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

type RouteFields = Readonly<Record<string, ValueExpression<RowKeyPart>>>;

/** A circuit view that supplies explicit route expressions and an optional wire row. */
export interface RoutedView<Row, Inputs extends Input, By extends RouteFields> extends Pick<
  Relation<Row, Inputs>,
  "rowType" | "expression" | "inputs"
> {
  readonly routing: { readonly by: By; readonly row: Expression };
}

export function routed<Row, Inputs extends Input, const By extends RouteFields>(
  source: Relation<Row, Inputs>,
  spec: { readonly by: (row: Reference<Row>) => By; readonly row?: never },
): RoutedView<Row, Inputs, By>;
export function routed<
  Row,
  Inputs extends Input,
  const By extends RouteFields,
  Result extends JsonObject,
>(
  source: Relation<Row, Inputs>,
  spec: {
    readonly by: (row: Reference<Row>) => By;
    readonly row: (row: Reference<Row>) => TypedExpression<Result>;
  },
): RoutedView<Result, Inputs, By>;
export function routed<Row, Inputs extends Input, By extends RouteFields>(
  source: Relation<Row, Inputs>,
  spec: {
    readonly by: (row: Reference<Row>) => By;
    readonly row?: (row: Reference<Row>) => Expression;
  },
): RoutedView<unknown, Inputs, By> {
  const { row } = selectors<Row>();

  return {
    expression: source.expression,
    inputs: source.inputs,
    routing: deepFreeze({ by: spec.by(row), row: spec.row?.(row) ?? row }),
  };
}

function relation<Row, Inputs extends Input>(
  expression: RelationExpression<Row>,
  inputs: Relation<Row, Inputs>["inputs"],
): Relation<Row, Inputs> {
  const row = selectors<Row>().row;

  const combineInputs = <Other extends Input>(other: Relation<unknown, Other>["inputs"]) =>
    new Map<string, { readonly state: Inputs | Other; readonly collection: string }>([
      ...inputs,
      ...other,
    ]);

  const join = <Right, Other extends Input, Alias extends string>(
    other: Relation<Right, Other>,
    spec: {
      readonly as: Alias;
      readonly on: (left: Reference<Row>, right: Reference<Right>) => BooleanExpression;
    },
  ) => {
    const refs = joinSelectors<Row, Right>();
    const on = spec.on(refs.left, refs.right);

    return {
      left: expression,
      right: other.expression,
      rightAlias: spec.as,
      on,
      combined: combineInputs(other.inputs),
    };
  };

  return {
    expression: deepFreeze(expression),
    inputs,
    where: (predicate) =>
      relation({ kind: "filter", input: expression, predicate: predicate(row) }, inputs),
    select: (fields) =>
      relation({ kind: "project", input: expression, fields: fields(row) }, inputs),
    selectRow: (project) =>
      relation({ kind: "project", input: expression, row: project(row) }, inputs),
    concat: (other) =>
      relation(
        { kind: "concat", left: expression, right: other.expression },
        combineInputs(other.inputs),
      ),
    closure: (spec) => {
      const node = {
        kind: "closure",
        input: expression,
        parent: spec.parent(row),
        maxDepth: spec.maxDepth,
      } as const;

      return relation(spec.part === undefined ? node : { ...node, part: spec.part(row) }, inputs);
    },
    join: (other, spec) => {
      const { combined, ...joined } = join(other, spec);

      return relation({ ...joined, kind: "inner-join" }, combined);
    },
    leftJoin: (other, spec) => {
      const { combined, ...joined } = join(other, spec);

      return relation({ ...joined, kind: "left-join" }, combined);
    },
    groupBy: (fields) => ({
      aggregate: (build) =>
        relation(
          {
            kind: "grouped-aggregate",
            input: expression,
            groupBy: fields(row),
            aggregates: build(row),
          },
          inputs,
        ),
    }),
    top: (spec) => {
      const top = {
        kind: "top-n",
        input: expression,
        orderBy: spec.by(row),
        limit: spec.limit,
      } as const;

      if (spec.partitionBy === undefined) return relation(top, inputs);

      const partition = spec.partitionBy(row);

      const partitionBy = Array.isArray<Expression | readonly Expression[]>(partition)
        ? partition
        : [partition];

      return relation({ ...top, partitionBy }, inputs);
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

  return relation(
    { kind: "source", sourceId: id, key: keyExpression(entry.key), stateKey: true },
    new Map([[id, { state, collection }]]),
  );
}

export const { count, sum, min, max, list } = aggregate;

export { field, fn, literal } from "./expression.ts";

const { key: compositeKey, keyRef } = selectors<never>();

export const key = Object.assign(compositeKey, {
  asc: (): SortTerm => ({ expression: keyRef, direction: "ascending" }),
});

export { circuit, migrate } from "./circuit.ts";

export type { CircuitDeclaration, CircuitBinding } from "./circuit.ts";
