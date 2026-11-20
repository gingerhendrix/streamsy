import { Array as Arr } from "effect";
import { Invoke } from "./ir/contracts.ts";

import type {
  AggregateExpression,
  BinaryOperator,
  Expression,
  JsonValue,
  ReferenceScope,
  SortTerm,
  UnaryOperator,
  VariadicOperator,
} from "./ir/contracts.ts";

declare const ExpressionType: unique symbol;

declare const AggregateType: unique symbol;

type Present<T> = Exclude<T, undefined>;

type LiteralOperand<T> =
  Present<T> extends JsonValue
    ? Present<T> | TypedExpression<Present<T>>
    : TypedExpression<Present<T>>;

type Ordered = number | string;

type ObjectFields<T, P = Present<T>> = P extends object
  ? { readonly [K in keyof P & string]-?: TypedExpression<P[K] | Extract<T, undefined>> }
  : unknown;

interface CommonOperations<T> {
  readonly [ExpressionType]: T;
  readonly eq: (right: LiteralOperand<T>) => TypedExpression<boolean>;
  readonly ne: (right: LiteralOperand<T>) => TypedExpression<boolean>;
  readonly in: (...values: readonly LiteralOperand<T>[]) => TypedExpression<boolean>;
}

type SortOperations<T> = undefined extends T
  ? unknown
  : Present<T> extends Ordered
    ? { readonly asc: () => SortTerm; readonly desc: () => SortTerm }
    : unknown;

type OrderedOperations<T> =
  Present<T> extends Ordered
    ? {
        readonly gt: (right: LiteralOperand<T>) => TypedExpression<boolean | Extract<T, undefined>>;
        readonly gte: (
          right: LiteralOperand<T>,
        ) => TypedExpression<boolean | Extract<T, undefined>>;
        readonly lt: (right: LiteralOperand<T>) => TypedExpression<boolean | Extract<T, undefined>>;
        readonly lte: (
          right: LiteralOperand<T>,
        ) => TypedExpression<boolean | Extract<T, undefined>>;
      }
    : unknown;

type BooleanOperations<T> =
  Present<T> extends boolean
    ? {
        readonly and: (...right: readonly TypedExpression<boolean>[]) => TypedExpression<boolean>;
        readonly or: (...right: readonly TypedExpression<boolean>[]) => TypedExpression<boolean>;
        readonly not: () => TypedExpression<boolean>;
      }
    : unknown;

type NumberOperations<T> =
  Present<T> extends number
    ? {
        readonly add: (right: LiteralOperand<T>) => TypedExpression<number | Extract<T, undefined>>;
      }
    : unknown;

type OptionalOperations<T> = undefined extends T
  ? {
      readonly isPresent: () => TypedExpression<boolean>;
      readonly unwrap: () => TypedExpression<Present<T>>;
      readonly orElse: (fallback: LiteralOperand<T>) => TypedExpression<Present<T>>;
    }
  : unknown;

/** A typed facade over a frozen, JSON-only expression tree. */
export type TypedExpression<T> = Expression &
  CommonOperations<T> &
  OrderedOperations<T> &
  BooleanOperations<T> &
  NumberOperations<T> &
  SortOperations<T> &
  OptionalOperations<T> &
  ObjectFields<T>;

export type ExpressionValue<E> = E extends { readonly [ExpressionType]: infer Value }
  ? Value
  : never;

export type Selected<F extends Readonly<Record<string, Expression>>> = {
  readonly [K in keyof F as undefined extends ExpressionValue<F[K]> ? never : K]: ExpressionValue<
    F[K]
  >;
} & {
  readonly [K in keyof F as undefined extends ExpressionValue<F[K]> ? K : never]?: Exclude<
    ExpressionValue<F[K]>,
    undefined
  >;
};

export type TypedAggregateExpression<T> = AggregateExpression & { readonly [AggregateType]?: T };

export type AggregateValue<E> = E extends { readonly [AggregateType]?: infer Value }
  ? Value
  : never;

export type Reference<T> = TypedExpression<T>;

export type BooleanExpression = TypedExpression<boolean>;

const unary = <T>(operator: UnaryOperator, operand: Expression): TypedExpression<T> =>
  decorate<T>(Object.freeze({ kind: "unary", operator, operand }));

const binary = <T>(
  operator: BinaryOperator,
  left: Expression,
  right: Expression,
): TypedExpression<T> => decorate<T>(Object.freeze({ kind: "binary", operator, left, right }));

const variadic = <T>(
  operator: VariadicOperator,
  operands: readonly Expression[],
): TypedExpression<T> =>
  decorate<T>(Object.freeze({ kind: "variadic", operator, operands: Object.freeze(operands) }));

const operand = <T>(value: LiteralOperand<T>): Expression => {
  if (isExpression(value)) return value;

  return literal(value);
};

const sort = (expression: Expression, direction: SortTerm["direction"]): SortTerm =>
  Object.freeze({ expression, direction });

const operations = new Set([
  "eq",
  "ne",
  "in",
  "gt",
  "gte",
  "lt",
  "lte",
  "and",
  "or",
  "not",
  "add",
  "isPresent",
  "unwrap",
  "orElse",
  "asc",
  "desc",
]);

/* oxlint-disable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters, anti-slop/no-reflect-get, anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-chained-type-assertions, anti-slop/no-unknown-returns, anti-slop/no-known-value-widening -- Proxies are the intentional typed authoring boundary: their JSON target is a closed Expression, and the compile-time facade is covered by expression.test.ts and type-fixtures.ts. */
function decorate<T>(node: Expression): TypedExpression<T> {
  const proxy = new Proxy(node, {
    get(target, property): unknown {
      // oxlint-disable-next-line effecttsgo/prefer-effect-record -- The closed IR union is an object with varying own keys, plus a private symbol.
      if (typeof property !== "string" || Object.hasOwn(target, property))
        return Reflect.get(target, property) as unknown;

      if (!operations.has(property)) return nestedReference<T>(target, property);

      switch (property) {
        case "eq":
          return (right: LiteralOperand<T>) => binary<boolean>("equal", target, operand(right));
        case "ne":
          return (right: LiteralOperand<T>) => binary<boolean>("not-equal", target, operand(right));
        case "in":
          return (...values: readonly LiteralOperand<T>[]) =>
            variadic<boolean>("in", [target, ...Arr.map(values, operand)]);
        case "gt":
          return (right: LiteralOperand<T>) =>
            binary<boolean>("greater-than", target, operand(right));
        case "gte":
          return (right: LiteralOperand<T>) =>
            binary<boolean>("greater-than-or-equal", target, operand(right));
        case "lt":
          return (right: LiteralOperand<T>) => binary<boolean>("less-than", target, operand(right));
        case "lte":
          return (right: LiteralOperand<T>) =>
            binary<boolean>("less-than-or-equal", target, operand(right));
        case "and":
          return (...right: readonly TypedExpression<boolean>[]) =>
            variadic<boolean>("and", [target, ...right]);
        case "or":
          return (...right: readonly TypedExpression<boolean>[]) =>
            variadic<boolean>("or", [target, ...right]);
        case "not":
          return () => unary<boolean>("not", target);
        case "add":
          return (right: LiteralOperand<T>) => binary<number>("add", target, operand(right));
        case "isPresent":
          return () => unary<boolean>("is-present", target);
        case "unwrap":
          return () => unary<Present<T>>("unwrap", target);
        case "orElse":
          return (right: LiteralOperand<T>) =>
            binary<Present<T>>("or-else", target, operand(right));
        case "asc":
          return () => sort(target, "ascending");
        case "desc":
          return () => sort(target, "descending");
        default:
          throw new TypeError(`unknown expression operation ${property}`);
      }
    },
  });

  if (!isDecoratableExpression<T>(proxy))
    throw new TypeError("expression nodes must be frozen before decoration");

  return proxy;
}

function isDecoratableExpression<T>(node: Expression): node is TypedExpression<T> {
  return Object.isFrozen(node);
}

function nestedReference<T>(target: Expression, property: string): TypedExpression<T> {
  return target.kind === "reference"
    ? reference<T>(target.scope, [...target.path, property])
    : decorate<T>(Object.freeze({ kind: "field", object: target, field: property }));
}

/** Select a field whose name overlaps a fluent operation or expression member. */
export function field<T, K extends keyof Present<T> & string>(
  expression: Expression & { readonly [ExpressionType]: T },
  name: K,
): TypedExpression<Present<T>[K] | Extract<T, undefined>> {
  return nestedReference(expression, name);
}

function isExpression(value: JsonValue | Expression): value is Expression {
  return typeof value === "object" && value !== null && "kind" in value;
}
/* oxlint-enable anti-slop/no-runtime-typeof, anti-slop/no-unknown-parameters, anti-slop/no-reflect-get, anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-chained-type-assertions, anti-slop/no-unknown-returns, anti-slop/no-known-value-widening */

const reference = <T>(scope: ReferenceScope, path: readonly string[]): TypedExpression<T> =>
  decorate<T>(Object.freeze({ kind: "reference", scope, path: Object.freeze(path) }));

export const literal = <T extends JsonValue>(value: T): TypedExpression<T> =>
  decorate<T>(Object.freeze({ kind: "literal", value }));

/** Declare a named, versioned, deterministic synchronous expression function. */
export function fn<
  Args extends readonly (JsonValue | undefined)[],
  Result extends JsonValue | undefined,
>(
  name: string,
  version: number,
  implementation: (...args: Args) => Result,
): (
  ...operands: { readonly [K in keyof Args]: Expression & { readonly [ExpressionType]: Args[K] } }
) => TypedExpression<Result> {
  if (name.trim().length === 0 || !Number.isSafeInteger(version) || version < 1)
    throw new RangeError("View.fn requires a name and a positive integer version");

  return (...operands) =>
    decorate<Result>(
      Object.freeze({
        kind: "call",
        name,
        version,
        operands: Object.freeze(operands),
        // SAFETY: the returned expression builder checks each operand against the corresponding argument.
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- Restore the tuple whose values came from the checked operands.
        [Invoke]: (args: readonly (JsonValue | undefined)[]) => implementation(...(args as Args)),
      }),
    );
}

export interface Selectors<Row, Event, State, Parameter, Right> {
  readonly row: Reference<Row>;
  readonly left: Reference<Row>;
  readonly right: Reference<Right>;
  readonly keyRef: Reference<unknown>;
  readonly event: Reference<Event>;
  readonly state: Reference<State>;
  readonly parameter: Reference<Parameter>;
  readonly literal: typeof literal;
  readonly key: (
    ...parts: readonly (
      | TypedExpression<boolean>
      | TypedExpression<number>
      | TypedExpression<string>
    )[]
  ) => TypedExpression<readonly RowKeyValue[]>;
}

type RowKeyValue = boolean | number | string;

export const selectors = <
  Row,
  Event = Row,
  State = Row,
  Parameter = never,
  Right = Row,
>(): Selectors<Row, Event, State, Parameter, Right> =>
  Object.freeze({
    row: reference<Row>("row", []),
    left: reference<Row>("left", []),
    right: reference<Right>("right", []),
    keyRef: reference<unknown>("key", []),
    event: reference<Event>("event", []),
    state: reference<State>("state", []),
    parameter: reference<Parameter>("parameter", []),
    literal,
    key: (
      ...parts: readonly (
        | TypedExpression<boolean>
        | TypedExpression<number>
        | TypedExpression<string>
      )[]
    ) => variadic<readonly RowKeyValue[]>("key", parts),
  });

export const joinSelectors = <Left, Right>(): {
  readonly left: Reference<Left>;
  readonly right: Reference<Right>;
} => {
  const roots = selectors<Left, Left, Left, never, Right>();

  return Object.freeze({ left: roots.left, right: roots.right });
};

export const parameterReference = <T>(name: string): TypedExpression<T> =>
  reference<T>("parameter", [name]);

/**
 * A declared row key: one field name, or an ordered tuple of field names.
 *
 * Declarations name key fields instead of building key expressions, so one
 * declaration can drive the plan key expression, a catalog's primary-key
 * metadata, and a sink's public collection metadata at once.
 */
export type DeclaredKey = string | readonly string[];

/** The key fields a row type actually has. */
export type KeyFieldsOf<Row> = (keyof Row & string) | readonly (keyof Row & string)[];

/**
 * Lower a declared key to its canonical row-scoped expression.
 *
 * One field lowers to the field reference. An ordered tuple lowers to the
 * composite `key` operator, preserving the declared field order.
 */
export const keyExpression = (declared: DeclaredKey): Expression => {
  const fields = [declared].flat();

  return Arr.isArray(declared)
    ? variadic<readonly RowKeyValue[]>(
        "key",
        Arr.map(fields, (name) => reference<RowKeyValue>("row", [name])),
      )
    : reference<unknown>("row", fields);
};

export const aggregate = Object.freeze({
  count: (): TypedAggregateExpression<number> =>
    Object.freeze({ kind: "aggregate", function: "count" }),
  countWhere: (expression: BooleanExpression): TypedAggregateExpression<number> =>
    Object.freeze({ kind: "aggregate", function: "count-where", expression }),
  sum: (expression: TypedExpression<number>): TypedAggregateExpression<number> =>
    Object.freeze({ kind: "aggregate", function: "sum", expression }),
  min: <T extends Ordered>(expression: TypedExpression<T>): TypedAggregateExpression<T> =>
    Object.freeze({ kind: "aggregate", function: "min", expression }),
  max: <T extends Ordered>(expression: TypedExpression<T>): TypedAggregateExpression<T> =>
    Object.freeze({ kind: "aggregate", function: "max", expression }),
});
