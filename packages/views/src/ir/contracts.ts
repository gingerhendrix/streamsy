import { Array as Arr } from "effect";

/** JSON-only contracts shared by declaration compilers and view engines. */
export type JsonScalar = null | boolean | number | string;

export type JsonValue = JsonScalar | JsonObject | JsonArray;

export type JsonObject = { readonly [name: string]: JsonValue };

export type JsonArray = readonly JsonValue[];

export type RowKeyPart = boolean | number | string;

export type RowKey = RowKeyPart | readonly RowKeyPart[];

export function isJsonObject(value: JsonValue): value is JsonObject {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- JsonValue has an undiscriminated object arm, and this guard is its parse boundary.
  return value !== null && typeof value === "object" && !Arr.isArray(value);
}

export interface LiteralExpression {
  readonly kind: "literal";
  readonly value: JsonValue;
}

export type ReferenceScope = "row" | "left" | "right" | "key" | "parameter";

export interface ReferenceExpression {
  readonly kind: "reference";
  readonly scope: ReferenceScope;
  readonly path: readonly string[];
}

export type UnaryOperator = "is-present" | "unwrap" | "not";

export interface UnaryExpression {
  readonly kind: "unary";
  readonly operator: UnaryOperator;
  readonly operand: Expression;
}

export type BinaryOperator =
  | "equal"
  | "not-equal"
  | "greater-than"
  | "greater-than-or-equal"
  | "less-than"
  | "less-than-or-equal"
  | "add"
  | "or-else";

export interface BinaryExpression {
  readonly kind: "binary";
  readonly operator: BinaryOperator;
  readonly left: Expression;
  readonly right: Expression;
}

export type VariadicOperator = "and" | "or" | "in" | "key";

export interface VariadicExpression {
  readonly kind: "variadic";
  readonly operator: VariadicOperator;
  readonly operands: readonly Expression[];
}

export interface FieldExpression {
  readonly kind: "field";
  readonly object: Expression;
  readonly field: string;
}

export const Invoke = Symbol("@streamsy/views/invoke");

export interface CallExpression {
  readonly kind: "call";
  readonly name: string;
  readonly version: number;
  readonly operands: readonly Expression[];
  /** The implementation travels with the expression and is omitted from canonical JSON. */
  readonly [Invoke]: (args: readonly (JsonValue | undefined)[]) => JsonValue | undefined;
}

export type Expression =
  | LiteralExpression
  | ReferenceExpression
  | UnaryExpression
  | BinaryExpression
  | VariadicExpression
  | FieldExpression
  | CallExpression;

export type AggregateFunction = "count" | "sum" | "min" | "max" | "list";

export interface AggregateExpression {
  readonly kind: "aggregate";
  readonly function: AggregateFunction;
  readonly expression?: Expression;
}

export interface SortTerm {
  readonly expression: Expression;
  readonly direction: "ascending" | "descending";
}

interface RelationNodeBase {
  readonly id: string;
}

export interface SourceNode extends RelationNodeBase {
  /** Durable State addresses keys as strings, including numeric key fields. */
  readonly stateKey?: true;
  readonly kind: "source";
  readonly sourceId: string;
  readonly key: Expression;
}

export interface FilterNode extends RelationNodeBase {
  readonly kind: "filter";
  readonly input: string;
  readonly predicate: Expression;
}

interface ProjectNodeBase extends RelationNodeBase {
  readonly kind: "project";
  readonly input: string;
}

export type ProjectNode = ProjectNodeBase &
  (
    | { readonly fields: Readonly<Record<string, Expression>>; readonly row?: never }
    | { readonly row: Expression; readonly fields?: never }
  );

export interface KeyNode extends RelationNodeBase {
  readonly kind: "key";
  readonly input: string;
  readonly key: Expression;
}

export interface InnerJoinNode extends RelationNodeBase {
  readonly kind: "inner-join";
  readonly left: string;
  readonly right: string;
  readonly on: Expression;
  readonly rightAlias: string;
}

export interface LeftJoinNode extends RelationNodeBase {
  readonly kind: "left-join";
  readonly left: string;
  readonly right: string;
  readonly on: Expression;
  readonly rightAlias: string;
}

export interface ConcatNode extends RelationNodeBase {
  readonly kind: "concat";
  readonly left: string;
  readonly right: string;
}

export interface GroupedAggregateNode extends RelationNodeBase {
  readonly kind: "grouped-aggregate";
  readonly input: string;
  readonly groupBy: Readonly<Record<string, Expression>>;
  readonly aggregates: Readonly<Record<string, AggregateExpression>>;
}

export interface TopNNode extends RelationNodeBase {
  readonly kind: "top-n";
  readonly input: string;
  readonly orderBy: readonly SortTerm[];
  readonly limit: number;
  readonly partitionBy?: readonly Expression[];
}

export type RelationNode =
  | SourceNode
  | FilterNode
  | ProjectNode
  | KeyNode
  | InnerJoinNode
  | LeftJoinNode
  | ConcatNode
  | GroupedAggregateNode
  | TopNNode;

export interface RelationPlan {
  readonly version: 3;
  readonly name: string;
  readonly parameters: readonly string[];
  readonly nodes: readonly RelationNode[];
  readonly output: string;
}
