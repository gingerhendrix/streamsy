/** JSON-only contracts shared by declaration compilers and view engines. */
export type JsonScalar = null | boolean | number | string;
export type JsonValue = JsonScalar | JsonObject | JsonArray;
export type JsonObject = { readonly [name: string]: JsonValue };
export type JsonArray = readonly JsonValue[];

export type RowKeyPart = boolean | number | string;
export type RowKey = RowKeyPart | readonly RowKeyPart[];

export function isJsonObject(value: JsonValue): value is JsonObject {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- JsonValue has an undiscriminated object arm, and this guard is its parse boundary.
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export type Change<Row, Key extends RowKey = RowKey> =
  | { readonly kind: "enter"; readonly key: Key; readonly after: Row }
  | { readonly kind: "update"; readonly key: Key; readonly before: Row; readonly after: Row }
  | { readonly kind: "exit"; readonly key: Key; readonly before: Row };

/** Source items are ordered as read; the cursor resumes strictly after this batch. */
export interface SourceBatch<Row> {
  readonly sourceId: string;
  readonly partition: string;
  readonly afterExclusiveCursor: string;
  readonly items: readonly Row[];
}

export interface DescriptorRef {
  readonly name: string;
  readonly version: number;
}

export interface LiteralExpression {
  readonly kind: "literal";
  readonly value: JsonValue;
}

export type ReferenceScope = "row" | "left" | "right" | "key" | "event" | "state" | "parameter";

export interface ReferenceExpression {
  readonly kind: "reference";
  readonly scope: ReferenceScope;
  readonly path: readonly string[];
}

export type UnaryOperator = "is-present" | "value" | "not";
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

export type Expression =
  | LiteralExpression
  | ReferenceExpression
  | UnaryExpression
  | BinaryExpression
  | VariadicExpression;

export type AggregateFunction = "count" | "count-where" | "sum" | "max";
export interface AggregateExpression {
  readonly kind: "aggregate";
  readonly function: AggregateFunction;
  readonly expression?: Expression;
}

export interface SortTerm {
  readonly expression: Expression;
  readonly direction: "ascending" | "descending";
}

export interface ParameterDescriptor {
  readonly schema: DescriptorRef;
  readonly maximum?: number;
}

interface RelationNodeBase {
  readonly id: string;
  readonly schema: DescriptorRef;
}

export interface SourceNode extends RelationNodeBase {
  readonly kind: "source";
  readonly sourceId: string;
  readonly partitionBy: Expression;
  readonly key: Expression;
  readonly mode: "facts" | "state";
}

export interface FilterNode extends RelationNodeBase {
  readonly kind: "filter";
  readonly input: string;
  readonly predicate: Expression;
}

export interface ProjectNode extends RelationNodeBase {
  readonly kind: "project";
  readonly input: string;
  readonly fields: Readonly<Record<string, Expression>>;
}

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
  readonly limit: Expression;
  readonly maximum: number;
  readonly partitionBy?: readonly Expression[];
}

export interface ReduceByKeyNode extends RelationNodeBase {
  readonly kind: "reduce-by-key";
  readonly input: string;
  readonly key: Expression;
  readonly reducer: DescriptorRef;
}

export type RelationNode =
  | SourceNode
  | FilterNode
  | ProjectNode
  | KeyNode
  | InnerJoinNode
  | LeftJoinNode
  | GroupedAggregateNode
  | TopNNode
  | ReduceByKeyNode;

export interface RelationPlan {
  readonly version: 3;
  readonly name: string;
  readonly parameters?: Readonly<Record<string, ParameterDescriptor>>;
  readonly nodes: readonly RelationNode[];
  readonly output: string;
}
