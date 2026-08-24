/**
 * Serializable contracts for incrementally maintained views.
 *
 * Adapted from the `contracts-spike-minimal` `@streamsy/views-ir` draft
 * (commit `11742f6`), narrowed to exactly the vocabulary this slice executes.
 * The spike's filter, project, key, left-join, grouped-aggregate and top-N
 * nodes are deliberately absent: an unexecutable node in the IR would be a
 * contract nobody can honour, and the first slice declares none of them.
 *
 * Everything here is inert data. Nothing in this module performs I/O, reads a
 * clock, or depends on Effect — that is what makes a plan hashable and a
 * reducer replayable.
 */

export type JsonScalar = null | boolean | number | string;
export type JsonValue = JsonScalar | JsonObject | JsonArray;
export type JsonObject = { readonly [name: string]: JsonValue };
export type JsonArray = readonly JsonValue[];

/**
 * The slice keys every maintained row by one string (`issueId`).
 *
 * The spike allowed composite keys. Narrowing to a string keeps the SQLite
 * primary key, the Durable State message `key`, and the TanStack DB collection
 * key the same value with no encoding step in between. Composite keys return
 * with the first view that needs one.
 */
export type RowKey = string;

/**
 * Narrow a JSON value to an object.
 *
 * `Array.isArray` alone does not narrow a readonly array out of `JsonValue`, so
 * the object case needs both checks.
 */
export function isJsonObject(value: JsonValue): value is JsonObject {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- This IS the parse boundary the rule asks for: `JsonValue` is a closed union whose object arm has no discriminator, so a shape check is the only way to establish it.
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export type Change<Row> =
  | { readonly kind: "enter"; readonly key: RowKey; readonly after: Row }
  | {
      readonly kind: "update";
      readonly key: RowKey;
      readonly before: Row;
      readonly after: Row;
    }
  | { readonly kind: "exit"; readonly key: RowKey; readonly before: Row };

/**
 * Source items as read, in source order. `afterExclusiveCursor` resumes
 * strictly after the final item represented by this batch — it is the value
 * that gets committed as the view's source checkpoint.
 */
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

/** The scopes the slice's expressions can read. */
export type ReferenceScope = "row" | "event" | "state";

export interface ReferenceExpression {
  readonly kind: "reference";
  readonly scope: ReferenceScope;
  readonly path: readonly string[];
}

export type Expression = LiteralExpression | ReferenceExpression;

interface RelationNodeBase {
  readonly id: string;
  readonly schema: DescriptorRef;
}

export interface SourceNode extends RelationNodeBase {
  readonly kind: "source";
  readonly sourceId: string;
  readonly key: Expression;
  readonly order: Expression;
  readonly partitionBy: Expression;
}

export interface ReduceByKeyNode extends RelationNodeBase {
  readonly kind: "reduce-by-key";
  readonly input: string;
  readonly key: Expression;
  readonly order: Expression;
  readonly reducer: DescriptorRef;
}

export type RelationNode = SourceNode | ReduceByKeyNode;

export interface RelationPlan {
  readonly version: 1;
  readonly name: string;
  readonly nodes: readonly RelationNode[];
  readonly output: string;
}
