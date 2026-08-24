/**
 * The plan interpreter.
 *
 * `maintain` is a pure function of (plan, reducer, prior state, source batch).
 * It performs no I/O, so the same call produces the same changes on the memory
 * host, the SQLite host, and inside a test. The caller loads prior state,
 * commits the result, and publishes the changes; the engine only decides what
 * changed.
 *
 * The slice maintains one keyed relation, so the interpreter walks a two-node
 * plan. It is written against {@link RelationPlan} rather than the declaration
 * so a later host can execute a plan it received rather than one it built.
 */
import type {
  Change,
  Expression,
  JsonObject,
  JsonValue,
  RelationPlan,
  RowKey,
} from "./contracts.ts";
import type { ReducerDeclaration } from "./dsl.ts";
import { evaluate, evaluateKey, evaluateOrder } from "./expression.ts";

/** A source item that the reducer cannot fold. Always a declaration or data bug. */
export class ReducerFault extends TypeError {
  constructor(
    readonly phase: "key" | "order" | "branch" | "evolve" | "decode",
    readonly sourceKey: string,
    cause: unknown,
  ) {
    super(`${phase} failed for source item ${sourceKey}: ${describe(cause)}`);
    this.name = "ReducerFault";
    this.cause = cause;
  }
}

export interface MaintainInput<Row> {
  readonly plan: RelationPlan;
  readonly reducer: ReducerDeclaration;
  /**
   * Decode a folded value into the maintained row type. Throws on a bad value.
   *
   * The parameter is `unknown` because this IS the parse boundary: the engine
   * merges an expression patch over prior state and hands the result to the
   * caller's schema, which is the only thing that can say whether it is a row.
   */
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Documented above: the callback is the schema boundary itself.
  readonly decodeRow: (value: unknown) => Row;
  /** Prior reducer state for every key this batch can touch. */
  readonly current: ReadonlyMap<RowKey, Row>;
  /** Decoded source items, in arrival order. */
  readonly items: readonly JsonObject[];
}

export interface MaintainResult<Row> {
  /** One coalesced change per touched key: `before` is the batch's entry state. */
  readonly changes: readonly Change<Row>[];
  /** Final reducer state for every touched key. */
  readonly rows: ReadonlyMap<RowKey, Row>;
}

/** Fold one batch of source items into keyed row changes. */
export function maintain<Row>(input: MaintainInput<Row>): MaintainResult<Row> {
  const node = input.plan.nodes.find((candidate) => candidate.id === input.plan.output);
  if (node === undefined || node.kind !== "reduce-by-key") {
    throw new TypeError(`plan ${input.plan.name} has no reduce-by-key output`);
  }

  const ordered = sortBySourceOrder(node.order, input.items);
  const before = new Map<RowKey, Row | undefined>();
  const after = new Map<RowKey, Row>();

  for (const item of ordered) {
    const key = keyOf(node.key, item);
    if (!before.has(key)) before.set(key, input.current.get(key));
    const previous = after.get(key) ?? input.current.get(key);
    after.set(key, fold(input, key, previous, item));
  }

  const changes: Change<Row>[] = [];
  for (const [key, next] of after) {
    const entry = before.get(key);
    if (entry === undefined) {
      changes.push({ kind: "enter", key, after: next });
    } else if (!sameRow(entry, next)) {
      changes.push({ kind: "update", key, before: entry, after: next });
    }
  }
  return { changes, rows: after };
}

/** Every key one batch of source items can touch, so the caller can load exactly those rows. */
export function touchedKeys(plan: RelationPlan, items: readonly JsonObject[]): readonly RowKey[] {
  const node = plan.nodes.find((candidate) => candidate.id === plan.output);
  if (node === undefined || node.kind !== "reduce-by-key") return [];
  const keys = new Set<RowKey>();
  for (const item of items) keys.add(keyOf(node.key, item));
  return [...keys];
}

function fold<Row>(
  input: MaintainInput<Row>,
  key: RowKey,
  previous: Row | undefined,
  item: JsonObject,
): Row {
  const discriminator = item[input.reducer.discriminator];
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- The discriminator field is named by the declaration and read out of decoded JSON; checking that it is a string is how the branch lookup is established.
  if (typeof discriminator !== "string") {
    throw new ReducerFault("branch", key, `${input.reducer.discriminator} is not a string`);
  }
  const branch = input.reducer.evolve[discriminator];
  if (branch === undefined) {
    throw new ReducerFault("branch", key, `no evolve branch for ${discriminator}`);
  }

  const patch: Record<string, JsonValue> = {};
  // SAFETY: `previous` came from `decodeRow`, so it is a value the caller's
  // schema accepted — a JSON object by construction. The evaluator reads it
  // through the `state` scope and never mutates it.
  const scopes = { event: item, state: previous as JsonValue | undefined };
  for (const [field, expression] of Object.entries(branch)) {
    try {
      patch[field] = evaluate(expression, scopes);
    } catch (cause) {
      throw new ReducerFault("evolve", key, cause);
    }
  }

  // SAFETY: same invariant — `previous` is a decoded row, so spreading it
  // yields its own fields and the patch overwrites exactly the declared ones.
  const merged = previous === undefined ? patch : { ...(previous as object), ...patch };
  try {
    return input.decodeRow(merged);
  } catch (cause) {
    throw new ReducerFault("decode", key, cause);
  }
}

/**
 * Sort by the declared source order.
 *
 * The sort is stable and `items` arrive in durable stream order, so two facts
 * that declare the same `sequence` keep the order the log already fixed. That
 * makes a replay of the same suffix converge on the same rows without the
 * declaration having to invent a second tie-breaker.
 */
function sortBySourceOrder(order: Expression, items: readonly JsonObject[]): readonly JsonObject[] {
  return [...items].sort((left, right) => orderOf(order, left) - orderOf(order, right));
}

function keyOf(expression: Expression, item: JsonObject): RowKey {
  try {
    return evaluateKey(expression, { row: item, event: item });
  } catch (cause) {
    throw new ReducerFault("key", JSON.stringify(item).slice(0, 120), cause);
  }
}

function orderOf(expression: Expression, item: JsonObject): number {
  try {
    return evaluateOrder(expression, { row: item, event: item });
  } catch (cause) {
    throw new ReducerFault("order", JSON.stringify(item).slice(0, 120), cause);
  }
}

function sameRow<Row>(left: Row, right: Row): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function describe(cause: unknown): string {
  return cause instanceof Error ? cause.message : String(cause);
}
