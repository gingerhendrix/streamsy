import { Array as Arr, Effect, Record as Rec, Schema, Option } from "effect";
import { collectPlanIssues, PlanCheckFailed, type PlanIssue } from "../check.ts";
import { canonicalJson } from "../runtime/key.ts";
import type { Expression, SortTerm, SqlRelationNode, SqlRelationPlan } from "../ir/contracts.ts";

export interface Arrangement {
  readonly id: string;
  readonly relation: string;
  readonly node: string;
  readonly key: readonly Expression[];
  readonly order: readonly SortTerm[];
  readonly optional: boolean;
}

export interface Program {
  readonly nodes: readonly SqlRelationNode[];
  readonly outputs: Readonly<Record<string, string>>;
  readonly arrangements: readonly Arrangement[];
  readonly stored: ReadonlySet<string>;
  readonly locations: ReadonlyMap<string, { readonly view: string; readonly node: string }>;
  readonly checkedKeys: ReadonlySet<string>;
  readonly stateKeys: ReadonlySet<string>;
}

export function dependencies(node: SqlRelationNode): readonly string[] {
  if (node.kind === "source") return [];

  if (node.kind === "inner-join" || node.kind === "left-join") return [node.left, node.right];

  return [node.input];
}

/** Equality joins may use computed expressions, but each side belongs to one input. */
export function joinKeys(on: Expression): { left: Expression; right: Expression } | undefined {
  if (on.kind !== "binary" || on.operator !== "equal") return undefined;

  if (
    Arr.every(scopes(on.left), (s) => s === "left") &&
    Arr.every(scopes(on.right), (s) => s === "right")
  )
    return { left: rowScope(on.left), right: rowScope(on.right) };

  if (
    Arr.every(scopes(on.right), (s) => s === "left") &&
    Arr.every(scopes(on.left), (s) => s === "right")
  )
    return { left: rowScope(on.right), right: rowScope(on.left) };

  return undefined;
}

function scopes(e: Expression): string[] {
  switch (e.kind) {
    case "reference":
      return e.scope === "parameter" ? [] : [e.scope];
    case "literal":
      return [];
    case "unary":
      return scopes(e.operand);
    case "field":
      return scopes(e.object);
    case "binary":
      return [...scopes(e.left), ...scopes(e.right)];
    case "variadic":
    case "call":
      return Arr.flatMap(e.operands, scopes);
  }
}

function rowScope(e: Expression): Expression {
  switch (e.kind) {
    case "reference":
      return e.scope === "parameter" ? e : { ...e, scope: "row" };
    case "literal":
      return e;
    case "unary":
      return { ...e, operand: rowScope(e.operand) };
    case "field":
      return { ...e, object: rowScope(e.object) };
    case "binary":
      return { ...e, left: rowScope(e.left), right: rowScope(e.right) };
    case "variadic":
    case "call":
      return { ...e, operands: Arr.map(e.operands, rowScope) };
  }
}

export function sqlPlanIssues(plan: SqlRelationPlan): readonly PlanIssue[] {
  const issues = [...collectPlanIssues(plan)];
  const seen = new Set<string>();

  for (const node of plan.nodes) {
    const messages: string[] = [];

    if (node.kind === "reduce-by-key")
      messages.push("SQL tier does not support fact folds (reduce-by-key)");

    if (node.kind === "source" && node.mode !== "state")
      messages.push("SQL tier requires Durable State sources");

    if (
      (node.kind === "inner-join" || node.kind === "left-join") &&
      joinKeys(node.on) === undefined
    )
      messages.push("SQL joins require an equality between left-only and right-only expressions");

    if (
      node.kind === "grouped-aggregate" &&
      Arr.some(Rec.values(node.aggregates), (a) => a.function === "count-where")
    )
      messages.push("SQL tier does not support count-where");

    for (const input of dependencies(node))
      if (!seen.has(input))
        messages.push(`input ${input} must precede its consumer (cycles are unsupported)`);

    for (const message of messages)
      issues.push({
        code: "unsupported-sql-plan",
        view: plan.name,
        nodeId: node.id,
        path: "nodes",
        message,
      });
    seen.add(node.id);
  }

  return issues;
}

export const compile = Effect.fn("ViewsSql.compile")(function* (
  views: Readonly<Record<string, SqlRelationPlan>>,
  stateOutputKeys: boolean,
) {
  const issues = Arr.flatMap(Rec.values(views), sqlPlanIssues);
  const sourceDefinitions = new Map<string, string>();

  for (const plan of Rec.values(views))
    for (const node of plan.nodes) {
      if (node.kind !== "source") continue;
      const { id: _id, ...definition } = node;
      const signature = signatureOf(definition);
      const previous = sourceDefinitions.get(node.sourceId);

      if (previous !== undefined && previous !== signature)
        issues.push({
          code: "unsupported-sql-plan",
          view: plan.name,
          nodeId: node.id,
          path: "sourceId",
          message: `conflicting declarations for source ${node.sourceId}`,
        });
      sourceDefinitions.set(node.sourceId, signature);
    }

  if (issues.length) return yield* Effect.fail(new PlanCheckFailed({ issues }));
  const nodes: SqlRelationNode[] = [];
  const signatures = new Map<string, string>();
  const outputs: Record<string, string> = {};
  const locations = new Map<string, { view: string; node: string }>();
  const stateKeys = new Set<string>();

  for (const [name, plan] of Rec.toEntries(views)) {
    const aliases = new Map<string, string>();

    for (const node of plan.nodes) {
      const remap = (id: string): string => {
        const found = aliases.get(id);

        if (found === undefined) throw new TypeError(`missing relation ${id}`);

        return found;
      };

      const mapped =
        node.kind === "source"
          ? node
          : node.kind === "inner-join" || node.kind === "left-join"
            ? { ...node, left: remap(node.left), right: remap(node.right) }
            : { ...node, input: remap(node.input) };

      const { id: _id, schema: _schema, ...definition } = mapped;
      const stateKey = stateOutputKeys && node.kind === "key" && node.id === plan.output;
      const signature = signatureOf({ definition, stateKey });
      let id = signatures.get(signature);

      if (id === undefined) {
        id = `r${nodes.length}`;
        signatures.set(signature, id);
        nodes.push({ ...mapped, id });
        locations.set(id, { view: name, node: node.id });

        if (stateKey) stateKeys.add(id);
      }

      aliases.set(node.id, id);
    }

    outputs[name] = aliases.get(plan.output) ?? "";
  }

  const arrangements: Arrangement[] = [];
  const stored = new Set<string>();
  const checkedKeys = new Set<string>();
  // Only record expressions that reproduce a relation's existing key from its row.
  const keyFields = new Map<string, { expression: Expression; stateKey: boolean }>();

  const arrange = (
    node: string,
    relation: string,
    key: readonly Expression[],
    order: readonly SortTerm[],
    optional = false,
  ) => {
    stored.add(relation);
    const signature = signatureOf({ relation, key, order });

    for (const [index, a] of arrangements.entries()) {
      if (signatureOf({ relation: a.relation, key: a.key, order: a.order }) !== signature) continue;
      // A required consumer (top) makes a shared index required; extrema alone skip absent operands.
      arrangements[index] = { ...a, optional: a.optional && optional };

      return;
    }

    arrangements.push({ id: `a${arrangements.length}`, node, relation, key, order, optional });
  };

  for (const node of nodes) {
    if (node.kind === "source") {
      stored.add(node.id);

      if (Arr.every(scopes(node.key), (scope) => scope === "row"))
        keyFields.set(node.id, { expression: node.key, stateKey: node.stateKey === true });
    }

    if (node.kind === "filter" || node.kind === "top-n") {
      const key = keyFields.get(node.input);

      if (key) keyFields.set(node.id, key);
    }

    if (node.kind === "project") {
      const key = keyFields.get(node.input);

      if (key)
        for (const [field, expression] of Rec.toEntries(node.fields))
          if (signatureOf(expression) === signatureOf(key.expression)) {
            keyFields.set(node.id, {
              expression: { kind: "reference", scope: "row", path: [field] },
              stateKey: key.stateKey,
            });
            break;
          }
    }

    if (node.kind === "key") {
      const inputKey = keyFields.get(node.input);
      const stateKey = stateKeys.has(node.id);

      const sameKey =
        inputKey?.stateKey === stateKey &&
        signatureOf(inputKey.expression) === signatureOf(node.key);

      const identity =
        !stateKey &&
        node.key.kind === "reference" &&
        node.key.scope === "key" &&
        node.key.path.length === 0;

      if (!sameKey && !identity) checkedKeys.add(node.id);

      if (Arr.every(scopes(node.key), (scope) => scope === "row"))
        keyFields.set(node.id, { expression: node.key, stateKey });
    }

    if (node.kind === "inner-join" || node.kind === "left-join") {
      const keys = joinKeys(node.on);

      if (!keys) throw new TypeError("checked join has no equality");
      arrange(node.id, node.left, [keys.left], []);
      arrange(node.id, node.right, [keys.right], []);
    }

    if (node.kind === "top-n") arrange(node.id, node.input, node.partitionBy ?? [], node.orderBy);

    if (node.kind === "grouped-aggregate") {
      if (Arr.every(Rec.keys(node.groupBy), (field) => !Rec.has(node.aggregates, field)))
        keyFields.set(node.id, {
          expression: {
            kind: "variadic",
            operator: "key",
            operands: Arr.map(Rec.keys(node.groupBy), (field) => ({
              kind: "reference",
              scope: "row",
              path: [field],
            })),
          },
          stateKey: false,
        });

      if (Arr.some(Rec.values(node.aggregates), (a) => a.function === "sum"))
        arrange(node.id, node.input, Rec.values(node.groupBy), []);

      for (const a of Rec.values(node.aggregates)) {
        if ((a.function === "min" || a.function === "max") && a.expression)
          arrange(
            node.id,
            node.input,
            Rec.values(node.groupBy),
            [{ expression: a.expression, direction: "ascending" }],
            true,
          );
      }
    }
  }

  return {
    nodes,
    outputs,
    arrangements,
    stored,
    locations,
    checkedKeys,
    stateKeys,
  } satisfies Program;
});

export function findArrangement(
  program: Program,
  relation: string,
  key: readonly Expression[],
  order: readonly SortTerm[],
): Arrangement {
  const signature = signatureOf({ relation, key, order });

  const found = Option.getOrUndefined(
    Arr.findFirst(
      program.arrangements,
      (a) => signatureOf({ relation: a.relation, key: a.key, order: a.order }) === signature,
    ),
  );

  if (!found) throw new TypeError("missing planned arrangement");

  return found;
}

/** Validate the JSON-only IR before canonicalizing interface-shaped values. */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- This is the JSON validation boundary for interface-shaped plan data.
export function signatureOf(value: unknown): string {
  return canonicalJson(Schema.decodeUnknownSync(Schema.Json)(value));
}
