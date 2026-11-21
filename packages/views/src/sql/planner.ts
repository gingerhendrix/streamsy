import { Array as Arr, Effect, Record as Rec } from "effect";
import { collectPlanIssues, PlanCheckFailed } from "../check.ts";
import { canonicalJson } from "../runtime/key.ts";
import type { Expression, SortTerm, RelationNode, RelationPlan } from "../ir/contracts.ts";

export interface Arrangement {
  readonly id: string;
  readonly relation: string;
  readonly node: string;
  readonly key: readonly Expression[];
  readonly order: readonly SortTerm[];
  readonly optional: boolean;
}

export interface Program {
  readonly nodes: readonly RelationNode[];
  readonly outputs: Readonly<Record<string, string>>;
  readonly pushedFilters: ReadonlyMap<string, readonly Extract<RelationNode, { kind: "filter" }>[]>;
  readonly arrangements: ReadonlyMap<string, Arrangement>;
  readonly stored: ReadonlySet<string>;
  readonly locations: ReadonlyMap<string, { readonly view: string; readonly node: string }>;
  readonly checkedKeys: ReadonlySet<string>;
  readonly stateKeys: ReadonlySet<string>;
}

export function dependencies(node: RelationNode): readonly string[] {
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

export const compile = Effect.fn("ViewsSql.compile")(function* (
  views: Readonly<Record<string, RelationPlan>>,
  stateOutputKeys: boolean,
) {
  const issues = Arr.flatMap(Rec.values(views), collectPlanIssues);
  const sourceDefinitions = new Map<string, string>();

  for (const plan of Rec.values(views))
    for (const node of plan.nodes) {
      if (
        (node.kind === "inner-join" || node.kind === "left-join") &&
        joinKeys(node.on) === undefined
      )
        issues.push({
          code: "unsupported-sql-plan",
          view: plan.name,
          nodeId: node.id,
          path: "nodes",
          message: "SQL joins require an equality between left-only and right-only expressions",
        });

      if (node.kind !== "source") continue;
      const { id: _id, ...definition } = node;
      const signature = canonicalJson(definition);
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
  const nodes: RelationNode[] = [];
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

      const { id: _id, ...definition } = mapped;
      const stateKey = stateOutputKeys && node.kind === "key" && node.id === plan.output;
      const signature = canonicalJson({ definition, stateKey });
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

  const arrangements = new Map<string, Arrangement>();
  const stored = new Set<string>();
  const checkedKeys = new Set<string>();
  const pushedFilters = new Map<string, Extract<RelationNode, { kind: "filter" }>[]>();
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
    const signature = canonicalJson({ relation, key, order });

    const existing = arrangements.get(signature);

    // A required consumer (top) makes a shared index required. Ordered aggregates skip absent operands.
    arrangements.set(
      signature,
      existing === undefined
        ? { id: `a${arrangements.size}`, node, relation, key, order, optional }
        : { ...existing, optional: existing.optional && optional },
    );
  };

  for (const node of nodes) {
    if (node.kind === "source") {
      stored.add(node.id);
      pushedFilters.set(node.id, []);

      if (Arr.every(scopes(node.key), (scope) => scope === "row"))
        keyFields.set(node.id, { expression: node.key, stateKey: node.stateKey === true });
    }

    // If anything can observe the unfiltered source, keep the full integral.
    // Otherwise the source only needs the union of its direct filters.
    for (const input of dependencies(node)) {
      const filters = pushedFilters.get(input);

      if (filters === undefined) continue;

      if (node.kind === "filter") filters.push(node);
      else pushedFilters.delete(input);
    }

    if (node.kind === "filter" || node.kind === "top-n") {
      const key = keyFields.get(node.input);

      if (key) keyFields.set(node.id, key);
    }

    if (node.kind === "project" && node.fields !== undefined) {
      const key = keyFields.get(node.input);

      if (key)
        for (const [field, expression] of Rec.toEntries(node.fields))
          if (canonicalJson(expression) === canonicalJson(key.expression)) {
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
        canonicalJson(inputKey.expression) === canonicalJson(node.key);

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
        if ((a.function === "min" || a.function === "max" || a.function === "list") && a.expression)
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

  for (const output of Rec.values(outputs)) pushedFilters.delete(output);

  return {
    nodes,
    outputs,
    pushedFilters,
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
  const signature = canonicalJson({ relation, key, order });

  const found = program.arrangements.get(signature);

  if (!found) throw new TypeError("missing planned arrangement");

  return found;
}
