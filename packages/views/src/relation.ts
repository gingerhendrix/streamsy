import { Record as Rec } from "effect";
import type {
  ClosureNode,
  ConcatNode,
  FilterNode,
  GroupedAggregateNode,
  InnerJoinNode,
  KeyNode,
  LeftJoinNode,
  ProjectNode,
  RelationNode,
  RelationPlan,
  SourceNode,
  TopNNode,
} from "./ir/contracts.ts";

type UnaryNode = FilterNode | ProjectNode | KeyNode | GroupedAggregateNode | TopNNode | ClosureNode;

type UnaryRelation<Node extends UnaryNode = UnaryNode> = Node extends UnaryNode
  ? Omit<Node, "id" | "input"> & { readonly input: RelationExpression }
  : never;

export type RelationExpression<Row = unknown> = (
  | Omit<SourceNode, "id">
  | UnaryRelation
  | ((
      | Omit<InnerJoinNode | LeftJoinNode, "id" | "left" | "right">
      | Omit<ConcatNode, "id" | "left" | "right">
    ) & {
      readonly left: RelationExpression;
      readonly right: RelationExpression;
    })
) & { readonly _row?: Row };

export function compilePlan(
  name: string,
  expression: RelationExpression,
  parameters: readonly string[] = [],
): RelationPlan {
  const nodes: RelationNode[] = [];
  const sources = new Set<string>();
  const shared = new Map<RelationExpression, string>();
  let index = 0;

  const walk = (relation: RelationExpression, reuse = false): string => {
    if (relation.kind === "source") {
      const id = relation.sourceId;

      if (!sources.has(id)) {
        sources.add(id);
        nodes.push({ ...relation, id });
      }

      return id;
    }

    if (reuse) {
      const previous = shared.get(relation);

      if (previous !== undefined) return previous;
    }

    const id = addNode(relation, reuse);

    if (reuse) shared.set(relation, id);

    return id;
  };

  const addNode = (relation: Exclude<RelationExpression, { kind: "source" }>, reuse: boolean) => {
    if ("left" in relation) {
      // Concat branches can reuse the same joined input. Preserve existing plan
      // identities by sharing nodes only inside those branches.
      const shareBranches = reuse || relation.kind === "concat";
      const left = walk(relation.left, shareBranches);
      const right = walk(relation.right, shareBranches);
      const id = `${name}/${relation.kind}/${index++}`;
      nodes.push({ ...relation, id, left, right });

      return id;
    }

    const input = walk(relation.input, reuse);
    const kind = relation.kind === "top-n" ? "top" : relation.kind;
    const id = `${name}/${kind}/${index++}`;
    nodes.push({ ...relation, id, input });

    return id;
  };

  const output = walk(expression);

  return deepFreeze({ version: 3, name, nodes, output, parameters });
}

export function deepFreeze<T>(value: T): T {
  // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Freeze only objects; JSON primitives need no work.
  if (value === null || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.freeze(value);

  for (const child of Rec.values(value)) deepFreeze(child);

  return value;
}
