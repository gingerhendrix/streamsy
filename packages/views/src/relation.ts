import { Record as Rec } from "effect";
import type {
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

type UnaryNode = FilterNode | ProjectNode | KeyNode | GroupedAggregateNode | TopNNode;

type UnaryRelation<Node extends UnaryNode = UnaryNode> = Node extends UnaryNode
  ? Omit<Node, "id" | "input"> & { readonly input: RelationExpression }
  : never;

export type RelationExpression<Row = unknown> = (
  | Omit<SourceNode, "id">
  | UnaryRelation
  | (Omit<InnerJoinNode | LeftJoinNode, "id" | "left" | "right"> & {
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
  let index = 0;

  const walk = (relation: RelationExpression): string => {
    if (relation.kind === "source") {
      const id = relation.sourceId;

      if (!sources.has(id)) {
        sources.add(id);
        nodes.push({ ...relation, id });
      }

      return id;
    }

    if ("left" in relation) {
      const left = walk(relation.left);
      const right = walk(relation.right);
      const id = `${name}/${relation.kind}/${index++}`;
      nodes.push({ ...relation, id, left, right });

      return id;
    }

    const input = walk(relation.input);
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
