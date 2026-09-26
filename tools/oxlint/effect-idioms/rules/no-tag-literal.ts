import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";

import { calleeName } from "../shared/tag-context.ts";
import { isTagKey, literalTagNames } from "../shared/tag-syntax.ts";

/** Test matchers whose expected value names the encoded `_tag` of the wire shape. */
const ASSERTION_CALLEES = new Set([
  "expect",
  "toEqual",
  "toStrictEqual",
  "toMatchObject",
  "toContainEqual",
  "toHaveBeenCalledWith",
]);

/** Reports whether an object literal is, or is nested in, the argument of a test assertion. */
function isAssertionValue(node: ESTree.ObjectExpression): boolean {
  let current: ESTree.Node = node;
  while (
    current.parent.type === "Property" ||
    current.parent.type === "ObjectExpression" ||
    current.parent.type === "ArrayExpression"
  ) {
    current = current.parent;
  }
  const parent = current.parent;
  if (parent.type !== "CallExpression") return false;
  const name = calleeName(parent);
  return name !== undefined && ASSERTION_CALLEES.has(name);
}

/** Reject hand-built `{ _tag: "Tag" }` values and `_tag = "Tag"` class fields. */
export const noTagLiteralRule = defineRule({
  meta: {
    type: "suggestion",
    docs: {
      description:
        "Disallow hand-built tagged values; construct them with Data.taggedEnum, Data.TaggedError, Schema.TaggedStruct, or Schema.TaggedError.",
    },
    messages: {
      tagLiteral:
        'Do not build a tagged value by hand with `_tag: "{{tag}}"`. Use a constructor from `Data.taggedEnum`, `Data.TaggedError`, `Schema.TaggedStruct`, or `Schema.TaggedError`, so one place owns the tag and its fields.',
      tagField:
        'Do not declare `_tag = "{{tag}}"` on a class by hand. Extend `Data.TaggedClass`, `Data.TaggedError`, `Schema.TaggedClass`, or `Schema.TaggedError`.',
    },
  },
  createOnce(context) {
    return {
      ObjectExpression(node) {
        for (const property of node.properties) {
          if (property.type !== "Property" || property.shorthand || property.method) continue;
          if (!isTagKey(property.key, property.computed)) continue;
          const tags = literalTagNames(property.value);
          if (tags === undefined || isAssertionValue(node)) continue;
          context.report({
            node: property,
            messageId: "tagLiteral",
            data: { tag: tags.join(" | ") },
          });
        }
      },
      PropertyDefinition(node) {
        if (node.value === null || !isTagKey(node.key, node.computed)) return;
        const tags = literalTagNames(node.value);
        if (tags === undefined) return;
        context.report({ node, messageId: "tagField", data: { tag: tags.join(" | ") } });
      },
    };
  },
});
