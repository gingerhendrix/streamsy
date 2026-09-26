import { defineRule } from "@oxlint/plugins";

import { isTagKey } from "../shared/tag-syntax.ts";

/**
 * Reject `const { _tag: tag } = value`. The pattern moves a tag check onto a plain local,
 * where `no-tag-comparison` and `no-underscore-dangle` no longer see it.
 */
export const noTagDestructureRule = defineRule({
  meta: {
    type: "suggestion",
    docs: {
      description:
        "Disallow destructuring `_tag`; narrow the tagged value itself with an Effect tag utility.",
    },
    messages: {
      tagDestructure:
        "Do not destructure `_tag` into a local. Narrow the value itself with `Predicate.isTagged`, `Match.valueTags`, or the guards of the union that owns it.",
    },
  },
  createOnce(context) {
    return {
      ObjectPattern(node) {
        for (const property of node.properties) {
          if (property.type !== "Property") continue;
          if (!isTagKey(property.key, property.computed)) continue;
          context.report({ node: property, messageId: "tagDestructure" });
        }
      },
    };
  },
});
