import { defineRule } from "@oxlint/plugins";

import { tagRead } from "../shared/tag-syntax.ts";

/** Reject `switch (value._tag)` in favour of exhaustive Effect matchers. */
export const noTagSwitchRule = defineRule({
  meta: {
    type: "suggestion",
    docs: {
      description:
        "Disallow switch statements on `_tag`; use Match.valueTags, $match, or a Schema.TaggedUnion match.",
    },
    messages: {
      tagSwitch:
        "Do not `switch` on `_tag`. Use `Match.valueTags(value, { ... })`, `$match` on a `Data.taggedEnum`, or `match` on a `Schema.TaggedUnion`. These check every case at compile time and return a value.",
    },
  },
  createOnce(context) {
    return {
      SwitchStatement(node) {
        if (tagRead(node.discriminant) === undefined) return;
        context.report({ node: node.discriminant, messageId: "tagSwitch" });
      },
    };
  },
});
