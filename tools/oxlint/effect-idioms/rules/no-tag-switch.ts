import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";

import { enclosingErrorHandlerKind } from "../shared/tag-context.ts";
import { isReasonTagRead, tagRead } from "../shared/tag-syntax.ts";

type MessageId = "tagSwitch" | "errorHandlerSwitch" | "reasonHandlerSwitch";

function switchMessage(node: ESTree.SwitchStatement, read: ESTree.MemberExpression): MessageId {
  if (enclosingErrorHandlerKind(node) !== "recovery") return "tagSwitch";
  return isReasonTagRead(read) ? "reasonHandlerSwitch" : "errorHandlerSwitch";
}

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
      errorHandlerSwitch:
        "Do not `switch` on `_tag` inside a catch handler. Recover with `Effect.catchTags({ ... })`, so each handled error leaves the error channel and the rest stay typed.",
      reasonHandlerSwitch:
        'Do not `switch` on `reason._tag` inside a catch handler. Recover with `Effect.catchReasons("<ErrorTag>", { ... })`.',
    },
  },
  createOnce(context) {
    return {
      SwitchStatement(node) {
        const read = tagRead(node.discriminant);
        if (read === undefined) return;
        context.report({ node: node.discriminant, messageId: switchMessage(node, read) });
      },
    };
  },
});
