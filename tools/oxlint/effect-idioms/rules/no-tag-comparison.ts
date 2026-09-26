import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";

import {
  builtInGuard,
  enclosingFunction,
  isErrorHandler,
  isRetryPredicate,
  isTagPredicateLambda,
} from "../shared/tag-context.ts";
import { TAG_FIELD, stringLiteralValue, tagRead } from "../shared/tag-syntax.ts";

const EQUALITY = new Set(["===", "!==", "==", "!="]);

type MessageId = "builtInGuard" | "errorHandler" | "tagPredicate" | "tagComparison";

interface TagComparison {
  readonly tag: string;
}

function comparedTag(node: ESTree.BinaryExpression): TagComparison | undefined {
  if (!EQUALITY.has(node.operator)) return undefined;
  const leftTag = stringLiteralValue(node.left);
  const rightTag = stringLiteralValue(node.right);
  if (tagRead(node.left) !== undefined && rightTag !== undefined) return { tag: rightTag };
  if (tagRead(node.right) !== undefined && leftTag !== undefined) return { tag: leftTag };
  return undefined;
}

function isTagInCheck(node: ESTree.BinaryExpression): boolean {
  if (node.operator !== "in") return false;
  return stringLiteralValue(node.left) === TAG_FIELD;
}

function messageFor(node: ESTree.BinaryExpression, tag: string): MessageId {
  if (builtInGuard(tag) !== undefined) return "builtInGuard";
  const fn = enclosingFunction(node);
  if (fn === undefined) return "tagComparison";
  if (isRetryPredicate(fn)) return "tagPredicate";
  if (isErrorHandler(fn)) return "errorHandler";
  if (isTagPredicateLambda(fn, node)) return "tagPredicate";
  return "tagComparison";
}

/** Reject `_tag` string comparisons and `"_tag" in` checks in favour of Effect tag utilities. */
export const noTagComparisonRule = defineRule({
  meta: {
    type: "suggestion",
    docs: {
      description:
        "Disallow comparing `_tag` with a string literal; use Effect tag utilities such as Predicate.isTagged, Match.valueTags, or catchTag.",
    },
    messages: {
      builtInGuard:
        'Use `{{guard}}` in place of comparing `_tag` with "{{tag}}". Effect exports a named guard for this built-in tag.',
      errorHandler:
        'Do not branch on `_tag` inside an error handler. Recover with `Effect.catchTag("{{tag}}", ...)` or `Effect.catchTags({ ... })` so the handled error leaves the error channel.',
      tagPredicate:
        'Use `Predicate.isTagged("{{tag}}")` in place of this `_tag` comparison. It is a real type guard.',
      tagComparison:
        'Do not compare `_tag` with "{{tag}}" by hand. Use `Predicate.isTagged`, `Match.valueTags`, `$is` on a `Data.taggedEnum`, or the `guards` of a `Schema.TaggedUnion`.',
      tagInCheck:
        '`"_tag" in value` guesses at structure. Use `Predicate.isTagged`, or a guard from the schema or enum that owns the union.',
    },
  },
  createOnce(context) {
    return {
      BinaryExpression(node) {
        if (isTagInCheck(node)) {
          context.report({ node, messageId: "tagInCheck" });
          return;
        }
        const comparison = comparedTag(node);
        if (comparison === undefined) return;
        const { tag } = comparison;
        context.report({
          node,
          messageId: messageFor(node, tag),
          data: { tag, guard: builtInGuard(tag) ?? "" },
        });
      },
    };
  },
});
