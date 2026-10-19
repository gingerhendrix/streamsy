import { defineRule } from "@oxlint/plugins";

import type { ESTree } from "@oxlint/plugins";

import {
  builtInGuard,
  enclosingFunction,
  errorHandlerKind,
  isRetryPredicate,
  isTagPredicateLambda,
} from "../shared/tag-context.ts";
import { TAG_FIELD, isReasonTagRead, stringLiteralValue, tagRead } from "../shared/tag-syntax.ts";

const EQUALITY = new Set(["===", "!==", "==", "!="]);

type MessageId =
  | "builtInGuard"
  | "errorHandler"
  | "reasonHandler"
  | "mappingHandler"
  | "tagPredicate"
  | "tagComparison";

interface TagComparison {
  readonly tag: string;
  readonly read: ESTree.MemberExpression;
}

function comparedTag(node: ESTree.BinaryExpression): TagComparison | undefined {
  if (!EQUALITY.has(node.operator)) return undefined;
  const leftRead = tagRead(node.left);
  const rightRead = tagRead(node.right);
  const leftTag = stringLiteralValue(node.left);
  const rightTag = stringLiteralValue(node.right);
  if (leftRead !== undefined && rightTag !== undefined) return { tag: rightTag, read: leftRead };
  if (rightRead !== undefined && leftTag !== undefined) return { tag: leftTag, read: rightRead };
  return undefined;
}

function isTagInCheck(node: ESTree.BinaryExpression): boolean {
  if (node.operator !== "in") return false;
  return stringLiteralValue(node.left) === TAG_FIELD;
}

function messageFor(node: ESTree.BinaryExpression, comparison: TagComparison): MessageId {
  if (builtInGuard(comparison.tag) !== undefined) return "builtInGuard";
  const fn = enclosingFunction(node);
  if (fn === undefined) return "tagComparison";
  if (isRetryPredicate(fn)) return "tagPredicate";
  const handler = errorHandlerKind(fn);
  if (handler === "recovery") {
    return isReasonTagRead(comparison.read) ? "reasonHandler" : "errorHandler";
  }
  if (handler === "mapping") return "mappingHandler";
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
        'Do not branch on `_tag` inside a catch handler. Recover with `Effect.catchTag("{{tag}}", ...)` or `Effect.catchTags({ ... })` so the handled error leaves the error channel.',
      reasonHandler:
        'Do not branch on `reason._tag` inside a catch handler. Recover with `Effect.catchReason("<ErrorTag>", "{{tag}}", ...)` or `Effect.catchReasons("<ErrorTag>", { ... })`.',
      mappingHandler:
        'Do not compare `_tag` with "{{tag}}" inside `mapError` or `tapError`. Map with `Match.valueTags(error, { ... })` or test with `Predicate.isTagged("{{tag}}")`. To handle only this error, use `Effect.catchTag("{{tag}}", ...)`.',
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
          messageId: messageFor(node, comparison),
          data: { tag, guard: builtInGuard(tag) ?? "" },
        });
      },
    };
  },
});
