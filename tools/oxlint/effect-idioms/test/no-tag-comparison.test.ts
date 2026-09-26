import { noTagComparisonRule } from "../rules/no-tag-comparison.ts";
import { ruleTester } from "./rule-tester.ts";

// Invalid cases are trimmed from Streamsy `effect-naming` at ac60a7e2.
ruleTester.run("no-tag-comparison", noTagComparisonRule, {
  valid: [
    'const isValue = Predicate.isTagged("Value");',
    "const failed = Exit.isFailure(exit);",
    "if (left._tag === right._tag) same();",
    "const label = `${entry._tag}`;",
    'const kind = typeof value === "string";',
    'if ("value" in option) read(option);',
  ],
  invalid: [
    {
      // examples/issue-tracker/server/state.ts
      code: 'effect.pipe(Effect.retry({ times: 3, while: (error) => error._tag === "OffsetMismatch" }));',
      errors: [{ messageId: "tagPredicate", data: { tag: "OffsetMismatch", guard: "" } }],
    },
    {
      // packages/projection/src/outputs.ts
      code: 'const values = Object.values(outputs).filter((output) => output._tag === "Value");',
      errors: [{ messageId: "tagPredicate", data: { tag: "Value", guard: "" } }],
    },
    {
      // packages/projection/src/watch.ts
      code: 'Effect.mapError((cause) => (cause._tag === "StreamGone" ? gone(cause) : other(cause)));',
      errors: [{ messageId: "errorHandler", data: { tag: "StreamGone", guard: "" } }],
    },
    {
      // examples/issue-tracker/server/app.ts
      code: 'if (decoded._tag === "Failure") respond(400);',
      errors: [
        {
          messageId: "builtInGuard",
          data: { tag: "Failure", guard: "Exit.isFailure or Result.isFailure" },
        },
      ],
    },
    {
      // examples/fold-agent/src/cli.ts
      code: 'const failed = failure._tag === "Some" && failure.value instanceof Error;',
      errors: [{ messageId: "builtInGuard", data: { tag: "Some", guard: "Option.isSome" } }],
    },
    {
      // packages/projection/src/run.ts
      code: 'const run = (projection) => projection._tag === "Fused" ? passFused(projection) : passStream(projection);',
      errors: [{ messageId: "tagComparison", data: { tag: "Fused", guard: "" } }],
    },
    {
      // examples/fold-agent/src/session-log.ts
      code: 'const bad = entry._tag !== "session_started";',
      errors: [{ messageId: "tagComparison", data: { tag: "session_started", guard: "" } }],
    },
    {
      code: 'const optional = value?.["_tag"] === "Rows";',
      errors: [{ messageId: "tagComparison", data: { tag: "Rows", guard: "" } }],
    },
    {
      // packages/projection/src/outputs.ts
      code: 'if ("_tag" in handler) useHandler(handler);',
      errors: [{ messageId: "tagInCheck" }],
    },
  ],
});
