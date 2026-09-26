import { noTagDestructureRule } from "../rules/no-tag-destructure.ts";
import { ruleTester } from "./rule-tester.ts";

// Invalid cases are trimmed from Streamsy `effect-naming` at ac60a7e2.
ruleTester.run("no-tag-destructure", noTagDestructureRule, {
  valid: ["const { route, detail } = error;", "const { tag } = options;"],
  invalid: [
    {
      // packages/serve/src/action/runtime.ts
      code: 'const isFail = (reason) => { const { _tag: tag } = reason; return tag === "Fail"; };',
      errors: [{ messageId: "tagDestructure" }],
    },
    {
      // packages/core/src/storage/memory/layer.ts
      code: "for (const operation of operations) { const { _tag, ...rest } = operation; apply(_tag, rest); }",
      errors: [{ messageId: "tagDestructure" }],
    },
    {
      code: "const names = results.map(({ _tag: tag }) => tag);",
      errors: [{ messageId: "tagDestructure" }],
    },
  ],
});
