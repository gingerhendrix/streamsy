import { noTagSwitchRule } from "../rules/no-tag-switch.ts";
import { ruleTester } from "./rule-tester.ts";

// Invalid cases are trimmed from Streamsy `effect-naming` at ac60a7e2.
ruleTester.run("no-tag-switch", noTagSwitchRule, {
  valid: [
    "switch (status) { case 200: ok(); }",
    "const text = Match.valueTags(result, { Created: () => 201, Exists: () => 200 });",
  ],
  invalid: [
    {
      // packages/core/src/http/create.ts
      code: 'switch (result._tag) { case "Created": created(); break; case "Exists": exists(); }',
      errors: [{ messageId: "tagSwitch", line: 1, column: 8 }],
    },
    {
      // packages/core/src/http/protocol-error-response.ts
      code: 'function toResponse(error) { switch (error["_tag"]) { default: return 500; } }',
      errors: [{ messageId: "tagSwitch" }],
    },
  ],
});
