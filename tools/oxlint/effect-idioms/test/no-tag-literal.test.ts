import { noTagLiteralRule } from "../rules/no-tag-literal.ts";
import { ruleTester } from "./rule-tester.ts";

// Invalid cases are trimmed from Streamsy `effect-naming` at ac60a7e2.
ruleTester.run("no-tag-literal", noTagLiteralRule, {
  valid: [
    "type Applied = { readonly _tag: 'Applied'; readonly ambient: boolean };",
    "interface Route { readonly _tag: 'StreamRoute' }",
    'const Created = Schema.TaggedStruct("Created", { record: Record });',
    'const Wire = Schema.Struct({ _tag: Schema.Literal("BadRequest") });',
    "const decoded = { _tag, route, detail };",
    'expect(body).toMatchObject({ _tag: "BadRequest" });',
    'expect(outcome).toEqual({ results: [{ _tag: "Created" }] });',
    "class Fault extends Data.TaggedError('Fault')<{ readonly detail: string }> {}",
  ],
  invalid: [
    {
      // examples/issue-tracker/server/state.ts
      code: 'const missing = Effect.fail({ _tag: "UnknownIssue" as const });',
      errors: [{ messageId: "tagLiteral", data: { tag: "UnknownIssue" } }],
    },
    {
      // packages/storage/src/storage.ts
      code: 'results.push({ _tag: "Created", record: copyRecord(current) });',
      errors: [{ messageId: "tagLiteral", data: { tag: "Created" } }],
    },
    {
      // packages/core/src/fetch/response.ts
      code: 'const outcome = { _tag: response.status === 201 ? ("Created" as const) : ("Exists" as const) };',
      errors: [{ messageId: "tagLiteral", data: { tag: "Created | Exists" } }],
    },
    {
      // packages/serve/src/serve.ts
      code: 'fail({ _tag: "TransportUnavailable", route: path, detail: "Storage unavailable" }, 503);',
      errors: [{ messageId: "tagLiteral", data: { tag: "TransportUnavailable" } }],
    },
    {
      code: 'const quoted = { "_tag": "Rows" };',
      errors: [{ messageId: "tagLiteral", data: { tag: "Rows" } }],
    },
    {
      code: 'class Rejected { readonly _tag = "Rejected" as const; }',
      errors: [{ messageId: "tagField", data: { tag: "Rejected" } }],
    },
  ],
});
