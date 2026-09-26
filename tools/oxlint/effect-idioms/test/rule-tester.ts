// Oxlint's RuleTester needs Node 22 or later. It rejects Bun, so these tests run with `node --test`.
import { describe, it } from "node:test";
import { RuleTester } from "oxlint/plugins-dev";

RuleTester.describe = describe;
RuleTester.it = it;

/** A RuleTester that parses every case as a TypeScript module and reports through node:test. */
export const ruleTester = new RuleTester({
  languageOptions: { sourceType: "module", parserOptions: { lang: "ts" } },
});
