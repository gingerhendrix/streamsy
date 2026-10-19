import { eslintCompatPlugin } from "@oxlint/plugins";

import { noTagComparisonRule } from "./rules/no-tag-comparison.ts";
import { noTagDestructureRule } from "./rules/no-tag-destructure.ts";
import { noTagLiteralRule } from "./rules/no-tag-literal.ts";
import { noTagSwitchRule } from "./rules/no-tag-switch.ts";

/** Repository-owned Oxlint rules that steer tagged values towards Effect's own utilities. */
const effectIdiomsPlugin = eslintCompatPlugin({
  meta: { name: "effect-idioms" },
  rules: {
    "no-tag-comparison": noTagComparisonRule,
    "no-tag-destructure": noTagDestructureRule,
    "no-tag-literal": noTagLiteralRule,
    "no-tag-switch": noTagSwitchRule,
  },
});

export default effectIdiomsPlugin;
