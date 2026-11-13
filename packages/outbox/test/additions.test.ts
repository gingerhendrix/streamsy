import { test } from "vitest";
import { withStore } from "./support.ts";
import { dueTimes } from "./addition-contracts.ts";

for (const mode of ["memory", "sqlite"] as const) {
  test(`${mode}: first due times and absorbed repeats`, () => withStore(mode, dueTimes));
}
