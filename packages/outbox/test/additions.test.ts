import { test } from "vitest";
import { withStore } from "./support.ts";
import { dueTimes, guardedSettlement } from "./addition-contracts.ts";

for (const mode of ["memory", "sqlite"] as const) {
  test(`${mode}: settlement refuses missing and terminal entries`, () =>
    withStore(mode, guardedSettlement));
  test(`${mode}: first due times and absorbed repeats`, () => withStore(mode, dueTimes));
}
