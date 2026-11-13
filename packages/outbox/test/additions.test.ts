import { test } from "vitest";
import { withStore } from "./support.ts";
import {
  dueTimes,
  guardedSettlement,
  filteredReads,
  boundedPruning,
  deadRequeue,
} from "./addition-contracts.ts";

for (const mode of ["memory", "sqlite"] as const) {
  test(`${mode}: dead requeue and fresh attempt accounting`, () => withStore(mode, deadRequeue));
  test(`${mode}: bounded pruning and reenqueuing`, () => withStore(mode, boundedPruning));
  test(`${mode}: filtered inspection and counts`, () => withStore(mode, filteredReads));
  test(`${mode}: settlement refuses missing and terminal entries`, () =>
    withStore(mode, guardedSettlement));
  test(`${mode}: first due times and absorbed repeats`, () => withStore(mode, dueTimes));
}
