/* oxlint-disable effecttsgo/async-function -- Bun file/package import boundary. */
import { expect, test } from "bun:test";

test("host entries and optional peers keep the package graph exact", async () => {
  const root = await Bun.file(new URL("../src/index.ts", import.meta.url)).text();
  expect(root).not.toContain("@effect/sql-sqlite-bun");
  expect(root).not.toContain("@effect/sql-sqlite-do");
});
