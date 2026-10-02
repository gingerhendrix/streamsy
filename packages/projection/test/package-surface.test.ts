import { join } from "node:path";
import { expect, test } from "bun:test";

test("the root entry does not import a SQL driver", async () => {
  const root = await Bun.file(join(import.meta.dir, "..", "dist", "index.js")).text();
  expect(root).not.toContain("@effect/sql");
  expect(root).not.toContain("effect/unstable/sql");
});
