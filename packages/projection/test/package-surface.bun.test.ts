import { existsSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import manifest from "../package.json";

test("built public entries and declared files exist", () => {
  for (const entry of Object.values(manifest.exports)) {
    expect(existsSync(join(import.meta.dir, "..", entry.types))).toBe(true);
    expect(existsSync(join(import.meta.dir, "..", entry.import))).toBe(true);
  }
});
