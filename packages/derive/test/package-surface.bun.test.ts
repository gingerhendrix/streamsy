/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- The test verifies built package files and public entry imports. */
import { existsSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Projection, StreamSource, StreamSink, Commit } from "@streamsy/derive";
import { layerMemory } from "@streamsy/derive/memory";
import { layer } from "@streamsy/derive/sqlite";
import manifest from "../package.json";

test("built public entries and declared files exist", () => {
  expect(Projection.catchUp).toBeFunction();
  expect(StreamSource.make).toBeFunction();
  expect(StreamSink.make).toBeFunction();
  expect(Commit.key).toBe("@streamsy/derive/Commit");
  expect(layerMemory).toBeFunction();
  expect(layer).toBeDefined();
  for (const entry of Object.values(manifest.exports)) {
    expect(existsSync(join(import.meta.dir, "..", entry.types))).toBe(true);
    expect(existsSync(join(import.meta.dir, "..", entry.import))).toBe(true);
  }
});
