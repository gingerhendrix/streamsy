import { existsSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Checkpoints, Projection, ProjectionFault } from "@streamsy/projection";
import { layerMemory } from "@streamsy/projection/memory";
import { Checkpoints as SqliteEntryCheckpoints } from "@streamsy/projection/sqlite";
import manifest from "../package.json";

test("built public entries and declared files exist", () => {
  expect(Projection.make).toBeFunction();
  expect(Projection.run).toBeFunction();
  expect(Projection.pass).toBeFunction();
  expect(Projection.stream).toBeFunction();
  expect(Projection.follow).toBeFunction();
  expect(Projection.items).toBeFunction();
  expect(Projection.each).toBeFunction();
  expect(Checkpoints.key).toBe("@streamsy/projection/Checkpoints");
  expect(SqliteEntryCheckpoints).toBe(Checkpoints);
  expect(new ProjectionFault({ phase: "load", reason: "invalid-budget", message: "" })._tag).toBe(
    "ProjectionFault",
  );
  expect(layerMemory).toBeFunction();
  for (const entry of Object.values(manifest.exports)) {
    expect(existsSync(join(import.meta.dir, "..", entry.types))).toBe(true);
    expect(existsSync(join(import.meta.dir, "..", entry.import))).toBe(true);
  }
});
