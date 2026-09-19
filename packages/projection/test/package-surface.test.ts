import * as Root from "@streamsy/projection";
import * as Checkpoint from "@streamsy/projection/checkpoint";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Layer } from "effect";
import { Checkpoints, Projection, ProjectionFault } from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";
import manifest from "../package.json";

test("built public entries and declared files exist", () => {
  expect(Checkpoint.recordKey).toBeFunction();
  expect(Checkpoint.fromStore).toBeFunction();
  expect(Checkpoint.CheckpointRecord).toBeDefined();
  for (const name of [
    "encodeKey",
    "recordKey",
    "fromStore",
    "PendingUnit",
    "PinnedRange",
    "producerId",
    "CheckpointRecord",
  ])
    expect(Object.keys(Root)).not.toContain(name);
  expect(Projection.make).toBeFunction();
  expect(Projection.run).toBeFunction();
  expect(Projection.pass).toBeFunction();
  expect(Projection.stream).toBeFunction();
  expect(Projection.follow).toBeFunction();
  expect(Projection.items).toBeFunction();
  expect(Projection.each).toBeFunction();
  expect(Projection.key).toBeFunction();
  expect(Checkpoints.key).toBe("@streamsy/projection/Checkpoints");
  expect(new ProjectionFault({ phase: "load", reason: "invalid-options", message: "" })._tag).toBe(
    "ProjectionFault",
  );
  expect(Layer.isLayer(Memory.layer)).toBe(true);
  expect(Memory.layerMemory).toBeFunction();
  expect(Layer.isLayer(Sqlite.layer)).toBe(true);
  expect(Object.keys(manifest.exports)).toEqual([".", "./memory", "./sqlite", "./checkpoint"]);
  for (const entry of Object.values(manifest.exports)) {
    expect(existsSync(join(import.meta.dir, "..", entry.types))).toBe(true);
    expect(existsSync(join(import.meta.dir, "..", entry.import))).toBe(true);
  }
  for (const built of ["dist/examples/minimal.js", "dist/examples/minimal.d.ts"]) {
    expect(existsSync(join(import.meta.dir, "..", built))).toBe(true);
  }
});

test("the root entry does not import a SQL driver", async () => {
  const root = await Bun.file(join(import.meta.dir, "..", "dist", "index.js")).text();
  expect(root).not.toContain("@effect/sql");
  expect(root).not.toContain("effect/unstable/sql");
});
