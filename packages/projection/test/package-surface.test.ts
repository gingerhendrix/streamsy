import * as Root from "@streamsy/projection";
import * as Checkpoint from "@streamsy/projection/checkpoint";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Layer } from "effect";
import { Checkpoints, Projection, ProjectionFault } from "@streamsy/projection";
import type {
  AtomicDeclaration,
  AtomicDefinition,
  AtomicStateDefinition,
  Declaration,
  Offsets,
  OutputsDeclaration,
  OutputsDefinition,
  Snapshot,
  StateDefinition,
  StateSource,
  Step,
  WatchOptions,
  RunOptions,
  StateApi,
} from "@streamsy/projection";
import * as Memory from "@streamsy/projection/memory";
import * as Sqlite from "@streamsy/projection/sqlite";
import manifest from "../package.json";

type PublicHelperTypes =
  | StateApi
  | Checkpoint.EncodedStore
  | Declaration<any, any>
  | AtomicDefinition<any, any, any, any>
  | AtomicStateDefinition<any, any, any, any, any, any>
  | AtomicDeclaration<any, any, any, any, any, any>
  | OutputsDefinition<any, any, any, any, any, any, any, any>
  | OutputsDeclaration<any, any, any, any, any, any, any>
  | Snapshot<any, any>
  | Offsets<any>
  | StateDefinition<any, any, any>
  | StateSource<any, any, any>
  | Step<any, any, any, any, any>
  | WatchOptions
  | RunOptions;
const acceptsPublicHelperTypes = (value: PublicHelperTypes): void => void value;
void acceptsPublicHelperTypes;

test("built public entries and declared files exist", () => {
  expect(Checkpoint.recordKey).toBeFunction();
  expect(Checkpoint.stateFromStore).toBeFunction();
  expect(Root.State).toBe(Projection.State);
  expect(Root.State.key).toBe("@streamsy/projection/State");
  expect(Projection.outputs).toBeFunction();
  expect(Object.keys(Root.Output).sort()).toEqual(["remove", "rows", "stream", "upsert"]);
  expect(Projection.read).toBeFunction();
  expect("fold" in Projection).toBe(false);
  expect("loadState" in Projection).toBe(false);
  expect(Projection.forget).toBeFunction();
  expect(Checkpoint.fromStore).toBeFunction();
  expect(Checkpoint.CheckpointRecord).toBeDefined();
  for (const name of [
    "encodeKey",
    "recordKey",
    "fromStore",
    "stateFromStore",
    "releaseLock",
    "hasLock",
    "PendingUnit",
    "PinnedRange",
    "producerId",
    "CheckpointRecord",
  ])
    expect(Object.keys(Root)).not.toContain(name);
  expect(Projection.atomic).toBeFunction();
  expect(Projection.run).toBeFunction();
  expect(Projection.pass).toBeFunction();
  expect("stream" in Projection).toBe(false);
  expect("fused" in Projection).toBe(false);
  expect(Projection.follow).toBeFunction();
  expect("family" in Projection).toBe(false);
  expect(Projection.watch).toBeFunction();
  expect(Projection.items).toBeFunction();
  expect(Projection.each).toBeFunction();
  expect(Projection.key).toBeFunction();
  expect(Checkpoints.key).toBe("@streamsy/projection/Checkpoints");
  expect(
    new ProjectionFault({
      phase: "load",
      reason: "invalid-options",
      message: "",
    })._tag,
  ).toBe("ProjectionFault");
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

test("root exports stay explicit", () => {
  expect(Object.keys(Root).sort()).toEqual([
    "Checkpoints",
    "Output",
    "Projection",
    "ProjectionFault",
    "State",
  ]);
  expect(Object.keys(Projection).sort()).toEqual([
    "State",
    "atomic",
    "each",
    "follow",
    "forget",
    "items",
    "key",
    "outputs",
    "pass",
    "read",
    "run",
    "watch",
  ]);
});
