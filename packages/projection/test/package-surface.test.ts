import * as Root from "@streamsy/projection";
import * as Checkpoint from "@streamsy/projection/checkpoint";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Layer } from "effect";
import { CheckpointStore, Projection, ProjectionFault, Transaction } from "@streamsy/projection";
import type {
  Declaration,
  Offsets,
  Snapshot,
  StateDefinition,
  Step,
  WatchOptions,
  RunOptions,
  CheckpointStoreApi,
} from "@streamsy/projection";
import manifest from "../package.json";

type PublicHelperTypes =
  | CheckpointStoreApi
  | Checkpoint.EncodedStore
  | Declaration<any, any>
  | Snapshot<any, any>
  | Offsets<any>
  | StateDefinition<any, any, any>
  | Step<number, never, never>
  | WatchOptions
  | RunOptions;
const acceptsPublicHelperTypes = (value: PublicHelperTypes): void => void value;
void acceptsPublicHelperTypes;

test("built public entries and declared files exist", () => {
  expect(Checkpoint.recordKey).toBeFunction();
  expect(Root.Transaction.key).toBe("@streamsy/projection/Transaction");
  expect(Projection.emit).toBeFunction();
  expect(Object.keys(Root)).not.toContain("Output");
  expect(Projection.read).toBeFunction();
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
  ])
    expect(Object.keys(Root)).not.toContain(name);
  expect(Projection.fold).toBeFunction();
  expect(Projection.run).toBeFunction();
  expect(Projection.pass).toBeFunction();
  expect(Projection.follow).toBeFunction();
  expect(Projection.watch).toBeFunction();
  expect(Projection.items).toBeFunction();
  expect(Projection.each).toBeFunction();
  expect(Projection.key).toBeFunction();
  expect(CheckpointStore.key).toBe("@streamsy/projection/CheckpointStore");
  expect(
    new ProjectionFault({
      phase: "load",
      reason: "invalid-options",
      message: "",
    })._tag,
  ).toBe("ProjectionFault");
  expect(Layer.isLayer(CheckpointStore.layer)).toBe(true);
  expect(Layer.isLayer(CheckpointStore.layerMemory)).toBe(true);
  expect(Layer.isLayer(Transaction.layer)).toBe(true);
  expect(Object.keys(manifest.exports)).toEqual([".", "./checkpoint"]);
  expect(Object.keys(manifest.dependencies).sort()).toEqual(["@streamsy/core", "effect"]);
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
    "CheckpointRecord",
    "CheckpointStore",
    "Commit",
    "HandlerFailed",
    "Projection",
    "ProjectionFault",
    "ProjectionRetry",
    "Transaction",
  ]);
  expect(Object.keys(Projection).sort()).toEqual([
    "decide",
    "each",
    "emit",
    "fold",
    "follow",
    "forget",
    "items",
    "key",
    "mapAccum",
    "mapAccumBatch",
    "pass",
    "process",
    "read",
    "run",
    "watch",
  ]);
});

test("Commit and checkpoint subpaths expose only their author contracts", () => {
  expect(Object.keys(Root.Commit).sort()).toEqual([
    "Accumulated",
    "Emitted",
    "Folded",
    "HandlerFailed",
    "TypeId",
    "atLeastOnce",
    "checkpoint",
    "make",
    "producer",
    "transaction",
    "withCompensation",
  ]);
  expect(Object.keys(Checkpoint).sort()).toEqual([
    "CheckpointRecord",
    "CheckpointStore",
    "fromStore",
    "recordKey",
  ]);
});
