import { expect, test } from "bun:test";
import { closeSync, mkdtempSync, openSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  cleanupWorkerdState,
  reclaimRoot,
  retentionDestination,
  type WorkerdOwnedState,
} from "./workerd-harness.ts";

test("workerd retention accepts a fresh destination and rejects overlap", () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-workerd-root-"));
  const retention = mkdtempSync(join(tmpdir(), ".streamsy-workerd-retention-"));
  try {
    expect(retentionDestination(root, join(retention, "fresh"))).toContain("fresh");
    expect(() => retentionDestination(root, root)).toThrow();
  } finally {
    rmSync(root, { recursive: true, force: true });
    rmSync(retention, { recursive: true, force: true });
  }
});

test("workerd reclamation attempts removal after copy failure", () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-workerd-reclaim-"));
  const retention = mkdtempSync(join(tmpdir(), ".streamsy-workerd-reclaim-target-"));
  const destination = join(retention, basename(root));
  closeSync(openSync(destination, "w"));
  const calls: Array<string> = [];
  try {
    const errors = reclaimRoot(root, retention, (path) => {
      calls.push(path);
      throw new Error("removal failed");
    });
    expect(calls).toEqual([root]);
    expect(errors).toHaveLength(2);
  } finally {
    rmSync(root, { recursive: true, force: true });
    rmSync(retention, { recursive: true, force: true });
  }
});

test("workerd startup ownership can be reclaimed after a transient removal failure", () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-workerd-startup-"));
  let attempts = 0;
  try {
    const first = reclaimRoot(root, undefined, () => {
      attempts += 1;
      if (attempts === 1) throw new Error("startup cleanup failed");
    });
    expect(first).toHaveLength(1);
    const second = reclaimRoot(root, undefined);
    expect(second).toHaveLength(0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("failed disposal keeps the instance and persistence root owned for retry", async () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-workerd-disposal-"));
  let disposeAttempts = 0;
  let removeAttempts = 0;
  const state: WorkerdOwnedState = {
    root,
    disposed: false,
    instance: {
      dispose: async () => {
        disposeAttempts += 1;
        if (disposeAttempts === 1) throw new Error("dispose failed");
      },
    },
  };
  try {
    const first = await cleanupWorkerdState(state, undefined, () => {
      removeAttempts += 1;
      rmSync(root, { recursive: true, force: true });
    });
    expect(first.done).toBe(false);
    expect(removeAttempts).toBe(0);
    expect(state.instance).toBeDefined();
    const second = await cleanupWorkerdState(state, undefined, () => {
      removeAttempts += 1;
      rmSync(root, { recursive: true, force: true });
    });
    expect(second.done).toBe(true);
    expect(disposeAttempts).toBe(2);
    expect(removeAttempts).toBe(1);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("startup state transitions retain a root when removal fails, then reclaim it", async () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-workerd-startup-state-"));
  let removeAttempts = 0;
  const state: WorkerdOwnedState = { root, disposed: true };
  try {
    const first = await cleanupWorkerdState(state, undefined, () => {
      removeAttempts += 1;
      if (removeAttempts === 1) throw new Error("startup removal failed");
      rmSync(root, { recursive: true, force: true });
    });
    expect(first.done).toBe(false);
    expect(state.root).toBe(root);
    const second = await cleanupWorkerdState(state, undefined, () => {
      removeAttempts += 1;
      rmSync(root, { recursive: true, force: true });
    });
    expect(second.done).toBe(true);
    expect(removeAttempts).toBe(2);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
