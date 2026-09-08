import { expect, test } from "bun:test";
import { closeSync, mkdtempSync, openSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { reclaimRoot, retentionDestination } from "./workerd-harness.ts";

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
    const errors = reclaimRoot(
      root,
      retention,
      (path) => {
        calls.push(path);
        throw new Error("removal failed");
      },
    );
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
