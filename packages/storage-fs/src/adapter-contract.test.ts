/**
 * Storage-adapter contract against the JSONL filesystem adapter.
 *
 * Runs the shared `runStorageAdapterContract` kit over a fresh isolated temp root
 * per case under `bun:test`, in BOTH configurations. `awaitChange` is REQUIRED on
 * the seam, so both modes exercise the adapter's own `runAwaitChangeLoop`-based
 * implementation:
 *   - watch OFF — wakes come from the in-process notifier plus the capped park
 *     (the polling floor for cross-process writers).
 *   - watch ON  — additionally races `fs.watch` as the cross-process wake source,
 *     including "a non-advancing wake re-checks and keeps waiting".
 * (Fork stays omitted — the kit auto-skips fork-dependent cases for a forkless
 * adapter.)
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, it } from "bun:test";
import { runStorageAdapterContract } from "@streamsy/core";
import { createFsStorageAdapter } from "./adapter.ts";

function freshRoot(): string {
  return mkdtempSync(path.join(tmpdir(), "streamsy-fs-contract-"));
}

describe("fs adapter — storage contract (watch off)", () => {
  runStorageAdapterContract(
    () => createFsStorageAdapter({ root: freshRoot(), watch: false, watchPollMs: 50 }),
    { it },
  );
});

describe("fs adapter — storage contract (watch on)", () => {
  runStorageAdapterContract(
    () => createFsStorageAdapter({ root: freshRoot(), watch: true, watchPollMs: 50 }),
    { it },
  );
});
