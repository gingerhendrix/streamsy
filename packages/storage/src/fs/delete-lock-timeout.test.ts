/* oxlint-disable effecttsgo/async-function, effecttsgo/global-date, effecttsgo/global-timers, effecttsgo/new-promise -- Bun owns this lock/timer regression. */
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, test } from "bun:test";
import { ZERO_OFFSET, type DeletePlan, type StreamRecord } from "@streamsy/core";
import { createFsStorageAdapter } from "./adapter.ts";
import { acquireLock, releaseLock } from "./lock.ts";

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function heldLockDeleteRegression(reason: DeletePlan["reason"]): Promise<void> {
  const root = mkdtempSync(path.join(tmpdir(), `streamsy-fs-${reason}-lock-`));
  let expiryCallbacks = 0;
  let resolveExpiry!: () => void;
  let expiryFired = new Promise<void>((resolve) => {
    resolveExpiry = resolve;
  });
  const expiresAtMs = Date.now() + 60_000;
  const record: StreamRecord = {
    id: "contended",
    config: { contentType: "text/plain", ttlSeconds: 60, createdAt: Date.now() },
    lifecycle: { expiresAtMs },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
  const adapter = createFsStorageAdapter({
    root,
    lock: { timeoutMs: 20, retryMs: 2, staleMs: 60_000 },
    watch: false,
    watchPollMs: 5_000,
    onScheduledExpiry: () => {
      expiryCallbacks += 1;
      resolveExpiry();
    },
  });

  try {
    expect(await adapter.create({ record })).toEqual({ status: "created", record });
    const handle = adapter.state.getExistingStream(record.id);
    expect(handle).toBeDefined();
    const lockPath = path.join(handle!.dir, ".lock");

    // Park both an active replacement-generation timer and a live waiter on the
    // cached handle before another owner takes the durable mutation lock.
    await adapter.scheduleExpiry(record.id, Date.now() + 80);
    const waiter = adapter.awaitChange(record.id, {
      fromOffset: ZERO_OFFSET,
      observedClosed: false,
      observedSoftDeleted: false,
      timeoutMs: 2_000,
    });
    let waiterSettled = false;
    void waiter.then(() => {
      waiterSettled = true;
    });
    await delay(10);

    expect(await acquireLock(lockPath, { timeoutMs: 20, retryMs: 2, staleMs: 60_000 })).toBe(true);
    const peerLock = readFileSync(lockPath, "utf8");
    const plan: DeletePlan =
      reason === "expiry"
        ? { streamId: record.id, reason, expectedExpiresAtMs: expiresAtMs }
        : { streamId: record.id, reason };

    expect(await adapter.delete(plan)).toEqual({ status: "busy" });
    expect(await adapter.getRecord(record.id)).toEqual(record);
    expect(readFileSync(lockPath, "utf8")).toBe(peerLock);
    expect(adapter.state.getExistingStream(record.id)).toBe(handle);
    expect(waiterSettled).toBe(false);

    // Firing proves the timeout did not cancel the replacement timer. Re-arm a
    // second generation so the successful purge below can prove cancellation.
    await Promise.race([
      expiryFired,
      delay(2_000).then(() => {
        throw new Error("replacement expiry callback did not survive lock timeout");
      }),
    ]);
    expect(expiryCallbacks).toBe(1);
    expiryFired = new Promise<void>((resolve) => {
      resolveExpiry = resolve;
    });
    await adapter.scheduleExpiry(record.id, Date.now() + 100);

    releaseLock(lockPath);
    expect(await adapter.delete(plan)).toEqual({ status: "purged" });
    expect(existsSync(lockPath)).toBe(false);
    expect(adapter.state.getExistingStream(record.id)).toBeUndefined();
    expect(await waiter).toEqual({
      status: "changed",
      snapshot: { present: false, currentOffset: ZERO_OFFSET, closed: false, softDeleted: false },
    });
    expect(await adapter.getRecord(record.id)).toBeNull();
    await Promise.race([expiryFired, delay(180)]);
    expect(expiryCallbacks).toBe(1);
  } finally {
    await adapter.cancelExpiry(record.id);
    rmSync(root, { recursive: true, force: true });
  }
}

describe("filesystem delete lock timeout", () => {
  test("manual delete is inert while a peer lock is held, then performs complete purge cleanup", () =>
    heldLockDeleteRegression("delete"));

  test("expiry delete is inert while a peer lock is held, then performs complete purge cleanup", () =>
    heldLockDeleteRegression("expiry"));
});
