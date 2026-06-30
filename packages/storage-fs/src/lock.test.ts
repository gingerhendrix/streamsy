/**
 * Cross-process exclusive lock: mutual exclusion, bounded timeout, and stale
 * recovery (age-based and dead-owner based).
 */
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "bun:test";
import { acquireLock, releaseLock } from "./lock.ts";

function freshLockPath(): string {
  return path.join(mkdtempSync(path.join(tmpdir(), "streamsy-fs-lock-")), ".lock");
}

describe("acquireLock / releaseLock", () => {
  it("grants the lock and writes an owner sentinel", async () => {
    const lockPath = freshLockPath();
    expect(await acquireLock(lockPath)).toBe(true);
    expect(existsSync(lockPath)).toBe(true);
    const sentinel = JSON.parse(readFileSync(lockPath, "utf8"));
    expect(sentinel.pid).toBe(process.pid);
    releaseLock(lockPath);
    expect(existsSync(lockPath)).toBe(false);
  });

  it("excludes a second holder until released", async () => {
    const lockPath = freshLockPath();
    expect(await acquireLock(lockPath)).toBe(true);

    const start = Date.now();
    expect(await acquireLock(lockPath, { timeoutMs: 100, retryMs: 10, staleMs: 60_000 })).toBe(
      false,
    );
    expect(Date.now() - start).toBeGreaterThanOrEqual(90);

    releaseLock(lockPath);
    expect(await acquireLock(lockPath, { timeoutMs: 100 })).toBe(true);
    releaseLock(lockPath);
  });

  it("reclaims a lock older than staleMs", async () => {
    const lockPath = freshLockPath();
    // A live holder (our own pid), but timestamped far in the past.
    writeFileSync(lockPath, JSON.stringify({ pid: process.pid, ts: 0, host: "" }));
    expect(await acquireLock(lockPath, { timeoutMs: 200, staleMs: 1000 })).toBe(true);
    releaseLock(lockPath);
  });

  it("reclaims a lock held by a dead owner", async () => {
    const lockPath = freshLockPath();
    // A fresh timestamp but an impossible/dead pid on this host.
    writeFileSync(lockPath, JSON.stringify({ pid: 2_147_483_646, ts: Date.now(), host: "" }));
    expect(await acquireLock(lockPath, { timeoutMs: 200, staleMs: 60_000 })).toBe(true);
    releaseLock(lockPath);
  });

  it("reclaims a corrupt sentinel", async () => {
    const lockPath = freshLockPath();
    writeFileSync(lockPath, "not json");
    expect(await acquireLock(lockPath, { timeoutMs: 200 })).toBe(true);
    releaseLock(lockPath);
  });
});
