/**
 * Cross-process exclusive per-stream lock.
 *
 * Acquisition is an atomic `open(O_CREAT | O_EXCL)` of a sentinel file — the
 * kernel guarantees exactly one creator wins, across processes, on a local
 * filesystem. The sentinel records the owner pid and a timestamp so a lock left
 * behind by a crashed (e.g. serverless) process can be reclaimed:
 *
 *   - **age:** older than `staleMs` ⇒ assume abandoned.
 *   - **liveness:** the owner pid no longer exists (same host) ⇒ abandoned.
 *
 * Acquisition is bounded by `timeoutMs`; on timeout the caller treats it like a
 * busy-retry exhaustion and returns a seam result (never throws where the seam
 * expects a result). The lock is advisory between cooperating adapter processes,
 * which is exactly the multi-writer model here.
 */
import { closeSync, openSync, readFileSync, unlinkSync, writeSync } from "node:fs";
import {
  isErrnoException,
  isJsonNumber,
  isJsonObject,
  isJsonString,
  type PersistedJson,
  type PersistedJsonObject,
} from "./guards.ts";

export interface LockOptions {
  /** Max time to wait for the lock before giving up. Default 5000ms. */
  timeoutMs?: number;
  /** A held lock older than this is considered stale and reclaimable. Default 30000ms. */
  staleMs?: number;
  /** Backoff between contended retries. Default 25ms. */
  retryMs?: number;
}

interface LockFileContents {
  pid: number;
  ts: number;
  host?: string;
}

const DEFAULTS = { timeoutMs: 5000, staleMs: 30_000, retryMs: 25 } as const;

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function pidIsAlive(pid: number): boolean {
  try {
    // Signal 0 performs error checking without delivering a signal.
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // ESRCH: no such process ⇒ dead. EPERM: exists but not ours ⇒ alive.
    return isErrnoException(error) && error.code === "EPERM";
  }
}

function isLockFileContents(value: PersistedJson): value is PersistedJsonObject & LockFileContents {
  if (!isJsonObject(value)) return false;
  if (!isJsonNumber(value.pid) || !isJsonNumber(value.ts)) return false;
  return value.host === undefined || isJsonString(value.host);
}

/**
 * Parse a sentinel's contents, or `null` when they are unusable.
 *
 * The staleness decision reads `ts` and `pid`, so the parsed value is validated
 * before those fields are trusted: a sentinel holding well-formed JSON of the
 * wrong shape would otherwise compare as `NaN`/`undefined`, be judged neither
 * aged-out nor dead-owned, and pin the lock forever.
 */
function parseLockFile(raw: string): LockFileContents | null {
  let parsed: PersistedJson;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  return isLockFileContents(parsed) ? parsed : null;
}

/** Whether the lock at `lockPath` looks abandoned and may be reclaimed. */
function lockIsStale(lockPath: string, staleMs: number): boolean {
  let raw: string;
  try {
    raw = readFileSync(lockPath, "utf8");
  } catch {
    // Gone or unreadable between checks — treat as reclaimable; the next
    // O_EXCL create will resolve the race deterministically.
    return true;
  }
  // Corrupt or unrecognizable sentinel: reclaim it.
  const parsed = parseLockFile(raw);
  if (parsed === null) return true;
  if (Date.now() - parsed.ts > staleMs) return true;
  if (parsed.host === undefined || parsed.host === hostId()) {
    return !pidIsAlive(parsed.pid);
  }
  return false;
}

function hostId(): string {
  return process.env.HOSTNAME ?? "";
}

/**
 * Try to acquire the lock, retrying until `timeoutMs` elapses.
 * Resolves `true` on success, `false` on timeout.
 */
export async function acquireLock(lockPath: string, options: LockOptions = {}): Promise<boolean> {
  const timeoutMs = options.timeoutMs ?? DEFAULTS.timeoutMs;
  const staleMs = options.staleMs ?? DEFAULTS.staleMs;
  const retryMs = options.retryMs ?? DEFAULTS.retryMs;
  const deadline = Date.now() + timeoutMs;

  while (true) {
    try {
      const fd = openSync(lockPath, "wx");
      try {
        const contents: LockFileContents = { pid: process.pid, ts: Date.now(), host: hostId() };
        writeSync(fd, JSON.stringify(contents));
      } finally {
        closeSync(fd);
      }
      return true;
    } catch (error) {
      if (!isErrnoException(error) || error.code !== "EEXIST") throw error;
      if (lockIsStale(lockPath, staleMs)) {
        try {
          unlinkSync(lockPath);
        } catch {
          // Another writer reclaimed it first; fall through to retry.
        }
      }
      if (Date.now() >= deadline) return false;
      await delay(retryMs);
    }
  }
}

/** Release a held lock. Tolerant of an already-removed sentinel (e.g. purge). */
export function releaseLock(lockPath: string): void {
  try {
    unlinkSync(lockPath);
  } catch {
    // Already gone (purged dir, reclaimed by a peer) — nothing to do.
  }
}
