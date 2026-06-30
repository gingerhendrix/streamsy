/**
 * Filesystem-backed per-stream handle bound to one id and one directory:
 *
 *   <root>/<safeId>/
 *     record.json     authoritative tail (id, config, lifecycle, currentOffset, counter)
 *     messages.jsonl  one envelope per line, append-only, offset-ordered
 *     producers.json  { [producerId]: { epoch, lastSeq } }
 *     .lock           cross-process exclusive sentinel
 *
 * The durable files are the single source of truth (the serverless requirement):
 * nothing about correctness depends on in-process state surviving. The in-process
 * notifier and expiry timer are pure latency optimizations.
 *
 * **Atomicity.** Every mutating intent runs the whole read-preconditions-then-write
 * sequence under the per-stream `.lock`, against freshly-read durable state, and is
 * all-or-nothing: a failed precondition writes nothing.
 *
 * **Crash consistency.** Message lines are flushed (fsync) BEFORE `record.json` is
 * advanced via tmp+fsync+rename. `record.currentOffset` is the authoritative
 * visible tail; `listMessages` ignores any trailing lines beyond it. A crash
 * between the message flush and the record rename therefore leaves harmless,
 * invisible extra lines (idempotent offsets make a later re-advance safe).
 *
 * This handle is adapter-private; the public seam is the flat `StorageAdapter`.
 */
import {
  closeSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  writeSync,
} from "node:fs";
import path from "node:path";
import { watch, type FSWatcher } from "node:fs";
import type {
  AppendPlan,
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StorageAppendResult,
  StorageDeleteResult,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "@streamsy/core";
import { runAwaitChangeLoop } from "@streamsy/core";
import {
  decodeEnvelope,
  encodeEnvelope,
  parseRecord,
  serializeRecord,
  streamDir,
} from "./codec.ts";
import { acquireLock, type LockOptions, releaseLock } from "./lock.ts";
import { Notifier } from "./notifier.ts";
import { TimeoutScheduler } from "./timeout-scheduler.ts";

type FailureReason = "offset" | "closed" | "producer";

/** Adapter-internal write engine input. A superset of `AppendPlan` with an
 * optional `createRecord` so `create` can reuse the same atomic engine. */
export interface WritePlan {
  createRecord?: StreamRecord;
  preconditions: AppendPlan["preconditions"];
  messages?: StoredMessage[];
  recordPatch?: StreamRecordPatch;
}

export type WriteResult =
  | { status: "committed"; record: StreamRecord }
  | { status: "precondition-failed"; record: StreamRecord | null; reason?: FailureReason };

type ProducerMap = Record<string, ProducerState>;

export interface FsStreamOptions {
  lock?: LockOptions;
  /** Add `fs.watch` as a cross-process wake source for `awaitChange`. */
  watch?: boolean;
  /** Upper bound on a single parked `awaitChange` wait (`parkCapMs`): a missed
   * wake — a lossy/dropped `fs.watch` event, or a cross-process write with
   * watch off — is repaired within this cap. Default 1000ms. */
  watchPollMs?: number;
}

const DEFAULT_WATCH_POLL_MS = 1000;

export class FsStream {
  readonly dir: string;
  private readonly recordPath: string;
  private readonly messagesPath: string;
  private readonly producersPath: string;
  private readonly lockPath: string;
  private readonly lockOptions: LockOptions;
  private readonly watchEnabled: boolean;
  private readonly watchPollMs: number;
  private readonly notifier = new Notifier();
  private readonly timeout: TimeoutScheduler;

  constructor(
    root: string,
    readonly id: StreamId,
    private readonly deleteFromCache: () => void,
    options: FsStreamOptions = {},
    onScheduledExpiry?: () => Promise<void> | void,
  ) {
    this.dir = streamDir(root, id);
    this.recordPath = path.join(this.dir, "record.json");
    this.messagesPath = path.join(this.dir, "messages.jsonl");
    this.producersPath = path.join(this.dir, "producers.json");
    this.lockPath = path.join(this.dir, ".lock");
    this.lockOptions = options.lock ?? {};
    this.watchEnabled = options.watch ?? false;
    this.watchPollMs = options.watchPollMs ?? DEFAULT_WATCH_POLL_MS;
    this.timeout = new TimeoutScheduler(onScheduledExpiry);
  }

  // ---- reads -------------------------------------------------------------

  getRecord(): Promise<StreamRecord | null> {
    return Promise.resolve(this.readRecord());
  }

  private readRecord(): StreamRecord | null {
    try {
      return parseRecord(readFileSync(this.recordPath, "utf8"));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    }
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return Promise.resolve(this.readProducers()[producerId]);
  }

  private readProducers(): ProducerMap {
    try {
      return JSON.parse(readFileSync(this.producersPath, "utf8")) as ProducerMap;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return {};
      throw error;
    }
  }

  listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const record = this.readRecord();
    if (!record) return Promise.resolve([]);
    const tail = record.currentOffset;

    let raw: string;
    try {
      raw = readFileSync(this.messagesPath, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return Promise.resolve([]);
      throw error;
    }

    // Keep the last occurrence per offset: a crash can leave an orphan line that a
    // later legitimate re-advance supersedes with a freshly-appended line.
    const byOffset = new Map<string, StoredMessage>();
    for (const line of raw.split("\n")) {
      const message = decodeEnvelope(line);
      if (!message) continue;
      if (message.offset > tail) continue; // ignore lines beyond the visible tail
      if (options.after !== undefined && message.offset <= options.after) continue;
      if (options.until !== undefined && message.offset > options.until) continue;
      byOffset.set(message.offset, message);
    }

    const result = [...byOffset.values()].toSorted((a, b) =>
      a.offset < b.offset ? -1 : a.offset > b.offset ? 1 : 0,
    );
    return Promise.resolve(options.limit !== undefined ? result.slice(0, options.limit) : result);
  }

  // ---- writes ------------------------------------------------------------

  async append(plan: AppendPlan): Promise<StorageAppendResult> {
    const out = await this.applyMutation({
      preconditions: plan.preconditions,
      messages: plan.messages,
      recordPatch: plan.recordPatch,
    });
    if (out.status === "committed") return { status: "appended", record: out.record };
    // `reason` is required on the seam. Preconditions are checked directly
    // against freshly-read durable state under the lock, so attribution is
    // exact whenever a precondition tripped; only an unattributable failure
    // (lock-acquisition timeout, record absent/purged before the intent)
    // falls back to "offset", as the contract licenses.
    return { status: "precondition-failed", record: out.record, reason: out.reason ?? "offset" };
  }

  /**
   * Atomic write engine shared by `append` and `create`. Acquires the per-stream
   * lock, runs the all-or-nothing mutation against freshly-read durable state,
   * and wakes parked `awaitChange` waiters after a successful commit. A lock
   * timeout is surfaced as `precondition-failed` (safe: the caller can retry),
   * never as a throw.
   */
  async applyMutation(plan: WritePlan): Promise<WriteResult> {
    mkdirSync(this.dir, { recursive: true });
    const acquired = await acquireLock(this.lockPath, this.lockOptions);
    if (!acquired) return { status: "precondition-failed", record: this.readRecord() };
    try {
      const result = this.applyMutationLocked(plan);
      if (result.status === "committed") this.notifier.wake();
      return result;
    } finally {
      releaseLock(this.lockPath);
    }
  }

  private applyMutationLocked(plan: WritePlan): WriteResult {
    let record = this.readRecord();

    if (plan.createRecord) {
      if (plan.createRecord.id !== this.id) {
        throw new Error(
          `storage-fs: record id ${plan.createRecord.id} does not match bound stream ${this.id}`,
        );
      }
      if (record) return { status: "precondition-failed", record };
      record = plan.createRecord; // not yet persisted; written below
    }

    if (!record) return { status: "precondition-failed", record: null };

    const pre = plan.preconditions;
    if (pre.expectedOffset !== undefined && pre.expectedOffset !== record.currentOffset) {
      return { status: "precondition-failed", record, reason: "offset" };
    }
    if (
      pre.expectedClosed !== undefined &&
      pre.expectedClosed !== (record.lifecycle.closed === true)
    ) {
      return { status: "precondition-failed", record, reason: "closed" };
    }

    // Producer compare-and-set is evaluated (but not written) before any write so
    // a failed CAS writes nothing.
    let nextProducers: ProducerMap | undefined;
    if (pre.producer) {
      const producers = this.readProducers();
      const current = producers[pre.producer.producerId];
      if (pre.producer.expected) {
        if (
          !current ||
          current.epoch !== pre.producer.expected.epoch ||
          current.lastSeq !== pre.producer.expected.lastSeq
        ) {
          return { status: "precondition-failed", record, reason: "producer" };
        }
      } else if (current) {
        return { status: "precondition-failed", record, reason: "producer" };
      }
      nextProducers = { ...producers, [pre.producer.producerId]: pre.producer.next };
    }

    // --- commit: messages first (fsync), then the authoritative record, then producers ---
    if (plan.messages && plan.messages.length > 0) {
      this.appendMessageLines(plan.messages, record.config.contentType);
    }
    const next = applyPatch(record, plan.recordPatch);
    this.writeFileAtomic(this.recordPath, serializeRecord(next));
    if (nextProducers) this.writeFileAtomic(this.producersPath, JSON.stringify(nextProducers));

    return { status: "committed", record: next };
  }

  private appendMessageLines(messages: StoredMessage[], contentType: string): void {
    const payload =
      messages.map((message) => encodeEnvelope(message, contentType)).join("\n") + "\n";
    const fd = openSync(this.messagesPath, "a");
    try {
      writeSync(fd, payload);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
  }

  private writeFileAtomic(target: string, contents: string): void {
    const tmp = `${target}.tmp.${process.pid}.${Date.now()}`;
    const fd = openSync(tmp, "w");
    try {
      writeSync(fd, contents);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(tmp, target); // atomic on POSIX
  }

  // ---- delete ------------------------------------------------------------

  /** Forkless purge: no dependents are possible, so delete is a plain purge. */
  async remove(reason: "delete" | "expiry"): Promise<StorageDeleteResult> {
    // Fast path for an absent stream: its directory (and thus its lock) may not
    // exist, so check before attempting to acquire the lock.
    if (this.readRecord() === null) return { status: "not-found" };

    const acquired = await acquireLock(this.lockPath, this.lockOptions);
    try {
      const record = this.readRecord();
      if (!record) return { status: "not-found" };
      if (reason === "delete" && record.lifecycle.softDeleted === true) return { status: "gone" };
      rmSync(this.dir, { recursive: true, force: true });
      return { status: "purged" };
    } finally {
      if (acquired) releaseLock(this.lockPath);
      this.timeout.cancel();
      // Surface the purge to any parked live waiter before the cache eviction:
      // its loop re-reads and observes the now-absent record (`!present`).
      this.notifier.wake();
      this.deleteFromCache();
    }
  }

  // ---- live wait ---------------------------------------------------------

  /**
   * Level-triggered live wait via the shared exported loop (REQUIRED on the
   * seam). The loop re-reads durable state FIRST on every iteration, so the
   * in-process notifier and the optional `fs.watch` are pure latency
   * optimizations, never correctness. Every park is capped at `watchPollMs`
   * (`parkCapMs`): the read→register window is not atomic w.r.t. wakes and
   * `fs.watch` is lossy, so a missed wake — or a cross-process write with
   * watch off — is repaired within the cap.
   */
  awaitChange(options: AwaitChangeOptions): Promise<AwaitChangeResult> {
    return runAwaitChangeLoop(
      {
        readRecord: () => this.readRecord(),
        waitForWake: (timeoutMs) => this.waitForWake(timeoutMs),
        parkCapMs: this.watchPollMs,
      },
      options,
    );
  }

  /** Park until a same-process wake, an `fs.watch` event (when enabled), or
   * `timeoutMs` — the loop's bounded-sleep floor when watch is off. */
  private waitForWake(timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      let settled = false;
      let watcher: FSWatcher | undefined;
      let unregister: (() => void) | undefined;

      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        unregister?.();
        if (watcher) {
          try {
            watcher.close();
          } catch {
            // already closed
          }
        }
        resolve();
      };

      const timer = setTimeout(finish, timeoutMs);
      unregister = this.notifier.register(finish);
      if (this.watchEnabled) {
        try {
          watcher = watch(this.dir, finish);
        } catch {
          // Directory may not exist yet; the notifier + timeout still cover us.
          watcher = undefined;
        }
      }
    });
  }

  /** Wake parked `awaitChange` waiters (used by external delete coordination). */
  wake(): void {
    this.notifier.wake();
  }

  // ---- expiry ------------------------------------------------------------

  scheduleExpiry(at: number): void {
    this.timeout.schedule(at);
  }

  cancelExpiry(): void {
    this.timeout.cancel();
  }
}

function applyPatch(record: StreamRecord, patch: StreamRecordPatch | undefined): StreamRecord {
  if (!patch) return record;
  return {
    ...record,
    config: { ...record.config, ...patch.config },
    lifecycle: { ...record.lifecycle, ...patch.lifecycle },
    currentOffset: patch.currentOffset ?? record.currentOffset,
    counter: patch.counter ?? record.counter,
  };
}
