/** TTL/Expires-At policy and expiry scheduling. */

import type { Stream } from "../../types/factory.ts";
import type { Clock, StreamId, StreamRecord } from "../../types/storage.ts";
import { runAfterCommit } from "./after-commit-effects.ts";

export type TouchReason = "read" | "live-read";
export type ScheduledExpiryHandler = (streamId: StreamId) => Promise<void>;
export type ResolveStorageStream = (streamId: StreamId) => Promise<Stream> | Stream;

export interface ExpiryConfig {
  ttlSeconds?: number;
  expiresAt?: string;
}

export interface ExpiryPolicyDeps {
  resolve: ResolveStorageStream;
  clock: Clock;
  onScheduledExpiry: ScheduledExpiryHandler;
}

export class ExpiryPolicy {
  constructor(private deps: ExpiryPolicyDeps) {}

  computeExpiresAtMs(config: ExpiryConfig): number | undefined {
    if (config.ttlSeconds !== undefined) return this.deps.clock.now() + config.ttlSeconds * 1000;
    if (config.expiresAt) return this.deps.clock.date(config.expiresAt).getTime();
    return undefined;
  }

  async touch(stream: Stream, record: StreamRecord, reason: TouchReason): Promise<StreamRecord> {
    if (record.config.ttlSeconds === undefined) return record;
    if (reason === "live-read") return record;
    const expiresAtMs = this.deps.clock.now() + record.config.ttlSeconds * 1000;
    const afterCommit = { scheduleExpiryAt: expiresAtMs };
    const out = await stream.commit({
      preconditions: { expectedOffset: record.currentOffset },
      recordPatch: { lifecycle: { expiresAtMs } },
      afterCommit,
    });
    if (out.status !== "committed") return record;
    await runAfterCommit(afterCommit, stream);
    return out.record;
  }

  async scheduleExpiry(record: StreamRecord): Promise<void> {
    const at = record.lifecycle.expiresAtMs;
    if (at !== undefined) {
      const stream = await this.deps.resolve(record.id);
      await stream.scheduleExpiry(at);
    }
  }

  /**
   * Lazily expire the stream if its deadline has passed, returning the current
   * record. The storage stream is already bound to an id, so callers should pass
   * that stream instead of threading a duplicate stream id alongside it. When
   * the stream expires the record is re-read so the returned value reflects the
   * post-expiry state.
   */
  async expireIfNeeded(stream: Stream): Promise<StreamRecord | null> {
    const record = await stream.getRecord();
    if (record && this.isExpired(record)) {
      await this.deps.onScheduledExpiry(stream.id);
      return stream.getRecord();
    }
    return record;
  }

  isExpired(record: StreamRecord): boolean {
    const at = record.lifecycle.expiresAtMs;
    return at !== undefined && at <= this.deps.clock.now();
  }
}
