/** TTL/Expires-At policy and expiry scheduling. */

import type { Stream } from "../../types/factory.ts";
import type { Clock, StreamId, StreamRecord } from "../../types/storage.ts";

export type TouchReason = "append" | "close" | "read" | "live-read";

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

  async touch(streamId: StreamId, record: StreamRecord, reason: TouchReason): Promise<void> {
    if (record.config.ttlSeconds === undefined) return;
    if (reason === "live-read") return;
    const stream = await this.deps.resolve(streamId);
    const expiresAtMs = this.deps.clock.now() + record.config.ttlSeconds * 1000;
    await stream.updateRecord({ lifecycle: { expiresAtMs } });
    await stream.expiry?.scheduleExpiry(expiresAtMs, () => this.deps.onScheduledExpiry(streamId));
  }

  async scheduleExpiry(record: StreamRecord): Promise<void> {
    const at = record.lifecycle.expiresAtMs;
    if (at !== undefined) {
      const stream = await this.deps.resolve(record.id);
      await stream.expiry?.scheduleExpiry(at, () => this.deps.onScheduledExpiry(record.id));
    }
  }

  /**
   * Lazily expire the stream if its deadline has passed, returning the current
   * record. Accepts an already-resolved storage stream so callers that have one
   * avoid a redundant factory lookup. When the stream expires the record is
   * re-read so the returned value reflects the post-expiry state.
   */
  async expireIfNeeded(streamId: StreamId, stream?: Stream): Promise<StreamRecord | null> {
    const resolved = stream ?? (await this.deps.resolve(streamId));
    const record = await resolved.getRecord();
    if (record && this.isExpired(record)) {
      await this.deps.onScheduledExpiry(streamId);
      return resolved.getRecord();
    }
    return record;
  }

  isExpired(record: StreamRecord): boolean {
    const at = record.lifecycle.expiresAtMs;
    return at !== undefined && at <= this.deps.clock.now();
  }
}
