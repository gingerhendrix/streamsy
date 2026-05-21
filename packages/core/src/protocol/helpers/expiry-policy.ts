/**
 * TTL/Expires-At policy and expiry scheduling for the durable streams protocol.
 *
 * Responsibilities:
 *
 * - `computeExpiresAtMs(config)` returns `clock.now() + ttlSeconds * 1000`
 *   when `ttlSeconds` is set, falling back to `clock.date(expiresAt).getTime()`
 *   when only `expiresAt` is set; absent both yields `undefined`. `ttlSeconds`
 *   takes precedence over `expiresAt` when both are present.
 * - `touch(streamId, record, reason)` is a no-op when the record has no
 *   `ttlSeconds`. The `live-read` reason never extends TTL. For `append`,
 *   `close`, or `read` it updates `lifecycle.expiresAtMs` to
 *   `clock.now() + ttlSeconds * 1000` and reschedules via
 *   `store.scheduleExpiry?`, passing the protocol's scheduled-expiry callback.
 * - `scheduleExpiry(record)` issues the initial scheduler call from the
 *   record's persisted `lifecycle.expiresAtMs`, no-op when absent.
 * - `expireIfNeeded(streamId)` loads the record and invokes the injected
 *   scheduled-expiry handler only when the persisted deadline is at or below
 *   the current clock; the handler is responsible for rechecking the
 *   persisted deadline before purge/soft-delete, so stale scheduled callbacks
 *   remain safe.
 *
 * Used by `StreamProtocol` before create/read/append/delete operations, by
 * create/fork services when scheduling initial expiry, by append/read/live-read
 * touch paths, and by `StreamGcService` for scheduled-expiry decisions. The
 * scheduled-expiry handler is injected so GC/delete behavior remains owned by
 * `StreamGcService` and `StreamProtocol` wiring.
 *
 * Not exported from `packages/core/src/index.ts`; service-level tests import
 * directly from this module.
 */

import type { Clock, StreamId, StreamRecord, StreamStoreAdapter } from "../../types/storage.ts";

export type TouchReason = "append" | "close" | "read" | "live-read";

export type ScheduledExpiryHandler = (streamId: StreamId) => Promise<void>;

export interface ExpiryConfig {
  ttlSeconds?: number;
  expiresAt?: string;
}

export class ExpiryPolicy {
  constructor(
    private store: StreamStoreAdapter,
    private clock: Clock,
    private onScheduledExpiry: ScheduledExpiryHandler,
  ) {}

  computeExpiresAtMs(config: ExpiryConfig): number | undefined {
    if (config.ttlSeconds !== undefined) return this.clock.now() + config.ttlSeconds * 1000;
    if (config.expiresAt) return this.clock.date(config.expiresAt).getTime();
    return undefined;
  }

  async touch(streamId: StreamId, record: StreamRecord, reason: TouchReason): Promise<void> {
    if (record.config.ttlSeconds === undefined) return;
    if (reason === "live-read") return;
    const expiresAtMs = this.clock.now() + record.config.ttlSeconds * 1000;
    await this.store.update(streamId, { lifecycle: { expiresAtMs } });
    await this.store.scheduleExpiry?.(streamId, expiresAtMs, () =>
      this.onScheduledExpiry(streamId),
    );
  }

  async scheduleExpiry(record: StreamRecord): Promise<void> {
    const at = record.lifecycle.expiresAtMs;
    if (at !== undefined)
      await this.store.scheduleExpiry?.(record.id, at, () => this.onScheduledExpiry(record.id));
  }

  async expireIfNeeded(streamId: StreamId): Promise<void> {
    const record = await this.store.get(streamId);
    if (record && this.isExpired(record)) await this.onScheduledExpiry(streamId);
  }

  isExpired(record: StreamRecord): boolean {
    const at = record.lifecycle.expiresAtMs;
    return at !== undefined && at <= this.clock.now();
  }
}
