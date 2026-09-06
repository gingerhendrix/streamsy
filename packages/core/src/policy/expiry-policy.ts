import type { StreamRecord } from "../schema/index.ts";
export interface ExpiryConfig {
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
}
/** Pure rules ported from ExpiryPolicy; touch and expiry execution belong to the protocol. */
export function computeExpiresAtMs(config: ExpiryConfig, now: number): number | undefined {
  if (config.ttlSeconds !== undefined) return now + config.ttlSeconds * 1000;
  if (config.expiresAt) return Date.parse(config.expiresAt);
  return undefined;
}
export function isExpired(record: StreamRecord, now: number): boolean {
  const at = record.lifecycle.expiresAtMs;
  return at !== undefined && at <= now;
}
