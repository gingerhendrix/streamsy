import type { StreamRecord } from "../schema/index.ts";

export interface ExpiryConfig {
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
}

/** Preserve the expiry strings accepted before Effect 4.1 broadened zone parsing. */
export function isValidExpiresAt(value: string): boolean {
  const hasZone = /Z|GMT|[+-]\d{2}$|[+-]\d{2}:?\d{2}$|\]$/.test(value);

  return !Number.isNaN(Date.parse(hasZone ? value : `${value}Z`));
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
