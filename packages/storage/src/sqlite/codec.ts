/**
 * Row <-> core type mapping for the SQLite adapter.
 *
 * Booleans are stored as integer 0/1, optional fields as NULL. Offsets are the
 * core-generated fixed-width strings, so SQL lexicographic ordering matches
 * `compareOffsets` and no separate ordinal column is required.
 */
import type { StreamRecord } from "@streamsy/core";

/** Row shape of `streamsy_streams`. */
export interface StreamRow {
  stream_id: string;
  content_type: string;
  ttl_seconds: number | null;
  expires_at: string | null;
  created_at: number;
  current_offset: string;
  counter: number;
  last_seq: string | null;
  closed: number;
  closed_at: number | null;
  forked_from: string | null;
  fork_offset: string | null;
  fork_sub_offset: number | null;
  soft_deleted: number;
  expires_at_ms: number | null;
}

/** Positional column list used by insert/update statements. */
export const STREAM_COLUMNS = [
  "stream_id",
  "content_type",
  "ttl_seconds",
  "expires_at",
  "created_at",
  "current_offset",
  "counter",
  "last_seq",
  "closed",
  "closed_at",
  "forked_from",
  "fork_offset",
  "fork_sub_offset",
  "soft_deleted",
  "expires_at_ms",
] as const;

export type StreamColumnValue = string | number | null;

/** Flatten a `StreamRecord` to the positional row values for write statements. */
export function recordToRow(record: StreamRecord): StreamColumnValue[] {
  const { config, lifecycle } = record;
  return [
    record.id,
    config.contentType,
    config.ttlSeconds ?? null,
    config.expiresAt ?? null,
    config.createdAt,
    record.currentOffset,
    record.counter,
    lifecycle.lastSeq ?? null,
    lifecycle.closed ? 1 : 0,
    lifecycle.closedAt ?? null,
    lifecycle.forkedFrom ?? null,
    lifecycle.forkOffset ?? null,
    lifecycle.forkSubOffset ?? null,
    lifecycle.softDeleted ? 1 : 0,
    lifecycle.expiresAtMs ?? null,
  ];
}

/** Rebuild a `StreamRecord` from a stored row, dropping NULL optionals. */
export function rowToRecord(row: StreamRow): StreamRecord {
  const config: StreamRecord["config"] = {
    contentType: row.content_type,
    createdAt: row.created_at,
  };
  if (row.ttl_seconds !== null) config.ttlSeconds = row.ttl_seconds;
  if (row.expires_at !== null) config.expiresAt = row.expires_at;

  const lifecycle: StreamRecord["lifecycle"] = {
    closed: row.closed === 1,
    softDeleted: row.soft_deleted === 1,
  };
  if (row.last_seq !== null) lifecycle.lastSeq = row.last_seq;
  if (row.closed_at !== null) lifecycle.closedAt = row.closed_at;
  if (row.forked_from !== null) lifecycle.forkedFrom = row.forked_from;
  if (row.fork_offset !== null) lifecycle.forkOffset = row.fork_offset;
  if (row.fork_sub_offset !== null) lifecycle.forkSubOffset = row.fork_sub_offset;
  if (row.expires_at_ms !== null) lifecycle.expiresAtMs = row.expires_at_ms;

  return {
    id: row.stream_id,
    config,
    lifecycle,
    currentOffset: row.current_offset,
    counter: row.counter,
  };
}
