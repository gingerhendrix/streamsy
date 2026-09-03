import { describe, expect, test } from "bun:test";
import { rowToRecord, type StreamRow } from "./codec.ts";

function storedRow(): StreamRow {
  return {
    stream_id: "stream",
    content_type: "text/plain",
    ttl_seconds: null,
    expires_at: null,
    created_at: 1_000,
    current_offset: "0000000000000000_0000000000000000",
    counter: 0,
    last_seq: null,
    closed: 0,
    closed_at: null,
    forked_from: null,
    fork_offset: null,
    fork_sub_offset: null,
    soft_deleted: 0,
    expires_at_ms: null,
  };
}

describe("SQLite row codec", () => {
  test("omits optional record fields stored as NULL", () => {
    const record = rowToRecord(storedRow());

    expect(record.config).toEqual({ contentType: "text/plain", createdAt: 1_000 });
    expect(record.lifecycle).toEqual({ closed: false, softDeleted: false });
    expect("ttlSeconds" in record.config).toBe(false);
    expect("expiresAt" in record.config).toBe(false);
    expect("lastSeq" in record.lifecycle).toBe(false);
    expect("closedAt" in record.lifecycle).toBe(false);
    expect("forkedFrom" in record.lifecycle).toBe(false);
    expect("forkOffset" in record.lifecycle).toBe(false);
    expect("forkSubOffset" in record.lifecycle).toBe(false);
    expect("expiresAtMs" in record.lifecycle).toBe(false);
  });

  test("assigns every optional record field stored with a value", () => {
    const row = storedRow();
    row.ttl_seconds = 60;
    row.expires_at = "2030-01-01T00:00:00.000Z";
    row.last_seq = "producer:4";
    row.closed = 1;
    row.closed_at = 2_000;
    row.forked_from = "parent";
    row.fork_offset = "0000000000000001_0000000000000000";
    row.fork_sub_offset = 3;
    row.soft_deleted = 1;
    row.expires_at_ms = 3_000;

    expect(rowToRecord(row)).toEqual({
      id: "stream",
      config: {
        contentType: "text/plain",
        createdAt: 1_000,
        ttlSeconds: 60,
        expiresAt: "2030-01-01T00:00:00.000Z",
      },
      lifecycle: {
        closed: true,
        softDeleted: true,
        lastSeq: "producer:4",
        closedAt: 2_000,
        forkedFrom: "parent",
        forkOffset: "0000000000000001_0000000000000000",
        forkSubOffset: 3,
        expiresAtMs: 3_000,
      },
      currentOffset: "0000000000000000_0000000000000000",
      counter: 0,
    });
  });
});
