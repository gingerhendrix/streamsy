export const STORAGE_SCHEMA_VERSION = 2;
export const STORAGE_VERSION_TABLE = "streamsy_storage_schema_version";

export const STORAGE_MIGRATIONS = [
  [
    `CREATE TABLE streamsy_streams (
 stream_id TEXT PRIMARY KEY, content_type TEXT NOT NULL, ttl_seconds REAL, expires_at TEXT,
 created_at REAL NOT NULL, current_offset TEXT NOT NULL, last_seq TEXT, closed INTEGER NOT NULL,
 closed_at REAL, forked_from TEXT, fork_offset TEXT, fork_sub_offset REAL,
 soft_deleted INTEGER NOT NULL, expires_at_ms REAL
)`,
    `CREATE TABLE streamsy_messages (
 stream_id TEXT NOT NULL, offset TEXT NOT NULL, timestamp REAL NOT NULL, data BLOB NOT NULL,
 PRIMARY KEY(stream_id, offset),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
)`,
    `CREATE TABLE streamsy_producers (
 stream_id TEXT NOT NULL, producer_id TEXT NOT NULL, epoch REAL NOT NULL, last_seq REAL NOT NULL,
 PRIMARY KEY(stream_id, producer_id),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
)`,
    `CREATE INDEX idx_streamsy_streams_forked_from ON streamsy_streams(forked_from)
 WHERE forked_from IS NOT NULL`,
  ],
  [
    `CREATE INDEX idx_streamsy_streams_expiry ON streamsy_streams(expires_at_ms, stream_id)
 WHERE expires_at_ms IS NOT NULL AND soft_deleted = 0`,
  ],
] as const;
