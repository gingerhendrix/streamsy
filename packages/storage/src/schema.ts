import type { DialectSql } from "./dialect.ts";

export const STORAGE_SCHEMA_VERSION = 1;

export const STORAGE_VERSION_TABLE = "streamsy_schema";

/** Version tables of earlier formats. A database that holds one must not be migrated. */
export const LEGACY_VERSION_TABLES = [
  "streamsy_schema_version",
  "streamsy_storage_schema_version",
] as const;

export const STORAGE_TABLES = [
  "streamsy_streams",
  "streamsy_messages",
  "streamsy_producers",
  "streamsy_owner_records",
] as const;

export const STORAGE_VERSION_TABLE_DDL: DialectSql<string> = {
  sqlite: `CREATE TABLE ${STORAGE_VERSION_TABLE} (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL)`,
  pg: `CREATE TABLE ${STORAGE_VERSION_TABLE} (version INTEGER PRIMARY KEY, applied_at_ms BIGINT NOT NULL)`,
  mysql: `CREATE TABLE IF NOT EXISTS ${STORAGE_VERSION_TABLE} (version INT PRIMARY KEY, applied_at_ms BIGINT NOT NULL) ENGINE=InnoDB`,
};

// MySQL keys need a bounded VARCHAR. Stream ids are capped at 512 characters by storage.
// `utf8mb4_0900_bin` compares bytes and does not pad, so "x" and "x " are different ids.
const MYSQL_STREAM_ID = "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin";

// InnoDB limits an index key to 3072 bytes. The producer key holds a stream id
// (2048 bytes) and a producer id, so the producer id gets 255 characters (1020 bytes).
const MYSQL_PRODUCER_ID = "VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin";

const MYSQL_OFFSET = "VARCHAR(33) CHARACTER SET ascii COLLATE ascii_bin";

// Owner record keys are encoded projection keys, bounded like stream ids and compared as bytes.
const MYSQL_OWNER_KEY = "VARCHAR(512) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin";

/**
 * Migration versions in order. Version `n` is entry `n - 1`. SQLite and
 * Postgres run one version inside the migration transaction. MySQL commits
 * each DDL statement by itself, so every MySQL statement must be safe to run
 * twice.
 */
export const STORAGE_MIGRATIONS: ReadonlyArray<DialectSql<ReadonlyArray<string>>> = [
  {
    sqlite: [
      `CREATE TABLE streamsy_streams (
 stream_id TEXT PRIMARY KEY, content_type TEXT NOT NULL, ttl_seconds REAL, expires_at TEXT,
 created_at REAL NOT NULL, current_offset TEXT NOT NULL, last_seq TEXT, closed INTEGER NOT NULL,
 closed_at REAL, forked_from TEXT, fork_offset TEXT, fork_sub_offset REAL,
 soft_deleted INTEGER NOT NULL, expires_at_ms REAL
)`,
      `CREATE TABLE streamsy_messages (
 stream_id TEXT NOT NULL, msg_offset TEXT NOT NULL, timestamp REAL NOT NULL, data BLOB NOT NULL,
 PRIMARY KEY(stream_id, msg_offset),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
)`,
      `CREATE TABLE streamsy_producers (
 stream_id TEXT NOT NULL, producer_id TEXT NOT NULL, epoch REAL NOT NULL, last_seq REAL NOT NULL,
 PRIMARY KEY(stream_id, producer_id),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
)`,
      `CREATE INDEX idx_streamsy_streams_forked_from ON streamsy_streams(forked_from)
 WHERE forked_from IS NOT NULL`,
      `CREATE INDEX idx_streamsy_streams_expiry ON streamsy_streams(expires_at_ms, stream_id)
 WHERE expires_at_ms IS NOT NULL AND soft_deleted = 0`,
      `CREATE TABLE streamsy_owner_records (owner_key TEXT PRIMARY KEY, owner_value TEXT NOT NULL)`,
    ],
    pg: [
      `CREATE TABLE streamsy_streams (
 stream_id TEXT COLLATE "C" PRIMARY KEY, content_type TEXT NOT NULL,
 ttl_seconds DOUBLE PRECISION, expires_at TEXT, created_at DOUBLE PRECISION NOT NULL,
 current_offset TEXT COLLATE "C" NOT NULL, last_seq TEXT, closed SMALLINT NOT NULL,
 closed_at DOUBLE PRECISION, forked_from TEXT COLLATE "C", fork_offset TEXT COLLATE "C",
 fork_sub_offset DOUBLE PRECISION, soft_deleted SMALLINT NOT NULL, expires_at_ms DOUBLE PRECISION
)`,
      `CREATE TABLE streamsy_messages (
 stream_id TEXT COLLATE "C" NOT NULL, msg_offset TEXT COLLATE "C" NOT NULL,
 timestamp DOUBLE PRECISION NOT NULL, data BYTEA NOT NULL,
 PRIMARY KEY(stream_id, msg_offset),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
)`,
      `CREATE TABLE streamsy_producers (
 stream_id TEXT COLLATE "C" NOT NULL, producer_id TEXT COLLATE "C" NOT NULL,
 epoch DOUBLE PRECISION NOT NULL, last_seq DOUBLE PRECISION NOT NULL,
 PRIMARY KEY(stream_id, producer_id),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
)`,
      `CREATE INDEX idx_streamsy_streams_forked_from ON streamsy_streams(forked_from)
 WHERE forked_from IS NOT NULL`,
      `CREATE INDEX idx_streamsy_streams_expiry ON streamsy_streams(expires_at_ms, stream_id)
 WHERE expires_at_ms IS NOT NULL AND soft_deleted = 0`,
      `CREATE TABLE streamsy_owner_records (
 owner_key TEXT COLLATE "C" PRIMARY KEY, owner_value TEXT NOT NULL
)`,
    ],
    // MySQL has no CREATE INDEX IF NOT EXISTS, so the indexes live in CREATE TABLE.
    mysql: [
      `CREATE TABLE IF NOT EXISTS streamsy_streams (
 stream_id ${MYSQL_STREAM_ID} PRIMARY KEY, content_type TEXT NOT NULL, ttl_seconds DOUBLE,
 expires_at TEXT, created_at DOUBLE NOT NULL, current_offset ${MYSQL_OFFSET} NOT NULL,
 last_seq TEXT, closed TINYINT NOT NULL, closed_at DOUBLE, forked_from ${MYSQL_STREAM_ID},
 fork_offset ${MYSQL_OFFSET}, fork_sub_offset DOUBLE, soft_deleted TINYINT NOT NULL,
 expires_at_ms DOUBLE,
 INDEX idx_streamsy_streams_forked_from (forked_from),
 INDEX idx_streamsy_streams_expiry (expires_at_ms, stream_id)
) ENGINE=InnoDB`,
      `CREATE TABLE IF NOT EXISTS streamsy_messages (
 stream_id ${MYSQL_STREAM_ID} NOT NULL, msg_offset ${MYSQL_OFFSET} NOT NULL,
 timestamp DOUBLE NOT NULL, data LONGBLOB NOT NULL,
 PRIMARY KEY(stream_id, msg_offset),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB`,
      `CREATE TABLE IF NOT EXISTS streamsy_producers (
 stream_id ${MYSQL_STREAM_ID} NOT NULL, producer_id ${MYSQL_PRODUCER_ID} NOT NULL,
 epoch DOUBLE NOT NULL, last_seq DOUBLE NOT NULL,
 PRIMARY KEY(stream_id, producer_id),
 FOREIGN KEY(stream_id) REFERENCES streamsy_streams(stream_id) ON DELETE CASCADE
) ENGINE=InnoDB`,
      `CREATE TABLE IF NOT EXISTS streamsy_owner_records (
 owner_key ${MYSQL_OWNER_KEY} PRIMARY KEY, owner_value LONGTEXT NOT NULL
) ENGINE=InnoDB`,
    ],
  },
];
