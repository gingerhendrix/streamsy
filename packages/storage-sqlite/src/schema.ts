/**
 * Logical SQL schema and migration runner for the Bun SQLite adapter.
 *
 * Migrations are applied idempotently on open (unless disabled). A
 * `streamsy_schema_version` table records the highest applied version so an
 * existing database file is upgraded in place. The schema mirrors the core
 * `StreamRecord` / `StoredMessage` / `ProducerState` shapes as flat columns
 * (no JSON blobs for hot fields) so it stays portable to other SQL dialects.
 */
import type { Database } from "bun:sqlite";

export const SCHEMA_VERSION = 1;

/** Ordered migration statements. Index `i` produces schema version `i + 1`. */
const MIGRATIONS: string[] = [
  // v1 — initial schema.
  `
  create table if not exists streamsy_streams (
    stream_id       text primary key,
    content_type    text not null,
    ttl_seconds     integer,
    expires_at      text,
    created_at      integer not null,
    current_offset  text not null,
    counter         integer not null,
    last_seq        text,
    closed          integer not null default 0,
    closed_at       integer,
    forked_from     text,
    fork_offset     text,
    child_ref_count integer not null default 0,
    soft_deleted    integer not null default 0,
    expires_at_ms   integer
  );

  create table if not exists streamsy_messages (
    stream_id  text not null,
    offset     text not null,
    timestamp  integer not null,
    data       blob not null,
    primary key (stream_id, offset),
    foreign key (stream_id) references streamsy_streams(stream_id) on delete cascade
  );

  create table if not exists streamsy_producers (
    stream_id   text not null,
    producer_id text not null,
    epoch       integer not null,
    last_seq    integer not null,
    primary key (stream_id, producer_id),
    foreign key (stream_id) references streamsy_streams(stream_id) on delete cascade
  );

  create index if not exists idx_streamsy_streams_expires_at_ms
    on streamsy_streams(expires_at_ms)
    where expires_at_ms is not null and soft_deleted = 0;

  create index if not exists idx_streamsy_streams_forked_from
    on streamsy_streams(forked_from)
    where forked_from is not null;
  `,
];

/**
 * Apply any outstanding migrations. Safe to call repeatedly; only versions
 * beyond the database's current version run, each inside its own transaction.
 */
export function migrate(db: Database): void {
  db.run(`
    create table if not exists streamsy_schema_version (
      version       integer primary key,
      applied_at_ms integer not null
    );
  `);

  const row = db
    .query<{ version: number | null }, []>(
      "select max(version) as version from streamsy_schema_version",
    )
    .get();
  const current = row?.version ?? 0;

  for (let version = current + 1; version <= MIGRATIONS.length; version++) {
    const statements = MIGRATIONS[version - 1]!;
    const applyMigration = db.transaction((appliedAtMs: number) => {
      db.run(statements);
      db.run("insert into streamsy_schema_version (version, applied_at_ms) values (?, ?)", [
        version,
        appliedAtMs,
      ]);
    });
    applyMigration(Date.now());
  }
}
