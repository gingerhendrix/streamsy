import { Context, Effect, Layer, Option, Predicate, Schema, type Stream } from "effect";
import {
  ChangeSnapshot,
  type MessageWindow,
  Offset,
  ProducerState,
  Storage,
  StorageFault,
  StoredMessage,
  StreamId,
  StreamRecord,
  ZERO_OFFSET,
  type Mutation,
  type MutationOutcome,
  type OperationResult,
  type RecordPatch,
} from "@streamsy/core";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError";
import {
  BoundaryRuntimeService,
  CommitBoundary,
  boundaryLayer,
  type BoundaryTestProbe,
  type BoundaryRuntime,
} from "./boundary.ts";
import { migrate } from "./migrations.ts";
import {
  DEFAULT_TRANSACTION_RETRY_ATTEMPTS,
  DEFAULT_TRANSACTION_RETRY_DELAY_MS,
  transactionRetryPolicy,
} from "./transaction-retry.ts";

export const DEFAULT_REPAIR_INTERVAL_MS = 1_000;
export { DEFAULT_TRANSACTION_RETRY_ATTEMPTS, DEFAULT_TRANSACTION_RETRY_DELAY_MS };
const STORAGE_KEY = "streamsy:storage";

export interface SqlStorageOptions {
  /** Cross-process and post-commit-interruption staleness bound. */
  readonly repairIntervalMs?: number;
  /** Total attempts for a standalone storage-owned transaction. */
  readonly transactionRetryAttempts?: number;
  /** Yield between retryable, known-rolled-back transaction attempts. */
  readonly transactionRetryDelayMs?: number;
}

const RecordRow = Schema.Struct({
  stream_id: StreamId,
  content_type: Schema.String,
  ttl_seconds: Schema.NullOr(Schema.Finite),
  expires_at: Schema.NullOr(Schema.String),
  created_at: Schema.Finite,
  current_offset: Offset,
  last_seq: Schema.NullOr(Schema.String),
  closed: Schema.Literals([0, 1]),
  closed_at: Schema.NullOr(Schema.Finite),
  forked_from: Schema.NullOr(StreamId),
  fork_offset: Schema.NullOr(Offset),
  fork_sub_offset: Schema.NullOr(Schema.Finite),
  soft_deleted: Schema.Literals([0, 1]),
  expires_at_ms: Schema.NullOr(Schema.Finite),
});

const ProducerRow = Schema.Struct({ epoch: Schema.Finite, last_seq: Schema.Finite });
const ExpiryRow = Schema.Struct({ at: Schema.Finite, stream_id: StreamId });
const SnapshotRow = Schema.Struct({
  current_offset: Schema.NullOr(Offset),
  closed: Schema.NullOr(Schema.Literals([0, 1])),
  soft_deleted: Schema.NullOr(Schema.Literals([0, 1])),
});

type RecordRow = typeof RecordRow.Type;
type RecordDatabaseRow = typeof RecordRow.Encoded;
type ProducerDatabaseRow = typeof ProducerRow.Encoded;
type ExpiryDatabaseRow = typeof ExpiryRow.Encoded;
type SnapshotDatabaseRow = typeof SnapshotRow.Encoded;

const decodeRecordRow = (row: RecordDatabaseRow) =>
  Schema.decodeEffect(RecordRow)(row).pipe(Effect.orDie);
const decodeProducerRow = (row: ProducerDatabaseRow) =>
  Schema.decodeEffect(ProducerRow)(row).pipe(Effect.orDie);
const decodeExpiryRow = (row: ExpiryDatabaseRow) =>
  Schema.decodeEffect(ExpiryRow)(row).pipe(Effect.orDie);
const decodeSnapshotRow = (row: SnapshotDatabaseRow) =>
  Schema.decodeEffect(SnapshotRow)(row).pipe(Effect.orDie);

interface MutableConfig {
  contentType: string;
  createdAt: number;
  ttlSeconds?: number;
  expiresAt?: string;
}

interface MutableLifecycle {
  closed: boolean;
  softDeleted: boolean;
  lastSeq?: string;
  closedAt?: number;
  forkedFrom?: StreamId;
  forkOffset?: Offset;
  forkSubOffset?: number;
  expiresAtMs?: number;
}

const toRecord = (row: RecordRow): StreamRecord => {
  const config: MutableConfig = {
    contentType: row.content_type,
    createdAt: row.created_at,
  };
  if (row.ttl_seconds !== null) config.ttlSeconds = row.ttl_seconds;
  if (row.expires_at !== null) config.expiresAt = row.expires_at;
  const lifecycle: MutableLifecycle = {
    closed: row.closed === 1,
    softDeleted: row.soft_deleted === 1,
  };
  if (row.last_seq !== null) lifecycle.lastSeq = row.last_seq;
  if (row.closed_at !== null) lifecycle.closedAt = row.closed_at;
  if (row.forked_from !== null) lifecycle.forkedFrom = row.forked_from;
  if (row.fork_offset !== null) lifecycle.forkOffset = row.fork_offset;
  if (row.fork_sub_offset !== null) lifecycle.forkSubOffset = row.fork_sub_offset;
  if (row.expires_at_ms !== null) lifecycle.expiresAtMs = row.expires_at_ms;
  return { id: row.stream_id, config, lifecycle, currentOffset: row.current_offset };
};

const patchRecord = (record: StreamRecord, patch: RecordPatch): StreamRecord => {
  const config = { ...record.config, ...patch.config };
  const lifecycle = { ...record.lifecycle, ...patch.lifecycle };
  for (const field of patch.clear ?? []) {
    if (field === "ttlSeconds") delete config.ttlSeconds;
    if (field === "expiresAt") delete config.expiresAt;
    if (field === "expiresAtMs") delete lifecycle.expiresAtMs;
    if (field === "lastSeq") delete lifecycle.lastSeq;
  }
  return {
    id: record.id,
    config,
    lifecycle,
    currentOffset: patch.currentOffset ?? record.currentOffset,
  };
};

const copyRecord = (record: StreamRecord): StreamRecord => ({
  id: record.id,
  currentOffset: record.currentOffset,
  config: { ...record.config },
  lifecycle: { ...record.lifecycle },
});

const sqlFault = (operation: string, error: SqlError): StorageFault =>
  new StorageFault({
    operation,
    message: error.message,
    retryable: error.isRetryable,
    cause: error,
  });

const attempt = <A>(operation: string, effect: Effect.Effect<A, SqlError>) =>
  effect.pipe(Effect.mapError((error) => sqlFault(operation, error)));

const query = <A extends object>(
  sql: SqlClient.SqlClient,
  operation: string,
  statement: string,
  params: ReadonlyArray<unknown> = [],
) => attempt(operation, sql.unsafe<A>(statement, params));

const execute = (
  sql: SqlClient.SqlClient,
  operation: string,
  statement: string,
  params: ReadonlyArray<unknown> = [],
) => query<Record<string, never>>(sql, operation, statement, params).pipe(Effect.asVoid);

const recordColumns =
  "stream_id,content_type,ttl_seconds,expires_at,created_at,current_offset,last_seq," +
  "closed,closed_at,forked_from,fork_offset,fork_sub_offset,soft_deleted,expires_at_ms";

const readRecord = (sql: SqlClient.SqlClient, id: StreamId) =>
  query<RecordDatabaseRow>(
    sql,
    "record",
    `SELECT ${recordColumns} FROM streamsy_streams WHERE stream_id = ?`,
    [id],
  ).pipe(
    Effect.flatMap((rows) => {
      const row = rows[0];
      return row === undefined
        ? Effect.succeed(Option.none<StreamRecord>())
        : decodeRecordRow(row).pipe(Effect.map((decoded) => Option.some(toRecord(decoded))));
    }),
  );

const insertRecord = (sql: SqlClient.SqlClient, record: StreamRecord) =>
  execute(
    sql,
    "mutate.create-record",
    "INSERT INTO streamsy_streams VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    [
      record.id,
      record.config.contentType,
      record.config.ttlSeconds ?? null,
      record.config.expiresAt ?? null,
      record.config.createdAt,
      record.currentOffset,
      record.lifecycle.lastSeq ?? null,
      record.lifecycle.closed ? 1 : 0,
      record.lifecycle.closedAt ?? null,
      record.lifecycle.forkedFrom ?? null,
      record.lifecycle.forkOffset ?? null,
      record.lifecycle.forkSubOffset ?? null,
      record.lifecycle.softDeleted ? 1 : 0,
      record.lifecycle.expiresAtMs ?? null,
    ],
  );

const updateRecord = (sql: SqlClient.SqlClient, record: StreamRecord) =>
  execute(
    sql,
    "mutate.update-record",
    "UPDATE streamsy_streams SET content_type=?,ttl_seconds=?,expires_at=?,created_at=?," +
      "current_offset=?,last_seq=?,closed=?,closed_at=?,forked_from=?,fork_offset=?," +
      "fork_sub_offset=?,soft_deleted=?,expires_at_ms=? WHERE stream_id=?",
    [
      record.config.contentType,
      record.config.ttlSeconds ?? null,
      record.config.expiresAt ?? null,
      record.config.createdAt,
      record.currentOffset,
      record.lifecycle.lastSeq ?? null,
      record.lifecycle.closed ? 1 : 0,
      record.lifecycle.closedAt ?? null,
      record.lifecycle.forkedFrom ?? null,
      record.lifecycle.forkOffset ?? null,
      record.lifecycle.forkSubOffset ?? null,
      record.lifecycle.softDeleted ? 1 : 0,
      record.lifecycle.expiresAtMs ?? null,
      record.id,
    ],
  );

const rejection = (
  index: number,
  reason: Extract<MutationOutcome, { readonly _tag: "Rejected" }>["reason"],
  record: Option.Option<StreamRecord>,
): MutationOutcome => ({ _tag: "Rejected", index, reason, record });

const preflight = (
  sql: SqlClient.SqlClient,
  mutation: Mutation,
): Effect.Effect<MutationOutcome | undefined, StorageFault> =>
  Effect.gen(function* () {
    for (const [index, operation] of mutation.operations.entries()) {
      const id = Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId;
      const current = yield* readRecord(sql, id);
      if (Predicate.isTagged(operation, "Create")) {
        if (Option.isSome(current)) return rejection(index, "exists", current);
        if (operation.forkSource !== undefined) {
          const source = yield* readRecord(sql, operation.forkSource.id);
          if (
            Option.isNone(source) ||
            source.value.lifecycle.softDeleted ||
            source.value.currentOffset < operation.forkSource.liveAtOffset
          )
            return rejection(index, "fork-source-gone", Option.none());
        }
        continue;
      }
      if (Option.isNone(current)) return rejection(index, "not-found", Option.none());
      const record = current.value;
      if (record.lifecycle.softDeleted) return rejection(index, "gone", current);
      if (Predicate.isTagged(operation, "Delete")) {
        if (
          operation.reason === "expiry" &&
          (operation.expectedExpiresAtMs === undefined ||
            operation.expectedExpiresAtMs !== record.lifecycle.expiresAtMs)
        )
          return rejection(index, "expiry-mismatch", current);
        continue;
      }
      if (
        operation.expectedOffset !== undefined &&
        operation.expectedOffset !== record.currentOffset
      )
        return rejection(index, "offset", current);
      if (
        operation.expectedClosed !== undefined &&
        operation.expectedClosed !== record.lifecycle.closed
      )
        return rejection(index, "closed", current);
      if (operation.producer !== undefined) {
        const rows = yield* query<ProducerDatabaseRow>(
          sql,
          "mutate.preflight-producer",
          "SELECT epoch,last_seq FROM streamsy_producers WHERE stream_id=? AND producer_id=?",
          [id, operation.producer.producerId],
        );
        const actual =
          rows[0] === undefined
            ? Option.none<ProducerState>()
            : Option.some(
                yield* decodeProducerRow(rows[0]).pipe(
                  Effect.map(({ epoch, last_seq: lastSeq }) => ({ epoch, lastSeq })),
                ),
              );
        const expected = operation.producer.expected;
        if (
          Option.isSome(actual) !== Option.isSome(expected) ||
          (Option.isSome(actual) &&
            Option.isSome(expected) &&
            (actual.value.epoch !== expected.value.epoch ||
              actual.value.lastSeq !== expected.value.lastSeq))
        )
          return rejection(index, "producer", current);
      }
    }
    return undefined;
  });

const hasDependents = (sql: SqlClient.SqlClient, id: StreamId) =>
  query<{ readonly present: number }>(
    sql,
    "mutate.dependents",
    "SELECT EXISTS(SELECT 1 FROM streamsy_streams WHERE forked_from=?) present",
    [id],
  ).pipe(Effect.map((rows) => rows[0]?.present === 1));

const purge = (sql: SqlClient.SqlClient, start: StreamRecord) =>
  Effect.gen(function* () {
    let current: StreamRecord | undefined = start;
    while (current !== undefined) {
      const parentId: StreamId | undefined = current.lifecycle.forkedFrom;
      // Keep cleanup explicit instead of depending on a host connection PRAGMA.
      yield* execute(
        sql,
        "mutate.purge-messages",
        "DELETE FROM streamsy_messages WHERE stream_id=?",
        [current.id],
      );
      yield* execute(
        sql,
        "mutate.purge-producers",
        "DELETE FROM streamsy_producers WHERE stream_id=?",
        [current.id],
      );
      yield* execute(sql, "mutate.purge", "DELETE FROM streamsy_streams WHERE stream_id=?", [
        current.id,
      ]);
      if (parentId === undefined) return;
      const parent: Option.Option<StreamRecord> = yield* readRecord(sql, parentId);
      if (
        Option.isNone(parent) ||
        !parent.value.lifecycle.softDeleted ||
        (yield* hasDependents(sql, parentId))
      )
        return;
      current = parent.value;
    }
  });

const applyMutation = (
  sql: SqlClient.SqlClient,
  mutation: Mutation,
): Effect.Effect<MutationOutcome, StorageFault> =>
  Effect.gen(function* () {
    const rejected = yield* preflight(sql, mutation);
    if (rejected !== undefined) return rejected;
    const records = new Map<StreamId, StreamRecord>();
    const results: Array<OperationResult> = [];

    // Creation first preserves a fork edge when source deletion appears first.
    for (const operation of mutation.operations) {
      if (!Predicate.isTagged(operation, "Create")) continue;
      yield* insertRecord(sql, operation.record);
      for (const message of operation.initialMessages)
        yield* execute(
          sql,
          "mutate.create-message",
          "INSERT INTO streamsy_messages(stream_id,offset,timestamp,data) VALUES (?,?,?,?)",
          [operation.record.id, message.offset, message.timestamp, new Uint8Array(message.data)],
        );
      records.set(operation.record.id, operation.record);
    }

    for (const operation of mutation.operations) {
      const id = Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId;
      const current = records.get(id) ?? Option.getOrThrow(yield* readRecord(sql, id));
      if (Predicate.isTagged(operation, "Create")) {
        results.push({ _tag: "Created", record: copyRecord(current) });
        continue;
      }
      if (Predicate.isTagged(operation, "Append")) {
        for (const message of operation.messages)
          yield* execute(
            sql,
            "mutate.append-message",
            "INSERT INTO streamsy_messages(stream_id,offset,timestamp,data) VALUES (?,?,?,?)",
            [id, message.offset, message.timestamp, new Uint8Array(message.data)],
          );
        const next = patchRecord(current, operation.patch);
        yield* updateRecord(sql, next);
        if (operation.producer !== undefined)
          yield* execute(
            sql,
            "mutate.producer",
            "INSERT INTO streamsy_producers(stream_id,producer_id,epoch,last_seq) VALUES (?,?,?,?) " +
              "ON CONFLICT(stream_id,producer_id) DO UPDATE SET epoch=excluded.epoch,last_seq=excluded.last_seq",
            [
              id,
              operation.producer.producerId,
              operation.producer.next.epoch,
              operation.producer.next.lastSeq,
            ],
          );
        records.set(id, next);
        results.push({ _tag: "Appended", record: next });
        continue;
      }
      if (yield* hasDependents(sql, id)) {
        const next = patchRecord(current, { lifecycle: { softDeleted: true } });
        yield* updateRecord(sql, next);
        results.push({ _tag: "SoftDeleted", record: next });
      } else {
        results.push({ _tag: "Purged", record: current });
        yield* purge(sql, current);
      }
    }
    const [first, ...rest] = results;
    if (first === undefined) return yield* Effect.die(new Error("Empty mutation"));
    return { _tag: "Applied" as const, results: [first, ...rest] };
  });

interface MessageDatabaseRow {
  readonly offset: unknown;
  readonly timestamp: unknown;
  readonly data: unknown;
}

const directMessage = (row: MessageDatabaseRow): StoredMessage => {
  if (
    !Predicate.isString(row.offset) ||
    !/^\d{16}_\d{16}$/.test(row.offset) ||
    !Predicate.isNumber(row.timestamp) ||
    !Number.isFinite(row.timestamp) ||
    !Predicate.isUint8Array(row.data)
  )
    throw new Error("Corrupt persisted message row");
  return {
    offset: Offset.make(row.offset),
    timestamp: row.timestamp,
    data: new Uint8Array(row.data),
  };
};

const readMessages = (sql: SqlClient.SqlClient, id: StreamId, window: MessageWindow) =>
  Effect.gen(function* () {
    let remaining = window.limit === undefined ? undefined : Math.max(0, Math.trunc(window.limit));
    if (remaining === 0) return [];
    const visited = new Set<StreamId>();
    const chain: Array<{ readonly id: StreamId; readonly until?: Offset }> = [];
    let cursor: StreamId | undefined = id;
    let until: Offset | undefined;
    while (cursor !== undefined) {
      if (visited.has(cursor)) return yield* Effect.die(new Error("Corrupt cyclic stream lineage"));
      visited.add(cursor);
      chain.push(until === undefined ? { id: cursor } : { id: cursor, until });
      const record: Option.Option<StreamRecord> = yield* readRecord(sql, cursor);
      if (Option.isNone(record)) break;
      cursor = record.value.lifecycle.forkedFrom;
      until = record.value.lifecycle.forkOffset;
    }
    const messages: Array<StoredMessage> = [];
    for (const segment of chain.toReversed()) {
      if (remaining === 0) break;
      const upper =
        segment.until === undefined
          ? window.until
          : window.until === undefined || segment.until < window.until
            ? segment.until
            : window.until;
      const predicates = ["stream_id=?"];
      const parameters: Array<unknown> = [segment.id];
      if (window.after !== undefined) {
        predicates.push("offset>?");
        parameters.push(window.after);
      }
      if (upper !== undefined) {
        predicates.push("offset<=?");
        parameters.push(upper);
      }
      if (remaining !== undefined) parameters.push(remaining);
      const rows = yield* query<MessageDatabaseRow>(
        sql,
        "messages",
        `SELECT offset,timestamp,data FROM streamsy_messages WHERE ${predicates.join(" AND ")} ` +
          "ORDER BY offset COLLATE BINARY" +
          (remaining === undefined ? "" : " LIMIT ?"),
        parameters,
      );
      for (const row of rows)
        messages.push(yield* Effect.try(() => directMessage(row)).pipe(Effect.orDie));
      if (remaining !== undefined) remaining -= rows.length;
    }
    return messages;
  });

const snapshot = (sql: SqlClient.SqlClient, id: StreamId) =>
  query<SnapshotDatabaseRow>(
    sql,
    "changes",
    "SELECT current_offset,closed,soft_deleted FROM streamsy_streams WHERE stream_id=?",
    [id],
  ).pipe(
    Effect.flatMap((rows) =>
      decodeSnapshotRow(rows[0] ?? { current_offset: null, closed: null, soft_deleted: null }),
    ),
    Effect.map(
      (row): ChangeSnapshot => ({
        present: row.current_offset !== null,
        currentOffset: row.current_offset ?? ZERO_OFFSET,
        closed: row.closed === 1,
        softDeleted: row.soft_deleted === 1,
      }),
    ),
  );

const makeStorage = (sql: SqlClient.SqlClient, boundary: BoundaryRuntime) =>
  Storage.of({
    capabilities: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
    record: Effect.fn("SqlStorage.record")((id) => readRecord(sql, id)),
    messages: Effect.fn("SqlStorage.messages")((id, window) => readMessages(sql, id, window)),
    producer: Effect.fn("SqlStorage.producer")((id, producerId) =>
      query<ProducerDatabaseRow>(
        sql,
        "producer",
        "SELECT epoch,last_seq FROM streamsy_producers WHERE stream_id=? AND producer_id=?",
        [id, producerId],
      ).pipe(
        Effect.flatMap((rows) =>
          rows[0] === undefined
            ? Effect.succeed(Option.none<ProducerState>())
            : decodeProducerRow(rows[0]).pipe(
                Effect.map(({ epoch, last_seq: lastSeq }) => Option.some({ epoch, lastSeq })),
              ),
        ),
      ),
    ),
    mutate: Effect.fn("SqlStorage.mutate")((mutation) => {
      const ids = mutation.operations.map((operation) =>
        Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId,
      );
      if (ids.length === 0 || new Set(ids).size !== ids.length)
        return Effect.die(new Error("Mutation requires distinct streams within atomicScope"));
      return boundary
        .mutation({
          keys: [STORAGE_KEY],
          effect: applyMutation(sql, mutation),
          committed: Predicate.isTagged("Applied"),
          retryable: (error) => error.retryable,
        })
        .pipe(
          Effect.catchIf(isSqlError, (error) => Effect.fail(sqlFault("mutate.transaction", error))),
        );
    }),
    changes: (id): Stream.Stream<ChangeSnapshot, StorageFault> =>
      boundary.changes({ keys: [STORAGE_KEY], read: snapshot(sql, id) }),
    nextExpiry: query<ExpiryDatabaseRow>(
      sql,
      "nextExpiry",
      "SELECT expires_at_ms at,stream_id FROM streamsy_streams " +
        "WHERE expires_at_ms IS NOT NULL AND soft_deleted=0 " +
        "ORDER BY expires_at_ms,stream_id COLLATE BINARY LIMIT 1",
    ).pipe(
      Effect.flatMap((rows) =>
        rows[0] === undefined
          ? Effect.succeed(Option.none<{ readonly at: number; readonly streamId: StreamId }>())
          : decodeExpiryRow(rows[0]).pipe(
              Effect.map(({ at, stream_id: streamId }) => Option.some({ at, streamId })),
            ),
      ),
    ),
  });

const makeLayer = (
  options: SqlStorageOptions,
  probe?: BoundaryTestProbe,
  transactionMaxOpsBeforeYield?: number,
) => {
  const transactionRetry = transactionRetryPolicy(options);
  const services = Layer.effectContext(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migrate(
        sql,
        {
          onTransactionAttempt:
            probe === undefined
              ? undefined
              : () => {
                  probe.migrationAttempts += 1;
                },
        },
        transactionRetry,
      );
      const boundary = yield* BoundaryRuntimeService;
      return Context.empty().pipe(
        Context.add(Storage, makeStorage(sql, boundary)),
        Context.add(CommitBoundary, CommitBoundary.of(boundary)),
      );
    }),
  );
  return services.pipe(
    Layer.provide(
      boundaryLayer(
        options.repairIntervalMs ?? DEFAULT_REPAIR_INTERVAL_MS,
        transactionRetry,
        probe,
        transactionMaxOpsBeforeYield,
      ),
    ),
  );
};

/** Generic SQLite-family SQL layer. Hosts supply one matching SqlClient/Reactivity graph. */
export const layer = (
  options: SqlStorageOptions = {},
  transactionMaxOpsBeforeYield?: number,
): Layer.Layer<
  Storage | CommitBoundary,
  StorageFault,
  SqlClient.SqlClient | Reactivity.Reactivity
> => makeLayer(options, undefined, transactionMaxOpsBeforeYield);

/** Internal test seam; deliberately absent from the package export map. */
export const layerWithTestProbe = (options: SqlStorageOptions, probe: BoundaryTestProbe) =>
  makeLayer(options, probe);
