import { attachOwner, ownerOf } from "@streamsy/core/internal/memory";
import { Context, Effect, Layer, Option, Predicate, Schema, type Stream } from "effect";
import {
  type Streamsy,
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
  type MutationApplied,
  MutationRejected,
  Mutations,
  OperationResults,
  type OperationResult,
  type RecordPatch,
} from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
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
// `CASE` gives a `number` on every dialect today. Postgres gives `boolean` for a bare
// `EXISTS` and `bigint` for an int8 result, so accept those shapes too.
const DependentsRow = Schema.Struct({ present: Schema.Literals([0, 1, 0n, 1n, false, true]) });
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
type DependentsDatabaseRow = typeof DependentsRow.Encoded;

const decodeRecordRow = (row: RecordDatabaseRow) =>
  Schema.decodeEffect(RecordRow)(row).pipe(Effect.orDie);
const decodeProducerRow = (row: ProducerDatabaseRow) =>
  Schema.decodeEffect(ProducerRow)(row).pipe(Effect.orDie);
const decodeExpiryRow = (row: ExpiryDatabaseRow) =>
  Schema.decodeEffect(ExpiryRow)(row).pipe(Effect.orDie);
const decodeSnapshotRow = (row: SnapshotDatabaseRow) =>
  Schema.decodeEffect(SnapshotRow)(row).pipe(Effect.orDie);
const decodeDependentsRow = (row: DependentsDatabaseRow) =>
  Schema.decodeEffect(DependentsRow)(row).pipe(Effect.orDie);

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

// A concurrent create of the same id has no row to lock, so the loser meets the
// primary key. Retrying runs preflight again, which then rejects with `exists`.
const createFault = (error: SqlError): StorageFault =>
  Predicate.isTagged(error.reason, "UniqueViolation")
    ? new StorageFault({
        operation: "mutate.create-record",
        message: error.message,
        retryable: true,
        cause: error,
      })
    : sqlFault("mutate.create-record", error);

const attempt = <A>(operation: string, effect: Effect.Effect<A, SqlError>) =>
  effect.pipe(Effect.mapError((error) => sqlFault(operation, error)));

const execute = (operation: string, statement: Effect.Effect<unknown, SqlError>) =>
  attempt(operation, statement).pipe(Effect.asVoid);

/** MySQL keys hold at most this many characters, so every dialect enforces it. */
export const MAX_STREAM_ID_LENGTH = 512;

const streamIdTooLong = (id: StreamId): Option.Option<StorageFault> => {
  // MySQL VARCHAR counts code points, which is what the spread yields.
  // oxlint-disable-next-line typescript/no-misused-spread -- code points are the intended unit
  const length = [...id].length;
  return length > MAX_STREAM_ID_LENGTH
    ? Option.some(
        new StorageFault({
          operation: "mutate.stream-id",
          message: `Stream id has ${length} characters; the limit is ${MAX_STREAM_ID_LENGTH}`,
          retryable: false,
        }),
      )
    : Option.none();
};

/**
 * Serialise writers on the stream rows. This must be the first statement of a
 * mutation transaction: on MySQL at REPEATABLE READ, any plain read before it
 * fixes the snapshot, and later plain reads miss commits made while waiting.
 * Sorted ids give every writer one lock order. SQLite already has one writer.
 */
const lockStreams = (sql: SqlClient.SqlClient, ids: ReadonlyArray<StreamId>) => {
  const sorted = [...new Set(ids)].toSorted();
  return sql.onDialectOrElse({
    sqlite: () => Effect.void,
    orElse: () =>
      execute(
        "mutate.lock",
        sql`SELECT stream_id FROM streamsy_streams WHERE stream_id IN ${sql.in(sorted)}
 ORDER BY stream_id FOR UPDATE`,
      ),
  });
};

const lockIds = (mutation: Mutation): ReadonlyArray<StreamId> =>
  mutation.operations.flatMap((operation) =>
    Predicate.isTagged(operation, "Create")
      ? operation.forkSource === undefined
        ? [operation.record.id]
        : [operation.record.id, operation.forkSource.id]
      : [operation.streamId],
  );

const recordColumns = (sql: SqlClient.SqlClient) =>
  sql.literal(
    "stream_id,content_type,ttl_seconds,expires_at,created_at,current_offset,last_seq," +
      "closed,closed_at,forked_from,fork_offset,fork_sub_offset,soft_deleted,expires_at_ms",
  );

const readRecord = (sql: SqlClient.SqlClient, id: StreamId) =>
  attempt(
    "record",
    sql<RecordDatabaseRow>`SELECT ${recordColumns(sql)} FROM streamsy_streams WHERE stream_id = ${id}`,
  ).pipe(
    Effect.flatMap((rows) => {
      const row = rows[0];
      return row === undefined
        ? Effect.succeed(Option.none<StreamRecord>())
        : decodeRecordRow(row).pipe(Effect.map((decoded) => Option.some(toRecord(decoded))));
    }),
  );

const recordValues = (record: StreamRecord) => ({
  content_type: record.config.contentType,
  ttl_seconds: record.config.ttlSeconds ?? null,
  expires_at: record.config.expiresAt ?? null,
  created_at: record.config.createdAt,
  current_offset: record.currentOffset,
  last_seq: record.lifecycle.lastSeq ?? null,
  closed: record.lifecycle.closed ? 1 : 0,
  closed_at: record.lifecycle.closedAt ?? null,
  forked_from: record.lifecycle.forkedFrom ?? null,
  fork_offset: record.lifecycle.forkOffset ?? null,
  fork_sub_offset: record.lifecycle.forkSubOffset ?? null,
  soft_deleted: record.lifecycle.softDeleted ? 1 : 0,
  expires_at_ms: record.lifecycle.expiresAtMs ?? null,
});

const insertRecord = (sql: SqlClient.SqlClient, record: StreamRecord) =>
  sql`INSERT INTO streamsy_streams ${sql.insert({ stream_id: record.id, ...recordValues(record) })}`.pipe(
    Effect.mapError(createFault),
    Effect.asVoid,
  );

const updateRecord = (sql: SqlClient.SqlClient, record: StreamRecord) =>
  execute(
    "mutate.update-record",
    sql`UPDATE streamsy_streams SET ${sql.update(recordValues(record))} WHERE stream_id = ${record.id}`,
  );

const insertMessage = (
  sql: SqlClient.SqlClient,
  operation: string,
  id: StreamId,
  message: StoredMessage,
) =>
  execute(
    operation,
    sql`INSERT INTO streamsy_messages ${sql.insert({
      stream_id: id,
      msg_offset: message.offset,
      timestamp: message.timestamp,
      data: new Uint8Array(message.data),
    })}`,
  );

const readProducer = (
  sql: SqlClient.SqlClient,
  operation: string,
  id: StreamId,
  producerId: string,
) =>
  attempt(
    operation,
    sql<ProducerDatabaseRow>`SELECT epoch, last_seq FROM streamsy_producers
 WHERE stream_id = ${id} AND producer_id = ${producerId}`,
  ).pipe(
    Effect.flatMap((rows) =>
      rows[0] === undefined
        ? Effect.succeed(Option.none<ProducerState>())
        : decodeProducerRow(rows[0]).pipe(
            Effect.map(({ epoch, last_seq: lastSeq }) => Option.some({ epoch, lastSeq })),
          ),
    ),
  );

const upsertProducer = (
  sql: SqlClient.SqlClient,
  id: StreamId,
  producerId: string,
  next: ProducerState,
) => {
  const row = sql.insert({
    stream_id: id,
    producer_id: producerId,
    epoch: next.epoch,
    last_seq: next.lastSeq,
  });
  return execute(
    "mutate.producer",
    sql.onDialectOrElse({
      mysql: () =>
        sql`INSERT INTO streamsy_producers ${row}
 ON DUPLICATE KEY UPDATE epoch = VALUES(epoch), last_seq = VALUES(last_seq)`,
      orElse: () =>
        sql`INSERT INTO streamsy_producers ${row}
 ON CONFLICT (stream_id, producer_id) DO UPDATE SET epoch = excluded.epoch, last_seq = excluded.last_seq`,
    }),
  );
};

const preflight = (
  sql: SqlClient.SqlClient,
  mutation: Mutation,
): Effect.Effect<void, MutationRejected | StorageFault> =>
  Effect.gen(function* () {
    for (const [index, operation] of mutation.operations.entries()) {
      const id = Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId;
      const current = yield* readRecord(sql, id);
      if (Predicate.isTagged(operation, "Create")) {
        if (Option.isSome(current))
          return yield* new MutationRejected({ index, reason: "exists", record: current });
        if (operation.forkSource !== undefined) {
          const source = yield* readRecord(sql, operation.forkSource.id);
          if (
            Option.isNone(source) ||
            source.value.lifecycle.softDeleted ||
            source.value.currentOffset < operation.forkSource.liveAtOffset
          )
            return yield* new MutationRejected({
              index,
              reason: "fork-source-gone",
              record: Option.none(),
            });
        }
        continue;
      }
      if (Option.isNone(current))
        return yield* new MutationRejected({ index, reason: "not-found", record: Option.none() });
      const record = current.value;
      if (record.lifecycle.softDeleted)
        return yield* new MutationRejected({ index, reason: "gone", record: current });
      if (Predicate.isTagged(operation, "Delete")) {
        if (
          operation.reason === "expiry" &&
          (operation.expectedExpiresAtMs === undefined ||
            operation.expectedExpiresAtMs !== record.lifecycle.expiresAtMs)
        )
          return yield* new MutationRejected({ index, reason: "expiry-mismatch", record: current });
        continue;
      }
      if (
        operation.expectedOffset !== undefined &&
        operation.expectedOffset !== record.currentOffset
      )
        return yield* new MutationRejected({ index, reason: "offset", record: current });
      if (
        operation.expectedClosed !== undefined &&
        operation.expectedClosed !== record.lifecycle.closed
      )
        return yield* new MutationRejected({ index, reason: "closed", record: current });
      if (operation.producer !== undefined) {
        const actual = yield* readProducer(
          sql,
          "mutate.preflight-producer",
          id,
          operation.producer.producerId,
        );
        const expected = operation.producer.expected;
        if (
          Option.isSome(actual) !== Option.isSome(expected) ||
          (Option.isSome(actual) &&
            Option.isSome(expected) &&
            (actual.value.epoch !== expected.value.epoch ||
              actual.value.lastSeq !== expected.value.lastSeq))
        )
          return yield* new MutationRejected({ index, reason: "producer", record: current });
      }
    }
    return undefined;
  });

const hasDependents = (sql: SqlClient.SqlClient, id: StreamId) =>
  attempt(
    "mutate.dependents",
    sql<DependentsDatabaseRow>`SELECT CASE WHEN EXISTS(
 SELECT 1 FROM streamsy_streams WHERE forked_from = ${id}
) THEN 1 ELSE 0 END AS present`,
  ).pipe(
    Effect.flatMap((rows) => decodeDependentsRow(rows[0] ?? { present: 0 })),
    Effect.map(({ present }) => present === 1 || present === 1n || present === true),
  );

const purge = (sql: SqlClient.SqlClient, start: StreamRecord) =>
  Effect.gen(function* () {
    let current: StreamRecord | undefined = start;
    while (current !== undefined) {
      const parentId: StreamId | undefined = current.lifecycle.forkedFrom;
      // Keep cleanup explicit instead of depending on a host connection PRAGMA.
      yield* execute(
        "mutate.purge-messages",
        sql`DELETE FROM streamsy_messages WHERE stream_id = ${current.id}`,
      );
      yield* execute(
        "mutate.purge-producers",
        sql`DELETE FROM streamsy_producers WHERE stream_id = ${current.id}`,
      );
      yield* execute(
        "mutate.purge",
        sql`DELETE FROM streamsy_streams WHERE stream_id = ${current.id}`,
      );
      if (parentId === undefined) return;
      // Two children of one soft-deleted parent can purge at once. Each waits here
      // for the other, so the last one sees no dependents and removes the parent.
      yield* lockStreams(sql, [parentId]);
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
): Effect.Effect<MutationApplied, MutationRejected | StorageFault> =>
  Effect.gen(function* () {
    yield* lockStreams(sql, lockIds(mutation));
    yield* preflight(sql, mutation);
    const records = new Map<StreamId, StreamRecord>();
    const results: Array<OperationResult> = [];

    // Creation first preserves a fork edge when source deletion appears first.
    for (const operation of mutation.operations) {
      if (!Predicate.isTagged(operation, "Create")) continue;
      yield* insertRecord(sql, operation.record);
      for (const message of operation.initialMessages)
        yield* insertMessage(sql, "mutate.create-message", operation.record.id, message);
      records.set(operation.record.id, operation.record);
    }

    for (const operation of mutation.operations) {
      const id = Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId;
      const current = records.get(id) ?? Option.getOrThrow(yield* readRecord(sql, id));
      if (Predicate.isTagged(operation, "Create")) {
        results.push(OperationResults.Created({ record: copyRecord(current) }));
        continue;
      }
      if (Predicate.isTagged(operation, "Append")) {
        for (const message of operation.messages)
          yield* insertMessage(sql, "mutate.append-message", id, message);
        const next = patchRecord(current, operation.patch);
        yield* updateRecord(sql, next);
        if (operation.producer !== undefined)
          yield* upsertProducer(sql, id, operation.producer.producerId, operation.producer.next);
        records.set(id, next);
        results.push(OperationResults.Appended({ record: next }));
        continue;
      }
      if (yield* hasDependents(sql, id)) {
        const next = patchRecord(current, { lifecycle: { softDeleted: true } });
        yield* updateRecord(sql, next);
        results.push(OperationResults.SoftDeleted({ record: next }));
      } else {
        results.push(OperationResults.Purged({ record: current }));
        yield* purge(sql, current);
      }
    }
    const [first, ...rest] = results;
    if (first === undefined) return yield* Effect.die(new Error("Empty mutation"));
    return Mutations.Applied({ results: [first, ...rest] });
  });

interface MessageDatabaseRow {
  readonly msg_offset: unknown;
  readonly timestamp: unknown;
  readonly data: unknown;
}

const directMessage = (row: MessageDatabaseRow): StoredMessage => {
  if (
    !Predicate.isString(row.msg_offset) ||
    !/^\d{16}_\d{16}$/.test(row.msg_offset) ||
    !Predicate.isNumber(row.timestamp) ||
    !Number.isFinite(row.timestamp) ||
    !Predicate.isUint8Array(row.data)
  )
    throw new Error("Corrupt persisted message row");
  return {
    offset: Offset.make(row.msg_offset),
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
      const predicates = [sql`stream_id = ${segment.id}`];
      if (window.after !== undefined) predicates.push(sql`msg_offset > ${window.after}`);
      if (upper !== undefined) predicates.push(sql`msg_offset <= ${upper}`);
      const rows = yield* attempt(
        "messages",
        sql<MessageDatabaseRow>`SELECT msg_offset, timestamp, data FROM streamsy_messages
 WHERE ${sql.and(predicates)} ORDER BY msg_offset${
   remaining === undefined ? sql.literal("") : sql` LIMIT ${remaining}`
 }`,
      );
      for (const row of rows)
        messages.push(yield* Effect.try(() => directMessage(row)).pipe(Effect.orDie));
      if (remaining !== undefined) remaining -= rows.length;
    }
    return messages;
  });

const snapshot = (sql: SqlClient.SqlClient, id: StreamId) =>
  attempt(
    "changes",
    sql<SnapshotDatabaseRow>`SELECT current_offset, closed, soft_deleted FROM streamsy_streams
 WHERE stream_id = ${id}`,
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
      readProducer(sql, "producer", id, producerId),
    ),
    mutate: Effect.fn("SqlStorage.mutate")((mutation) => {
      const ids = mutation.operations.map((operation) =>
        Predicate.isTagged(operation, "Create") ? operation.record.id : operation.streamId,
      );
      if (ids.length === 0 || new Set(ids).size !== ids.length)
        return Effect.die(new Error("Mutation requires distinct streams within atomicScope"));
      for (const operation of mutation.operations) {
        if (!Predicate.isTagged(operation, "Create")) continue;
        const tooLong = streamIdTooLong(operation.record.id);
        if (Option.isSome(tooLong)) return Effect.fail(tooLong.value);
      }
      return boundary
        .mutation({
          keys: [STORAGE_KEY],
          effect: applyMutation(sql, mutation),
          retryable: (error) => Predicate.isTagged(error, "StorageFault") && error.retryable,
        })
        .pipe(
          Effect.catchIf(isSqlError, (error) => Effect.fail(sqlFault("mutate.transaction", error))),
        );
    }),
    changes: (id): Stream.Stream<ChangeSnapshot, StorageFault> =>
      boundary.changes({ keys: [STORAGE_KEY], read: snapshot(sql, id) }),
    nextExpiry: attempt(
      "nextExpiry",
      sql<ExpiryDatabaseRow>`SELECT expires_at_ms AS at, stream_id FROM streamsy_streams
 WHERE expires_at_ms IS NOT NULL AND soft_deleted = 0
 ORDER BY expires_at_ms, stream_id LIMIT 1`,
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
        Context.add(Storage, attachOwner(makeStorage(sql, boundary), ownerOf(boundary))),
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

/** Generic SQLite-stream SQL layer. Hosts supply one matching SqlClient/Reactivity graph. */
export const storageLayer = (
  options: SqlStorageOptions = {},
): Layer.Layer<
  Storage | CommitBoundary,
  StorageFault,
  SqlClient.SqlClient | Reactivity.Reactivity
> => makeLayer(options);

/** Internal host seam for the official Durable Object transaction boundary. */
export const layerWithDurableObjectTransactions = (options: SqlStorageOptions) =>
  makeLayer(options, undefined, Number.MAX_SAFE_INTEGER);

/** Internal test seam; deliberately absent from the package export map. */
export const layerWithTestProbe = (options: SqlStorageOptions, probe: BoundaryTestProbe) =>
  makeLayer(options, probe);

/** Protocol over SQL storage. */
export const layerProtocol = (options: SqlStorageOptions & Protocol.ProtocolOptions = {}) =>
  Protocol.layer(options).pipe(Layer.provideMerge(storageLayer(options)));

export function layer<const Keys extends ReadonlyArray<Streamsy.Any>>(
  options: SqlStorageOptions & Protocol.ProtocolOptions,
  group: Streamsy.Group<Keys>,
): Layer.Layer<
  | Layer.Success<ReturnType<typeof storageLayer>>
  | Streamsy.Services<Streamsy.Group<Keys>>
  | Streamsy.ChangeFeed<Streamsy.Services<Streamsy.Group<Keys>>>,
  Layer.Error<ReturnType<typeof storageLayer>>,
  Layer.Services<ReturnType<typeof storageLayer>>
>;
export function layer(
  options: SqlStorageOptions & Protocol.ProtocolOptions = {},
  group: Streamsy.Group,
) {
  const storage = storageLayer(options);
  const protocol = Protocol.layer(options).pipe(Layer.provideMerge(storage));
  return Protocol.bind(protocol, group).pipe(Layer.provideMerge(storage));
}
