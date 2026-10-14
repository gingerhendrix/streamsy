import { CommitBoundary, Streamsy, StorageFault } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import { defineActionSink } from "@streamsy/serve/action";
import { OutboxStore } from "@streamsy/serve/action/outbox";
import { createSqlOutboxBacking, migrateOutbox } from "@streamsy/serve/action/sql";
import { draftsFor, actionSinkHandler, OutboxWakeup } from "@streamsy/serve/action/runtime";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { FileOp, Identifier, Version, overwriteMarkFromOp } from "../server/domain.ts";
import { Changes, Change } from "../server/streams.ts";
import { WriteHook } from "../server/folder-store.ts";
import type { Env } from "./env.ts";

export const Notice = Schema.Struct({ folderId: Identifier, version: Version, operation: FileOp });

export type Notice = typeof Notice.Type;

export class Notices extends Streamsy.json<Notices>()("FolderSync/Notices", "notices", {
  params: {},
  schema: Notice,
}) {}

export const notices = defineActionSink<Notice, { readonly key: "folderId" }>({
  name: "folder-sync.notice",
  from: { key: "folderId" },
  handler: { name: "folder-sync.index", version: 1 },
  payload: { encode: JSON.stringify, decode: Schema.decodeUnknownSync(Notice) },
  idempotencyKey: ({ folderId, version }) => `${folderId}/${version}`,
  partitionBy: ({ folderId }) => folderId,
  delivery: { maxAttempts: 1000000, initialBackoffMs: 100, backoffFactor: 2, maxBackoffMs: 30000 },
});

export const NoticeHook = Layer.effect(
  WriteHook,
  Effect.gen(function* () {
    const store = yield* OutboxStore;

    return WriteHook.of({
      committed: (folderId, operation, version) =>
        Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) =>
            store.enqueue(draftsFor(notices, [{ folderId, operation, version }], now)),
          ),
          Effect.asVoid,
          Effect.mapError(
            (cause) =>
              new StorageFault({
                operation: "notice.enqueue",
                message: String(cause),
                retryable: true,
              }),
          ),
        ),
    });
  }),
);

export const OutboxLive = Layer.effect(
  OutboxStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migrateOutbox(sql);

    return OutboxStore.of(createSqlOutboxBacking(sql));
  }),
);

export class NoticeTransportFailure extends Schema.TaggedError<NoticeTransportFailure>()(
  "NoticeTransportFailure",
  {},
) {}

/** The Worker's route to the index object. Built from `env`, so the static wakeup can use it. */
export class NoticeTransport extends Context.Service<
  NoticeTransport,
  { readonly send: (body: string) => Effect.Effect<Response, NoticeTransportFailure> }
>()("FolderSync/NoticeTransport") {}

export const noticeTransportLayer = (env: Env) =>
  Layer.succeed(
    NoticeTransport,
    NoticeTransport.of({
      send: (body) =>
        Effect.tryPromise({
          try: () =>
            env.INDEX.get(env.INDEX.idFromName("index")).fetch("https://internal/__notice", {
              method: "POST",
              body,
            }),
          catch: () => new NoticeTransportFailure(),
        }),
    }),
  );

const noticeHandler = actionSinkHandler(notices, (delivery, refuse) =>
  Effect.gen(function* () {
    const transport = yield* NoticeTransport;

    const response = yield* transport.send(notices.payload.encode(delivery.payload)).pipe(
      Effect.mapError(() => refuse.retryable("Index transport unavailable")),
      Effect.timeoutOrElse({
        duration: "2 seconds",
        orElse: () => Effect.fail(refuse.retryable("Index transport timed out")),
      }),
    );

    if (response.status !== 200)
      return yield* refuse.retryable(`Index answered ${response.status}`);

    return yield* Effect.void;
  }),
);

/**
 * Delivers notices from the folder object's alarm. A write arms the alarm in its
 * mutation finalizer. The ordered drain stops behind a retrying notice, so the
 * index sees each folder's commits in order.
 */
export const noticeWakeup = OutboxWakeup.make(notices, noticeHandler, { ordered: true });

export const noticeTables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS folder_sync_notices (folderId TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY (folderId, version))`;
  yield* sql`CREATE TABLE IF NOT EXISTS folder_sync_index_heads (folderId TEXT NOT NULL, path TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY (folderId, path))`;
});

/** Receiver dedup and authoritative inbox append share the index owner's transaction. */
export const receiveNotice = (notice: Notice) =>
  Effect.gen(function* () {
    const boundary = yield* CommitBoundary;
    const sql = yield* SqlClient.SqlClient;
    const inbox = yield* Notices;
    yield* boundary.withTransaction(
      Effect.gen(function* () {
        const inserted =
          yield* sql`INSERT INTO folder_sync_notices (folderId, version) VALUES (${notice.folderId}, ${notice.version}) ON CONFLICT DO NOTHING RETURNING version`;

        if (inserted.length > 0) yield* inbox.append({}, [notice]);
      }),
    );
  });

/** Late retries cannot move a path backward. Tombstones retain their version in this table. */
export const noticeChanges = Projection.process({
  id: "notice-changes",
  params: {},
  inputs: { input: Notices },
  commit: Commit.transaction,
  process: Projection.each(({ item }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const stream = yield* Changes;
      const { folderId, version, operation } = item;

      const rows = yield* sql<{
        version: number;
      }>`SELECT version FROM folder_sync_index_heads WHERE folderId = ${folderId} AND path = ${operation.path}`;

      if (rows[0] !== undefined && rows[0].version >= version) return;

      const metadata = {
        folderId,
        version,
        path: operation.path,
        size: operation.type === "put" ? operation.size : 0,
        deleted: operation.type === "delete",
        author: operation.author,
        updatedAt: operation.updatedAt ?? "1970-01-01T00:00:00.000Z",
        ...overwriteMarkFromOp(operation),
      };

      const change: Change =
        operation.type === "delete"
          ? metadata
          : {
              ...metadata,
              hash: operation.hash,
              ...("inline" in operation ? { inline: operation.inline } : { blob: operation.blob }),
            };

      yield* stream.append({}, [change]);
      yield* sql`INSERT INTO folder_sync_index_heads (folderId, path, version) VALUES (${folderId}, ${operation.path}, ${version}) ON CONFLICT (folderId, path) DO UPDATE SET version = excluded.version`;
    }),
  ),
});
