import { CommitBoundary } from "@streamsy/core";
import { Effect } from "effect";
import { SqlClient } from "effect/sql";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";

/** Persist the identity of this index. Rebuilds must rotate it before exposing new outputs. */
export const feedEpoch = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS folder_sync_epoch (id INTEGER PRIMARY KEY, epoch TEXT NOT NULL)`;
  yield* sql`INSERT INTO folder_sync_epoch (id, epoch) VALUES (1, lower(hex(randomblob(16)))) ON CONFLICT DO NOTHING`;
  const rows = yield* sql<{ epoch: string }>`SELECT epoch FROM folder_sync_epoch WHERE id = 1`;

  if (rows[0] === undefined) return yield* Effect.die("Missing feed epoch");

  return rows[0].epoch;
});

export const rotateFeedEpoch = Effect.gen(function* () {
  const boundary = yield* CommitBoundary;

  return yield* boundary.withTransaction(
    Effect.gen(function* () {
      yield* feedEpoch;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE folder_sync_epoch SET epoch = lower(hex(randomblob(16))) WHERE id = 1`;

      return yield* feedEpoch;
    }),
  );
});

export const EpochHeaders = HttpRouter.middleware(
  (handler) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;

      if (!request.url.split("?")[0]?.endsWith("/streams/changes")) return yield* handler;
      const boundary = yield* CommitBoundary;
      const epoch = yield* boundary.withTransaction(feedEpoch).pipe(Effect.orDie);

      return HttpServerResponse.setHeader(yield* handler, "stream-epoch", epoch);
    }),
  { global: true },
);
