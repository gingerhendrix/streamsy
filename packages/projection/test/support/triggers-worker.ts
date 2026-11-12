/* oxlint-disable effecttsgo/async-function -- Native Worker and Durable Object entry points. */
import type {
  DurableObjectNamespace,
  DurableObjectState,
  Request as WorkerRequest,
} from "@cloudflare/workers-types";
import { Effect, Layer, Option, Schema } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { DurableState, Offset, StorageFault, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import {
  CheckpointStore,
  Commit,
  Projection,
  ProjectionWakeup,
  Transaction,
} from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { OwnerRecords, ProjectionMarks } from "@streamsy/core/internal";
import { SqlClient } from "effect/sql";

class Files extends Streamsy.json<Files>()("TriggersWorker/Files", "files/:folderId", {
  params: { folderId: Schema.String },
  schema: Schema.String,
}) {}

class Wake extends Streamsy.wake<Wake>()("TriggersWorker/Wake", "wake", { params: {} }) {}

const projection = Projection.process({
  id: "trigger-index",
  params: { folderId: Schema.String },
  inputs: { files: Files },
  commit: Commit.transaction,
  process: (_batch, unit) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO runs (folder, count) VALUES (${unit.params.folderId}, 1) ON CONFLICT(folder) DO UPDATE SET count = count + 1`;
    }),
});

const initialise = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE IF NOT EXISTS runs (folder TEXT PRIMARY KEY, count INTEGER NOT NULL)`;
    const files = yield* Files;

    for (const folderId of ["docs", "later"]) {
      const exists = yield* files.head({ folderId }).pipe(Effect.option);

      if (Option.isNone(exists)) {
        yield* files.create({ folderId });
        yield* files.append({ folderId }, [folderId]);
      }
    }

    yield* (yield* Wake).create({});
  }).pipe(Effect.orDie),
);

const layer = (state: DurableObjectState) =>
  Layer.mergeAll(
    initialise,
    CheckpointStore.layer,
    Transaction.layer,
    Projection.Registry.layer,
  ).pipe(
    Layer.provideMerge(
      DurableObjectStorage.layer(Streamsy.group(Files, Wake), {
        client: { storage: state.storage },
      }),
    ),
  );

const requestRun = Effect.gen(function* () {
  const { folderId = "" } = yield* HttpRouter.params;
  const stream = Files.at({ folderId }).id;
  yield* (yield* Wake).append(
    {},
    DurableState.changes(Wake.address({}), {}, [
      DurableState.upsert("stream", { stream, offset: Offset.make(ZERO_OFFSET) }),
    ]),
  );

  return HttpServerResponse.text("requested");
}).pipe(Effect.orDie);

const app = Layer.mergeAll(
  HttpRouter.add("POST", "/request/:folderId", requestRun),
  // GET intentionally skips host reconciliation, modelling eviction before the alarm is armed.
  HttpRouter.add("GET", "/quiet/:folderId", requestRun),
  HttpRouter.add(
    "GET",
    "/atomic/:after",
    Effect.gen(function* () {
      const { after } = yield* HttpRouter.params;
      const records = yield* OwnerRecords;
      let cursorKey = "";

      const runner = yield* ProjectionWakeup.attach(projection, { triggers: [Wake] }).pipe(
        Effect.provideService(OwnerRecords, {
          ...records,
          write: (key, value) =>
            Effect.gen(function* () {
              cursorKey = key;

              if (after === "true") yield* records.write(key, value);

              return yield* new StorageFault({
                operation: "test.cursor",
                message: "injected",
                retryable: true,
              });
            }),
        }),
      );

      const result = yield* runner.run.pipe(Effect.result);
      const cursor = yield* records.read(cursorKey);
      const marks = yield* (yield* ProjectionMarks).read(projection.id, ['{"folderId":"docs"}']);

      return HttpServerResponse.jsonUnsafe({
        failed: result._tag === "Failure",
        cursor: Option.getOrNull(cursor),
        marks: marks.size,
      });
    }).pipe(Effect.scoped, Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/status",
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const runs = yield* sql<{
        folder: string;
        count: number;
      }>`SELECT folder, count FROM runs ORDER BY folder`;

      return HttpServerResponse.jsonUnsafe({
        runs,
        runners: yield* (yield* Projection.Registry).status,
      });
    }).pipe(Effect.orDie),
  ),
);

interface Env {
  readonly INDEX: DurableObjectNamespace;
}

export class TriggerObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof layer>>
>({
  app,
  layer,
  wakeups: [ProjectionWakeup.attach(projection, { triggers: [Wake] })],
}) {}

export default {
  async fetch(request: WorkerRequest, env: Env) {
    return env.INDEX.get(env.INDEX.idFromName("index")).fetch(request);
  },
};
