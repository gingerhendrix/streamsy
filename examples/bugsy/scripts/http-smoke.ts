import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Console, Effect, Layer, Record, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import {
  check,
  getJson,
  postCommand,
  readBoard,
  scratchDatabase,
  serverDatabase,
  startServer,
} from "./support.ts";

const Status = Schema.Struct({ offsets: Schema.Record(Schema.String, Schema.String) });

const smoke = Effect.gen(function* () {
  const storage = yield* serverDatabase;
  // Used only when no database URL is set.
  const database = yield* scratchDatabase("streamsy-issue-smoke");
  const server = yield* startServer(database);
  const baseUrl = server.url;
  yield* postCommand(baseUrl, "live", {
    type: "create",
    commandId: "smoke-1",
    issueId: "smoke",
    projectId: "streamsy",
    title: "Smoke",
    status: "todo",
  });
  const before = yield* getJson(`${baseUrl}/api/workspaces/live/status`, Status);
  yield* check(Record.keys(before.offsets).length === 5, "checkpoint does not have five inputs");
  yield* check((yield* readBoard(baseUrl, "live")).rows.length === 1, "first row missing");
  yield* server.stop;
  yield* startServer(database, server.port);
  const resumed = yield* getJson(`${baseUrl}/api/workspaces/live/status`, Status);
  yield* check(Schema.toEquivalence(Status)(before, resumed), "checkpoint changed on restart");
  yield* postCommand(baseUrl, "live", {
    type: "status",
    commandId: "smoke-2",
    issueId: "smoke",
    status: "done",
  });
  yield* check(
    (yield* readBoard(baseUrl, "live")).rows[0]?.status === "done",
    "second command missing",
  );
  yield* Console.log(`smoke:http ok on ${storage}: checkpoint and row resumed, then advanced once`);
});

smoke.pipe(
  Effect.scoped,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);
