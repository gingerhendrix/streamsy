import { BunRuntime, BunServices } from "@effect/platform-bun";
import { DurableState, StreamAddress, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Array, Console, Effect, Layer, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/http";
import { IssueEvent } from "../domain/issue.ts";
import { LabelCountRow, WorkspaceSummary } from "../domain/outputs.ts";
import { applicationLayer, createInputs, sqliteStorage } from "../server/host.ts";
import { tracker } from "../server/outputs.ts";
import { Labels, LabelEvents } from "../server/streams.ts";
import {
  check,
  getJson,
  postCommand,
  readBoard,
  scratchDatabase,
  SmokeFailure,
  startServer,
} from "./support.ts";

/** The label-counts State page: its changes and any State controls. */
const LabelCountChanges = Schema.Array(
  Schema.Union([
    StreamAddress.stateChange({ type: "labelCounts", schema: LabelCountRow }),
    StreamAddress.stateControl,
  ]),
);

/** Writes labels and a membership, and runs the outputs before the server starts. */
const produce = Effect.gen(function* () {
  yield* createInputs("live");
  const ref = Labels.at({ workspaceId: "live" });

  const label = {
    workspaceId: "live",
    labelId: "keep",
    name: "Keep",
    color: "#123456",
    updatedAt: "2026-09-23T10:00:00Z",
  };

  const removed = { ...label, labelId: "remove", name: "Remove" };
  yield* (yield* Labels).append(
    { workspaceId: "live" },
    DurableState.changes(ref, { offset: ZERO_OFFSET }, [
      DurableState.upsert("label", label),
      DurableState.upsert("label", removed),
    ]),
  );
  yield* Projection.run(tracker.at({ workspaceId: "live" }));
  yield* (yield* Labels).append({ workspaceId: "live" }, [
    { type: "label", key: removed.labelId, headers: { operation: "delete" } },
  ]);
  yield* (yield* LabelEvents).append({ workspaceId: "live" }, [
    {
      type: "LabelAttached",
      workspaceId: "live",
      issueId: "smoke",
      labelId: "keep",
      membershipId: "smoke.keep",
      eventId: "label-1",
      sequence: 0,
      occurredAt: label.updatedAt,
    },
  ]);
  yield* Projection.run(tracker.at({ workspaceId: "live" }));
});

const smoke = Effect.gen(function* () {
  const database = yield* scratchDatabase("streamsy-issue-serve");
  // The producer closes its SQLite connection before the server opens the file.
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- The producer layer must close here.
  yield* produce.pipe(Effect.provide(applicationLayer(sqliteStorage(database))));
  const server = yield* startServer(database);
  const baseUrl = server.url;
  yield* postCommand(baseUrl, "live", {
    type: "create",
    commandId: "serve-1",
    issueId: "smoke",
    projectId: "p1",
    title: "Served",
    status: "todo",
  });
  yield* postCommand(baseUrl, "live", {
    type: "status",
    commandId: "serve-2",
    issueId: "smoke",
    status: "done",
  });

  const verify = (afterRestartCommand = false) =>
    Effect.gen(function* () {
      const status = afterRestartCommand ? "todo" : "done";
      const board = yield* readBoard(baseUrl, "live");
      yield* check(
        board.rows.length === 1 && board.rows[0]?.status === status,
        "board rows missing",
      );
      yield* check(Array.join(board.rows[0]?.labelIds ?? [], ",") === "keep", "membership missing");

      // This small fixture fits label-counts and transitions in one protocol page.
      const changes = yield* getJson(
        `${baseUrl}/state/workspaces/live/label-counts?offset=-1`,
        LabelCountChanges,
      );

      yield* check(
        Array.some(
          changes,
          (change) =>
            StreamAddress.isChange(change) &&
            change.key === "remove" &&
            change.headers.operation === "delete" &&
            !("old_value" in change),
        ),
        "key-only output delete missing",
      );
      const counts = new Map<string, LabelCountRow>();

      for (const change of changes) {
        if (StreamAddress.isControl(change)) {
          if (change.headers.control === "reset") counts.clear();
        } else if ("value" in change) counts.set(change.key, change.value);
        else counts.delete(change.key);
      }

      yield* check(counts.size === 1 && counts.get("keep")?.count === 1, "label counts incorrect");

      const transitions = yield* getJson(
        `${baseUrl}/feed/workspaces/live/issue-transitions?offset=-1`,
        Schema.Array(IssueEvent),
      );

      yield* check(
        Array.join(
          Array.map(transitions, (event) => event.eventId),
          ",",
        ) === (afterRestartCommand ? "serve-1,serve-2,serve-3" : "serve-1,serve-2"),
        "transitions duplicated or missing",
      );
      const response = yield* HttpClient.get(`${baseUrl}/workspaces/live/summary`);
      yield* check(response.status === 200, "summary unavailable");
      const summary = yield* HttpClientResponse.schemaBodyJson(WorkspaceSummary)(response);
      yield* check(
        summary.issueCount === 1 && summary.doneCount === (afterRestartCommand ? 0 : 1),
        "summary incorrect",
      );
      const etag = response.headers["etag"];

      if (etag === undefined) return yield* new SmokeFailure({ message: "summary has no ETag" });

      const revalidated = yield* HttpClient.get(`${baseUrl}/workspaces/live/summary`, {
        headers: { "if-none-match": etag },
      });

      yield* check(revalidated.status === 304, "summary revalidation failed");

      return etag;
    });

  const etag = yield* verify();
  yield* server.stop;
  yield* startServer(database, server.port);
  yield* check((yield* verify()) === etag, "summary changed on restart");
  yield* postCommand(baseUrl, "live", {
    type: "status",
    commandId: "serve-3",
    issueId: "smoke",
    status: "todo",
  });
  yield* check((yield* verify(true)) !== etag, "summary ETag did not advance");

  const advanced = yield* HttpClient.get(`${baseUrl}/workspaces/live/summary`, {
    headers: { "if-none-match": etag },
  });

  yield* check(
    advanced.status === 200 && advanced.headers["etag"] !== etag,
    "old ETag incorrectly revalidated",
  );
  yield* Console.log(
    "smoke:serve ok: rows, key-only delete, transitions, ETag/304 and SQLite restart",
  );
});

smoke.pipe(
  Effect.scoped,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);
