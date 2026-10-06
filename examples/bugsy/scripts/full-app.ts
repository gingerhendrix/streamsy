import { BunRuntime, BunServices } from "@effect/platform-bun";
import { DurableState, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Array, Console, Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { SqlClient } from "effect/sql";
import recording from "./recordings/acme.json";
import { IssueEvent, IssueLabelEvent } from "../domain/issue.ts";
import { applicationLayer, createInputs, sqliteStorage } from "../server/host.ts";
import { issueRows } from "../server/projection.ts";
import { Projects, Users, Labels, Events, LabelEvents } from "../server/streams.ts";
import {
  check,
  getJson,
  post,
  readBoard,
  scratchDatabase,
  SmokeFailure,
  startServer,
} from "./support.ts";

const Rows = Schema.Struct({ rows: Schema.Array(Schema.Unknown) });

/** Fails when a recorded catalog is empty, because `append` needs at least one item. */
const nonEmpty = <A>(rows: ReadonlyArray<A>) =>
  Array.isReadonlyArrayNonEmpty(rows)
    ? Effect.succeed(rows)
    : Effect.fail(new SmokeFailure({ message: "The recording has an empty catalog" }));

/** Loads the recording and the catalog, then checks the SQL rows of the catalog. */
const produce = Effect.gen(function* () {
  yield* createInputs("acme");
  yield* (yield* Events).append(
    { workspaceId: "acme" },
    yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(IssueEvent))(recording.issueEvents),
  );
  yield* (yield* LabelEvents).append(
    { workspaceId: "acme" },
    yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(IssueLabelEvent))(
      recording.issueLabelEvents,
    ),
  );
  const projectRef = Projects.at({ workspaceId: "acme" });
  const userRef = Users.at({ workspaceId: "acme" });
  const labelRef = Labels.at({ workspaceId: "acme" });
  yield* (yield* Projects).append(
    { workspaceId: "acme" },
    DurableState.changes(
      projectRef,
      { offset: ZERO_OFFSET },
      Array.map(yield* nonEmpty(recording.catalog.projects), (row) =>
        DurableState.upsert("project", row),
      ),
    ),
  );
  yield* (yield* Users).append(
    { workspaceId: "acme" },
    DurableState.changes(
      userRef,
      { offset: ZERO_OFFSET },
      Array.map(yield* nonEmpty(recording.catalog.users), (row) =>
        DurableState.upsert("user", row),
      ),
    ),
  );
  yield* (yield* Labels).append(
    { workspaceId: "acme" },
    DurableState.changes(
      labelRef,
      { offset: ZERO_OFFSET },
      Array.map(yield* nonEmpty(recording.catalog.labels), (row) =>
        DurableState.upsert("label", row),
      ),
    ),
  );
  yield* Projection.run(issueRows.at({ workspaceId: "acme" }));
  const sql = yield* SqlClient.SqlClient;
  for (const [table, expected] of [
    ["projects", 1],
    ["users", 2],
    ["labels", 2],
  ] as const) {
    const rows = yield* sql<{ readonly n: number }>`SELECT COUNT(*) AS n FROM ${sql(table)}`;
    yield* check(rows[0]?.n === expected, `expected ${expected} ${table}, got ${rows[0]?.n}`);
  }
});

const app = Effect.gen(function* () {
  const database = yield* scratchDatabase("streamsy-issue-full");
  // The producer closes its SQLite connection before the server opens the file.
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- The producer layer must close here.
  yield* produce.pipe(Effect.provide(applicationLayer(sqliteStorage(database))));

  const server = yield* startServer(database);
  const baseUrl = server.url;
  const acme = yield* readBoard(baseUrl, "acme");
  yield* check(
    acme.rows.find((row) => row.issueId === "acme-1")?.status === "done",
    "older sequence overwrote acme-1",
  );
  yield* check(
    acme.rows.find((row) => row.issueId === "acme-2")?.assigneeId === "grace",
    "assignment missing",
  );
  yield* check(
    acme.rows.find((row) => row.issueId === "acme-2")?.labelIds.length === 2,
    "labels missing",
  );
  yield* check(
    acme.rows.find((row) => row.issueId === "acme-1")?.labelIds.length === 0,
    "detached label remained on acme-1",
  );
  const changes = yield* getJson(
    `${baseUrl}/feed/workspaces/acme/issue-transitions?offset=-1`,
    Schema.Array(Schema.Unknown),
  );
  const drafts = yield* getJson(`${baseUrl}/api/workspaces/acme/drafts`, Rows);
  yield* check(changes.length === 4, `expected 4 accepted changes, got ${changes.length}`);
  yield* check(drafts.rows.length === 1, `expected one draft, got ${drafts.rows.length}`);

  yield* post(`${baseUrl}/api/workspaces/live/commands`, {
    type: "create",
    commandId: "live-1",
    issueId: "live-issue",
    projectId: "streamsy",
    title: "Live issue",
    status: "todo",
  });
  yield* post(`${baseUrl}/api/workspaces/live/commands`, {
    type: "status",
    commandId: "live-2",
    issueId: "live-issue",
    status: "done",
  });
  yield* check(
    (yield* readBoard(baseUrl, "live")).rows[0]?.status === "done",
    "live commands were not projected",
  );

  yield* server.stop;
  yield* startServer(database, server.port);
  yield* check(
    (yield* readBoard(baseUrl, "acme")).rows.length === 2,
    "recorded rows did not survive restart",
  );
  yield* check(
    (yield* readBoard(baseUrl, "live")).rows[0]?.status === "done",
    "live row did not survive restart",
  );
  yield* check(
    (yield* getJson(`${baseUrl}/api/workspaces/acme/drafts`, Rows)).rows.length === 1,
    "draft did not survive restart",
  );
  yield* Console.log("app:full ok: recording, live commands, sequence law, and restart resume");
});

app.pipe(
  Effect.scoped,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);
