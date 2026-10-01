import { DurableState, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Array, Effect, ManagedRuntime, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";
import recording from "./recordings/acme.json";
import { IssueEvent, IssueLabelEvent } from "../domain/issue.ts";
import { applicationLayer, createInputs } from "../server/host.ts";
import { issueRows } from "../server/projection.ts";
import { Projects, Users, Labels, Events, LabelEvents } from "../server/streams.ts";
import {
  post,
  readBoard,
  requestJson,
  scratchDirectory,
  startServer,
  stopServer,
  waitForServer,
} from "./support.ts";

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
function nonEmpty<A>(rows: ReadonlyArray<A>): Array.NonEmptyReadonlyArray<A> {
  assert(Array.isReadonlyArrayNonEmpty(rows), "The recording has an empty catalog");
  return rows;
}
const scratch = await scratchDirectory("streamsy-issue-full");
const port = 20_000 + Math.floor(Math.random() * 20_000);
const baseUrl = `http://127.0.0.1:${port}`;

try {
  const producer = ManagedRuntime.make(applicationLayer(scratch.database));
  await producer.runPromise(
    Effect.gen(function* () {
      yield* createInputs("acme");
      yield* (yield* Events).append(
        { workspaceId: "acme" },
        Schema.decodeUnknownSync(Schema.NonEmptyArray(IssueEvent))(recording.issueEvents),
      );
      yield* (yield* LabelEvents).append(
        { workspaceId: "acme" },
        Schema.decodeUnknownSync(Schema.NonEmptyArray(IssueLabelEvent))(recording.issueLabelEvents),
      );
      const projectRef = Projects.at({ workspaceId: "acme" });
      const userRef = Users.at({ workspaceId: "acme" });
      const labelRef = Labels.at({ workspaceId: "acme" });
      yield* (yield* Projects).append(
        { workspaceId: "acme" },
        DurableState.changes(
          projectRef,
          { offset: ZERO_OFFSET },
          Array.map(nonEmpty(recording.catalog.projects), (row) =>
            DurableState.upsert("project", row),
          ),
        ),
      );
      yield* (yield* Users).append(
        { workspaceId: "acme" },
        DurableState.changes(
          userRef,
          { offset: ZERO_OFFSET },
          Array.map(nonEmpty(recording.catalog.users), (row) => DurableState.upsert("user", row)),
        ),
      );
      yield* (yield* Labels).append(
        { workspaceId: "acme" },
        DurableState.changes(
          labelRef,
          { offset: ZERO_OFFSET },
          Array.map(nonEmpty(recording.catalog.labels), (row) => DurableState.upsert("label", row)),
        ),
      );
      yield* Projection.run(issueRows.at({ workspaceId: "acme" }));
      const sql = yield* SqlClient.SqlClient;
      for (const [table, expected] of [
        ["projects", 1],
        ["users", 2],
        ["labels", 2],
      ] as const) {
        const rows = yield* sql.unsafe<{ readonly n: number }>(
          `SELECT COUNT(*) AS n FROM ${table}`,
        );
        assert(rows[0]?.n === expected, `expected ${expected} ${table}, got ${rows[0]?.n}`);
      }
    }),
  );
  await producer.dispose();

  let server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  const acme = await readBoard(baseUrl, "acme");
  assert(
    acme.rows.find((row) => row.issueId === "acme-1")?.status === "done",
    "older sequence overwrote acme-1",
  );
  assert(
    acme.rows.find((row) => row.issueId === "acme-2")?.assigneeId === "grace",
    "assignment missing",
  );
  assert(
    acme.rows.find((row) => row.issueId === "acme-2")?.labelIds.length === 2,
    "labels missing",
  );
  assert(
    acme.rows.find((row) => row.issueId === "acme-1")?.labelIds.length === 0,
    "detached label remained on acme-1",
  );
  const changes = await requestJson<unknown[]>(
    baseUrl,
    "/feed/workspaces/acme/issue-transitions?offset=-1",
  );
  const drafts = await requestJson<{ rows: unknown[] }>(baseUrl, "/api/workspaces/acme/drafts");
  assert(changes.length === 4, `expected 4 accepted changes, got ${changes.length}`);
  assert(drafts.rows.length === 1, `expected one draft, got ${drafts.rows.length}`);

  await post(baseUrl, "/api/workspaces/live/commands", {
    type: "create",
    commandId: "live-1",
    issueId: "live-issue",
    projectId: "streamsy",
    title: "Live issue",
    status: "todo",
  });
  await post(baseUrl, "/api/workspaces/live/commands", {
    type: "status",
    commandId: "live-2",
    issueId: "live-issue",
    status: "done",
  });
  assert(
    (await readBoard(baseUrl, "live")).rows[0]?.status === "done",
    "live commands were not projected",
  );

  await stopServer(server);
  server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  assert(
    (await readBoard(baseUrl, "acme")).rows.length === 2,
    "recorded rows did not survive restart",
  );
  assert(
    (await readBoard(baseUrl, "live")).rows[0]?.status === "done",
    "live row did not survive restart",
  );
  assert(
    (await requestJson<{ rows: unknown[] }>(baseUrl, "/api/workspaces/acme/drafts")).rows.length ===
      1,
    "draft did not survive restart",
  );
  await stopServer(server);
  console.log("app:full ok: recording, live commands, sequence law, and restart resume");
} finally {
  await scratch.remove();
}
