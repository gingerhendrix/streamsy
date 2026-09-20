import { State, Streams, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Effect, ManagedRuntime, Schema } from "effect";
import recording from "./recordings/acme.json";
import { IssueEvent, IssueLabelEvent } from "../domain/issue.ts";
import { applicationLayer, createInputs } from "../server/host.ts";
import { issueRows } from "../server/projection.ts";
import {
  events,
  labelEvents,
  labelStream,
  projectStream,
  refs,
  userStream,
} from "../server/streams.ts";
import {
  post,
  requestJson,
  scratchDirectory,
  startServer,
  stopServer,
  waitForServer,
} from "./support.ts";

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
const scratch = await scratchDirectory("streamsy-issue-full");
const port = 20_000 + Math.floor(Math.random() * 20_000);
const baseUrl = `http://127.0.0.1:${port}`;

try {
  const producer = ManagedRuntime.make(applicationLayer(scratch.database));
  await producer.runPromise(
    Effect.gen(function* () {
      yield* createInputs(refs("acme"));
      yield* Streams.append(
        events.ref({ workspaceId: "acme" }),
        Schema.decodeUnknownSync(Schema.Array(IssueEvent))(recording.issueEvents),
      );
      yield* Streams.append(
        labelEvents.ref({ workspaceId: "acme" }),
        Schema.decodeUnknownSync(Schema.Array(IssueLabelEvent))(recording.issueLabelEvents),
      );
      const projectRef = projectStream("acme");
      const userRef = userStream("acme");
      const labelRef = labelStream("acme");
      yield* Streams.append(
        projectRef,
        State.changes(
          projectRef,
          { offset: ZERO_OFFSET },
          recording.catalog.projects.map((row) => State.upsert("project", row)),
        ),
      );
      yield* Streams.append(
        userRef,
        State.changes(
          userRef,
          { offset: ZERO_OFFSET },
          recording.catalog.users.map((row) => State.upsert("user", row)),
        ),
      );
      yield* Streams.append(
        labelRef,
        State.changes(
          labelRef,
          { offset: ZERO_OFFSET },
          recording.catalog.labels.map((row) => State.upsert("label", row)),
        ),
      );
      yield* Projection.run(issueRows.member({ workspaceId: "acme" }));
    }),
  );
  await producer.dispose();

  let server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  const acme = await requestJson<{
    rows: Array<{ issueId: string; status: string; assigneeId?: string; labelIds: string[] }>;
  }>(baseUrl, "/api/workspaces/acme/issues");
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
  const changes = await requestJson<{ rows: unknown[] }>(baseUrl, "/api/workspaces/acme/changes");
  const drafts = await requestJson<{ rows: unknown[] }>(baseUrl, "/api/workspaces/acme/drafts");
  assert(changes.rows.length === 4, `expected 4 accepted changes, got ${changes.rows.length}`);
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
    (await requestJson<{ rows: Array<{ status: string }> }>(baseUrl, "/api/workspaces/live/issues"))
      .rows[0]?.status === "done",
    "live commands were not projected",
  );

  await stopServer(server);
  server = startServer(port, scratch.database);
  await waitForServer(baseUrl);
  assert(
    (await requestJson<{ rows: unknown[] }>(baseUrl, "/api/workspaces/acme/issues")).rows.length ===
      2,
    "recorded rows did not survive restart",
  );
  assert(
    (await requestJson<{ rows: Array<{ status: string }> }>(baseUrl, "/api/workspaces/live/issues"))
      .rows[0]?.status === "done",
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
