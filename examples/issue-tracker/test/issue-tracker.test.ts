import { expect, test } from "bun:test";
import { State, Streams, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Effect, ManagedRuntime } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { foldIssue, type IssueEvent } from "../domain/issue.ts";
import { applicationLayer, createInputs } from "../server/host.ts";
import { issueRows } from "../server/projection.ts";
import { events, refs, userStream } from "../server/streams.ts";
import { transact } from "../server/state.ts";
import { scratchDirectory, startServer, stopServer, waitForServer } from "../scripts/support.ts";

const created: IssueEvent = {
  type: "IssueCreated",
  eventId: "e1",
  workspaceId: "live",
  issueId: "i1",
  projectId: "p1",
  title: "Issue",
  status: "todo",
  sequence: 3,
  occurredAt: "2026-09-20T10:00:00.000Z",
};

test("the fold keeps the newest sequence when an older fact arrives later", () => {
  const first = foldIssue(undefined, created);
  const older: IssueEvent = {
    type: "IssueStatusChanged",
    eventId: "e0",
    workspaceId: "live",
    issueId: "i1",
    status: "backlog",
    sequence: 2,
    occurredAt: "2026-09-20T09:00:00.000Z",
  };
  expect(foldIssue(first, older)).toBe(first);
});

test("the fused handler writes application rows through the SQLite host", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs(refs("live"));
        yield* Streams.append(events.ref({ workspaceId: "live" }), [created]);
        const result = yield* Projection.run(issueRows.member({ workspaceId: "live" }));
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql.unsafe<{ readonly status: string }>("SELECT status FROM issues");
        expect(result.items).toBe(1);
        expect(rows).toEqual([{ status: "todo" }]);
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("the change trigger stays alive beside the request trigger on one key", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:", ["live"]));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* transact("live", {
          type: "create",
          commandId: "both-1",
          issueId: "both",
          projectId: "p1",
          title: "Both",
          status: "todo",
        });
        yield* Projection.serialized(issueRows.member({ workspaceId: "live" }), { limit: 5 });
        const sql = yield* SqlClient.SqlClient;
        yield* Streams.append(events.ref({ workspaceId: "live" }), [
          { ...created, eventId: "change-2", issueId: "change-only", sequence: 1 },
        ]);
        let count = 0;
        for (let attempt = 0; attempt < 20 && count !== 2; attempt++) {
          const rows = yield* sql.unsafe<{ readonly n: number }>(
            "SELECT COUNT(*) AS n FROM issues",
          );
          count = rows[0]?.n ?? 0;
          if (count !== 2) yield* Effect.sleep("10 millis");
        }
        expect(count).toBe(2);
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("catalog state upserts and deletes a row", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(":memory:"));
  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs(refs("live"));
        const ref = userStream("live");
        const ada = {
          userId: "ada",
          workspaceId: "live",
          name: "Ada",
          updatedAt: "2026-09-20T10:00:00.000Z",
        };
        yield* Streams.append(
          ref,
          State.changes(ref, { offset: ZERO_OFFSET }, [State.upsert("user", ada)]),
        );
        yield* Projection.run(issueRows.member({ workspaceId: "live" }));
        const sql = yield* SqlClient.SqlClient;
        const afterUpsert = yield* sql.unsafe<{ readonly n: number }>(
          "SELECT COUNT(*) AS n FROM users",
        );
        expect(afterUpsert[0]?.n).toBe(1);
        yield* Streams.append(
          ref,
          State.changes(ref, { offset: "1" }, [State.delete("user", ada)]),
        );
        yield* Projection.run(issueRows.member({ workspaceId: "live" }));
        const afterDelete = yield* sql.unsafe<{ readonly n: number }>(
          "SELECT COUNT(*) AS n FROM users",
        );
        expect(afterDelete[0]?.n).toBe(0);
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("an unknown workspace is a 404 and creates nothing", async () => {
  const scratch = await scratchDirectory("streamsy-issue-test");
  const port = 20_000 + Math.floor(Math.random() * 20_000);
  const server = startServer(port, scratch.database);
  try {
    const baseUrl = `http://127.0.0.1:${port}`;
    await waitForServer(baseUrl);
    const response = await fetch(`${baseUrl}/api/workspaces/missing/issues`);
    expect(response.status).toBe(404);
    expect((await fetch(`${baseUrl}/api/workspaces/live/commands`)).status).toBe(404);
    const stream = await fetch(`${baseUrl}/streams/issue-tracker/missing/issue-events?offset=-1`);
    expect(stream.status).toBe(404);
  } finally {
    await stopServer(server);
    await scratch.remove();
  }
});
