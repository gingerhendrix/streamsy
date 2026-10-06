import { expect, test } from "bun:test";
import { Projection } from "@streamsy/projection";
import { Effect, ManagedRuntime } from "effect";
import { SqlClient } from "effect/sql";
import { foldIssue, type IssueEvent } from "../domain/issue.ts";
import { applicationLayer, createInputs, sqliteStorage } from "../server/host.ts";
import { issueRows } from "../server/projection.ts";
import { Events } from "../server/streams.ts";
import { transact, UnknownIssue } from "../server/state.ts";
import { scratchDirectory, startServer, stopServer, waitForServer } from "./support.ts";

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

test("transact decides from the issueRows tables and appends at the events offset", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(sqliteStorage(":memory:")));

  try {
    await runtime.runPromise(
      Effect.gen(function* () {
        yield* createInputs("live");
        const events = yield* Events;
        // A fact appended outside `transact`: the next command must see it.
        yield* events.append({ workspaceId: "live" }, [created]);

        const first = yield* transact("live", {
          type: "status",
          commandId: "c1",
          issueId: "i1",
          status: "done",
        });

        const assigned = yield* transact("live", {
          type: "assign",
          commandId: "c2",
          issueId: "i1",
          assigneeId: "ada",
        });

        expect(first.event).toMatchObject({ type: "IssueStatusChanged", sequence: 4 });
        expect(assigned.event).toMatchObject({ status: "done", assigneeId: "ada", sequence: 5 });
        expect(assigned.offset).toBe((yield* events.head({ workspaceId: "live" })).nextOffset);

        const unknown = yield* Effect.flip(
          transact("live", { type: "status", commandId: "c3", issueId: "nope", status: "done" }),
        );

        expect(unknown).toBeInstanceOf(UnknownIssue);
        const { items } = yield* events.collect({ workspaceId: "live" });
        expect(items.map((item) => item.eventId)).toEqual(["e1", "c1", "c2"]);
      }),
    );
  } finally {
    await runtime.dispose();
  }
});

test("the change trigger stays alive beside the request trigger on one key", async () => {
  const runtime = ManagedRuntime.make(applicationLayer(sqliteStorage(":memory:"), ["live"]));

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
        yield* Projection.run(issueRows.at({ workspaceId: "live" }), { limit: 5 });
        const sql = yield* SqlClient.SqlClient;
        yield* (yield* Events).append({ workspaceId: "live" }, [
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
        expect(yield* sql.unsafe("SELECT issue_id, status FROM issues ORDER BY issue_id")).toEqual([
          { issue_id: "both", status: "todo" },
          { issue_id: "change-only", status: "todo" },
        ]);
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
    const response = await fetch(`${baseUrl}/api/workspaces/missing/status`);
    expect(response.status).toBe(404);

    for (const path of [
      "/state/workspaces/missing/issues",
      "/state/workspaces/missing/label-counts",
      "/feed/workspaces/missing/issue-transitions",
    ])
      expect((await fetch(`${baseUrl}${path}?offset=-1`)).status).toBe(404);
    expect((await fetch(`${baseUrl}/workspaces/missing/summary`)).status).toBe(200);
    expect((await fetch(`${baseUrl}/api/workspaces/live/commands`)).status).toBe(404);
    const stream = await fetch(`${baseUrl}/streams/bugsy/missing/issue-events?offset=-1`);
    expect(stream.status).toBe(404);
  } finally {
    await stopServer(server);
    await scratch.remove();
  }
});
