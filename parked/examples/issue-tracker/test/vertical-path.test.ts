/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is the Effect application behind the host's Web `fetch` handler. */
/**
 * The whole executable path, on memory layers.
 *
 * One command becomes one canonical fact, the declaration maintains one keyed
 * row from it, and the sink publishes that row as a Durable State product. The
 * same declaration runs here with an in-memory log and an in-memory store — the
 * SQLite suite runs it again with neither.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { CommandResponse, HealthResponse, IssuesResponse, SeedResponse } from "../shared/api.ts";
import { memoryLayer } from "../server/persistence/store.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function fresh(): Host {
  const created = host();
  open.push(created);
  return created;
}

describe("the vertical path on memory layers", () => {
  test("health reports the declaration's own plan identity", async () => {
    const health = await json(await call(fresh(), "GET", "/health"), HealthResponse);
    expect(health.view).toBe("issue-tracker.issues");
    expect(health.planHash).toMatch(/^[0-9a-f]{8}$/);
  });

  test("a created issue becomes one maintained row in the declared column", async () => {
    const instance = fresh();
    const created = await json(
      await call(
        instance,
        "POST",
        "/api/workspaces/main/issues",
        createIssueBody("cmd-1", "issue-1", "Declare the view", "todo"),
      ),
      CommandResponse,
    );
    expect(created.reconciled).toBe(false);
    expect(created.row?.status).toBe("todo");
    expect(created.maintenance.folded).toBe(1);

    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows.map((row) => row.issueId)).toEqual(["issue-1"]);
  });

  test("moving an issue updates the same row rather than adding one", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Move me"),
    );
    const moved = await json(
      await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
        commandId: "cmd-2",
        status: "done",
      }),
      CommandResponse,
    );
    expect(moved.row?.status).toBe("done");
    expect(moved.maintenance.changed).toBe(1);

    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows).toHaveLength(1);
    expect(listed.rows[0]?.status).toBe("done");
  });

  test("a repeated commandId appends nothing and reports the original acceptance", async () => {
    const instance = fresh();
    const body = createIssueBody("cmd-1", "issue-1", "Only once");
    const first = await json(
      await call(instance, "POST", "/api/workspaces/main/issues", body),
      CommandResponse,
    );
    const retried = await call(instance, "POST", "/api/workspaces/main/issues", body);
    expect(retried.status).toBe(200);
    const second = await json(retried, CommandResponse);

    expect(second.reconciled).toBe(true);
    expect(second.ack.offset).toBe(first.ack.offset);
    expect(second.eventId).toBe(first.eventId);
    expect(second.sequence).toBe(first.sequence);
    // Nothing was folded, so nothing was appended.
    expect(second.maintenance.folded).toBe(0);
    expect(second.maintenance.changed).toBe(0);
    expect(second.maintenance.checkpoint).toBe(first.maintenance.checkpoint);
  });

  test("a repeated move reconciles too, so a retried drag cannot double-transition", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Move once"),
    );
    const move = { commandId: "cmd-move", status: "in_progress" };
    const first = await json(
      await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", move),
      CommandResponse,
    );
    const second = await json(
      await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", move),
      CommandResponse,
    );
    expect(second.reconciled).toBe(true);
    expect(second.ack.offset).toBe(first.ack.offset);
    expect(second.maintenance.folded).toBe(0);
  });

  test("seeding is idempotent and fills all four columns", async () => {
    const instance = fresh();
    const seeded = await json(
      await call(instance, "POST", "/api/workspaces/main/seed"),
      SeedResponse,
    );
    expect(seeded.seeded).toBe(true);
    const again = await json(
      await call(instance, "POST", "/api/workspaces/main/seed"),
      SeedResponse,
    );
    expect(again.seeded).toBe(false);

    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(new Set(listed.rows.map((row) => row.status))).toEqual(
      new Set(["backlog", "todo", "in_progress", "done"]),
    );
  });

  test("moving an unknown issue is a typed 404, and appends nothing", async () => {
    const instance = fresh();
    const response = await call(instance, "POST", "/api/workspaces/main/issues/ghost/status", {
      commandId: "cmd-ghost",
      status: "done",
    });
    expect(response.status).toBe(404);
    expect(await response.json()).toMatchObject({ error: "unknown-issue" });
  });

  test("a malformed body is rejected before anything durable happens", async () => {
    const instance = fresh();
    const response = await call(instance, "POST", "/api/workspaces/main/issues", {
      commandId: "cmd-1",
      issueId: "issue-1",
      projectId: "streamsy",
      title: "Bad status",
      status: "shipped",
    });
    expect(response.status).toBe(400);
    const listed = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(listed.rows).toEqual([]);
  });

  test("a malformed durable row is typed restore poison, never a served row", async () => {
    // The store's preload plants a value that carries the right key and the
    // wrong shape, which is exactly the failure a schema-backed restore exists
    // to catch.
    const instance = host({
      store: memoryLayer({
        preload: { main: { "issue-1": JSON.stringify({ issueId: "issue-1", status: "shipped" }) } },
      }),
    });
    open.push(instance);
    const response = await call(instance, "GET", "/api/workspaces/main/issues");
    expect(response.status).toBe(500);
    expect(await response.json()).toMatchObject({ error: "state-restore-poison" });
  });
});
