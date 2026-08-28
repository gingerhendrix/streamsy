/* oxlint-disable effecttsgo/async-function, effecttsgo/global-date, effecttsgo/global-timers, effecttsgo/new-promise -- bun:test owns Promise-native workerd timing. */
import { afterEach, describe, expect, test } from "bun:test";
import { Schema } from "effect";
import { WorkspaceSummary } from "../domain/issue.ts";
import {
  ApiError,
  CommandResponse,
  DrainResponse,
  HealthResponse,
  IssuesResponse,
  NotificationsResponse,
  TransitionFeedResponse,
} from "../shared/api.ts";
import {
  decodeResponse,
  jsonRequest,
  workerdHarness,
  type WorkerdHarness,
} from "./cloudflare-support.ts";

const open: WorkerdHarness[] = [];
const AlarmControlResponse = Schema.Struct({ alarm: Schema.NullOr(Schema.Finite) });
afterEach(async () => {
  await Promise.all(open.splice(0).map((harness) => harness.close()));
});

async function fresh(): Promise<WorkerdHarness> {
  const harness = await workerdHarness();
  open.push(harness);
  return harness;
}

async function workspaceControl(
  harness: WorkerdHarness,
  workspaceId: string,
  query: string,
): Promise<{ alarm: number | null }> {
  const namespace = await harness.mf.getDurableObjectNamespace("WORKSPACES");
  const stub = namespace.get(namespace.idFromName(`workspace:${workspaceId}`));
  const response = await stub.fetch(`http://workspace.internal/_streamsy/maintenance?${query}`, {
    headers: { "x-streamsy-partition-key": `workspace:${workspaceId}` },
  });
  if (!response.ok) throw new Error(`workspace control failed: ${await response.text()}`);
  return Schema.decodeUnknownSync(AlarmControlResponse)(await response.json());
}

async function eventually(assertion: () => Promise<boolean>, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await assertion())) {
    if (Date.now() >= deadline) throw new Error(`condition not met within ${timeoutMs}ms`);
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

const create = (commandId: string, issueId: string, title: string) => ({
  commandId,
  issueId,
  projectId: "streamsy",
  title,
  status: "todo",
});

describe("Cloudflare workspace placement on real workerd storage", () => {
  test("clones immutable asset binding responses before propagating request ids", async () => {
    const harness = await workerdHarness("test/cloudflare-assets-worker.ts");
    open.push(harness);
    const response = await harness.fetch("/app.js", {
      headers: { "x-request-id": "asset-request" },
    });
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("immutable-asset");
    expect(response.headers.get("content-type")).toContain("text/plain");
    expect(response.headers.get("x-request-id")).toBe("asset-request");
  });

  test("gateway owns health, typed unavailable domains, and request ids", async () => {
    const harness = await fresh();
    const health = await harness.fetch("/health", { headers: { "x-request-id": "request-1" } });
    expect(health.status).toBe(200);
    expect(health.headers.get("x-request-id")).toBe("request-1");
    expect(await decodeResponse(health, HealthResponse)).toMatchObject({
      status: "ok",
      deployment: "workerd-test",
    });

    const user = await harness.fetch("/api/users/ada/inbox");
    expect(user.status).toBe(503);
    expect(await decodeResponse(user, ApiError)).toMatchObject({
      error: "domain-placement-unavailable",
    });
    const global = await harness.fetch("/api/global/exchange");
    expect(global.status).toBe(503);
    expect(await decodeResponse(global, ApiError)).toMatchObject({
      error: "domain-placement-unavailable",
    });
  });

  test("runs the vertical path and resumes checked products after actor wake", async () => {
    const harness = await fresh();
    expect((await harness.fetch("/api/workspaces/main/seed", jsonRequest("POST"))).status).toBe(
      200,
    );
    const created = await decodeResponse(
      await harness.fetch(
        "/api/workspaces/main/issues",
        jsonRequest("POST", create("cmd-created", "issue-cf", "Cloudflare issue")),
      ),
      CommandResponse,
    );
    const originalOffset = created.ack.offset;
    expect(created.row?.status).toBe("todo");
    expect(
      (
        await harness.fetch(
          "/api/workspaces/main/issues/issue-cf/assignee",
          jsonRequest("POST", { commandId: "cmd-assign", assigneeId: "ada" }),
        )
      ).status,
    ).toBe(200);

    const boardBefore = await harness.fetch("/state/workspaces/main/issues");
    expect(boardBefore.status).toBe(200);
    const boardOffset = boardBefore.headers.get("stream-next-offset");
    expect(boardOffset).toBeString();
    const countsBefore = await harness.fetch("/state/workspaces/main/label-counts");
    expect(countsBefore.status).toBe(200);
    const countOffset = countsBefore.headers.get("stream-next-offset");
    expect(countOffset).toBeString();

    const alarmPending = await decodeResponse(
      await harness.fetch("/api/workspaces/main/notifications"),
      NotificationsResponse,
    );
    expect(alarmPending.pending + alarmPending.delivered).toBe(1);
    expect(alarmPending.dead).toBe(0);
    await harness.runWorkspaceMaintenance("main");
    const alarmDelivered = await decodeResponse(
      await harness.fetch("/api/workspaces/main/notifications"),
      NotificationsResponse,
    );
    expect(alarmDelivered).toMatchObject({ pending: 0, delivered: 1, dead: 0 });

    await harness.evictWorkspace("main");

    const duplicate = await decodeResponse(
      await harness.fetch(
        "/api/workspaces/main/issues",
        jsonRequest("POST", create("cmd-created", "issue-cf", "Cloudflare issue")),
      ),
      CommandResponse,
    );
    expect(duplicate.reconciled).toBe(true);
    expect(duplicate.ack.offset).toBe(originalOffset);

    expect(
      (
        await harness.fetch(
          "/api/workspaces/main/issues/issue-cf/status",
          jsonRequest("POST", { commandId: "cmd-move", status: "done" }),
        )
      ).status,
    ).toBe(200);
    expect(
      (
        await harness.fetch(
          "/api/workspaces/main/issues/issue-cf/labels",
          jsonRequest("POST", { commandId: "cmd-label-on", labelId: "bug" }),
        )
      ).status,
    ).toBe(200);
    expect(
      (
        await harness.fetch(
          "/api/workspaces/main/issues/issue-cf/labels/detach",
          jsonRequest("POST", { commandId: "cmd-label-off", labelId: "bug" }),
        )
      ).status,
    ).toBe(200);

    const boardSuffix = await harness.fetch(
      `/state/workspaces/main/issues?offset=${encodeURIComponent(boardOffset ?? "")}`,
    );
    expect(boardSuffix.status).toBe(200);
    expect(boardSuffix.headers.get("stream-next-offset")).not.toBe(boardOffset);
    const countSuffix = await harness.fetch(
      `/state/workspaces/main/label-counts?offset=${encodeURIComponent(countOffset ?? "")}`,
    );
    expect(countSuffix.status).toBe(200);
    expect(countSuffix.headers.get("stream-next-offset")).not.toBe(countOffset);

    const feed = await decodeResponse(
      await harness.fetch("/feed/workspaces/main/issue-transitions"),
      TransitionFeedResponse,
    );
    expect(feed.events.length).toBeGreaterThan(0);
    const summary = await decodeResponse(
      await harness.fetch("/document/workspaces/main/summary"),
      WorkspaceSummary,
    );
    expect(summary.issues.total).toBe(5);
    const drained = await decodeResponse(
      await harness.fetch("/api/workspaces/main/notifications/drain", jsonRequest("POST")),
      DrainResponse,
    );
    expect(drained).toMatchObject({ workspaceId: "main" });
    const notifications = await decodeResponse(
      await harness.fetch("/api/workspaces/main/notifications"),
      NotificationsResponse,
    );
    expect(notifications.outbox).toHaveLength(1);
  });

  test("distinct canonical object names isolate every workspace-owned table", async () => {
    const harness = await fresh();
    await harness.fetch(
      "/api/workspaces/left/issues",
      jsonRequest("POST", create("shared-command", "left-issue", "Left only")),
    );
    await harness.fetch(
      "/api/workspaces/right/issues",
      jsonRequest("POST", create("shared-command", "right-issue", "Right only")),
    );
    await harness.fetch(
      "/api/workspaces/left/issues/left-issue/assignee",
      jsonRequest("POST", { commandId: "shared-assignment", assigneeId: "ada" }),
    );
    await harness.fetch(
      "/api/workspaces/right/issues/right-issue/assignee",
      jsonRequest("POST", { commandId: "shared-assignment", assigneeId: "ada" }),
    );
    const left = await decodeResponse(
      await harness.fetch("/api/workspaces/left/issues"),
      IssuesResponse,
    );
    const right = await decodeResponse(
      await harness.fetch("/api/workspaces/right/issues"),
      IssuesResponse,
    );
    expect(left.rows.map((row) => row.issueId)).toEqual(["left-issue"]);
    expect(right.rows.map((row) => row.issueId)).toEqual(["right-issue"]);

    const ids = await harness.mf.listDurableObjectIds("WorkspacePartitionObject");
    expect(ids).toHaveLength(2);

    const leftStorage = await harness.mf.unsafeGetDurableObjectStorage(
      "",
      "WorkspacePartitionObject",
      { name: "workspace:left" },
    );
    const rightStorage = await harness.mf.unsafeGetDurableObjectStorage(
      "",
      "WorkspacePartitionObject",
      { name: "workspace:right" },
    );
    expect(
      await leftStorage.exec<{ command_id: string }>(
        "SELECT command_id FROM command_receipts ORDER BY command_id",
      ),
    ).toEqual(
      await rightStorage.exec<{ command_id: string }>(
        "SELECT command_id FROM command_receipts ORDER BY command_id",
      ),
    );
    expect(
      (
        await leftStorage.exec<{ partition_id: string }>(
          "SELECT DISTINCT partition_id FROM streamsy_effect_outbox",
        )
      ).map((row) => row.partition_id),
    ).toEqual(["left"]);
    expect(
      (
        await rightStorage.exec<{ partition_id: string }>(
          "SELECT DISTINCT partition_id FROM streamsy_effect_outbox",
        )
      ).map((row) => row.partition_id),
    ).toEqual(["right"]);
    expect(
      (
        await leftStorage.exec<{ stream_id: string }>(
          "SELECT stream_id FROM issue_tracker_streams ORDER BY stream_id",
        )
      ).every((row) => row.stream_id.includes("workspaces/left")),
    ).toBe(true);
    expect(
      (
        await rightStorage.exec<{ stream_id: string }>(
          "SELECT stream_id FROM issue_tracker_streams ORDER BY stream_id",
        )
      ).every((row) => row.stream_id.includes("workspaces/right")),
    ).toBe(true);
    await harness.evictWorkspace("left");
    expect(
      (await decodeResponse(await harness.fetch("/api/workspaces/right/issues"), IssuesResponse))
        .rows,
    ).toEqual(right.rows);
    expect(
      (await decodeResponse(await harness.fetch("/api/workspaces/left/issues"), IssuesResponse))
        .rows,
    ).toEqual(left.rows);
  });

  test("a pre-armed platform alarm drains an outbox commit after failure and eviction", async () => {
    const harness = await fresh();
    await harness.fetch(
      "/api/workspaces/alarm/issues",
      jsonRequest("POST", create("alarm-create", "alarm-issue", "Alarm issue")),
    );
    const failed = await harness.fetch("/api/workspaces/alarm/issues/alarm-issue/assignee", {
      ...jsonRequest("POST", { commandId: "alarm-assign", assigneeId: "ada" }),
      headers: {
        "content-type": "application/json",
        "x-streamsy-test-failpoint": "after-application-commit",
      },
    });
    expect(failed.status).toBe(503);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:alarm",
    });
    expect(
      await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"),
    ).toEqual([{ state: "pending" }]);
    await harness.evictWorkspace("alarm");

    // No Worker or object fetch follows eviction: workerd's scheduled alarm is
    // the only event able to change this durable row.
    await eventually(
      async () =>
        (await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"))[0]
          ?.state === "delivered",
    );
    expect(
      await storage.exec<{ accepted: number }>(
        "SELECT COUNT(*) accepted FROM issue_tracker_notification_acceptances",
      ),
    ).toEqual([{ accepted: 1 }]);
  });

  test("raw-stream reconciliation preserves a paused application guard", async () => {
    const harness = await fresh();
    await harness.fetch(
      "/api/workspaces/guard-race/issues",
      jsonRequest("POST", create("guard-create", "guard-issue", "Guard race")),
    );
    const application = harness.fetch("/api/workspaces/guard-race/issues/guard-issue/assignee", {
      ...jsonRequest("POST", { commandId: "guard-assign", assigneeId: "ada" }),
      headers: {
        "content-type": "application/json",
        "x-streamsy-test-failpoint": "pause-after-prearm-then-fail",
      },
    });
    const raw = await harness.fetch("/streams/workspaces/guard-race/raw", {
      method: "PUT",
      headers: { "x-streamsy-test-failpoint": "release-paused-application" },
    });
    expect(raw.status).toBe(201);
    expect((await application).status).toBe(503);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:guard-race",
    });
    expect(
      await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"),
    ).toEqual([{ state: "pending" }]);
    await harness.evictWorkspace("guard-race");

    // Only the platform alarm follows eviction; the raw reconciliation was
    // allowed to finish before the application committed and failed.
    await eventually(
      async () =>
        (await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"))[0]
          ?.state === "delivered",
    );
  });

  test("guard phase failures finalize ownership exactly once on the resident actor", async () => {
    const harness = await fresh();
    const cases = [
      { point: "begin-get", ttl: false },
      { point: "begin-set", ttl: false },
      { point: "notification-discovery", ttl: false },
      { point: "reconcile-set", ttl: true },
      { point: "reconcile-delete", ttl: false },
    ] as const;

    for (const entry of cases) {
      const workspace = `guard-${entry.point}`;
      const headers = new Headers({
        "x-streamsy-test-failpoint": `fail-alarm-${entry.point}`,
      });
      if (entry.ttl) headers.set("stream-ttl", "30");
      const failed = await harness.fetch(`/streams/workspaces/${workspace}/failed`, {
        method: "PUT",
        headers,
      });
      expect(failed.status).toBeGreaterThanOrEqual(500);
      const recovered = await harness.fetch(`/streams/workspaces/${workspace}/recovered`, {
        method: "PUT",
      });
      expect(recovered.status).toBe(201);
      expect(recovered.headers.get("x-streamsy-test-guarded-operations")).toBe("0");
    }
  });

  test("a failed guarded reconciliation get cannot underflow the next interleaving", async () => {
    const harness = await fresh();
    const workspace = "guard-reconcile-get";
    await harness.fetch(
      `/api/workspaces/${workspace}/issues`,
      jsonRequest("POST", create("guard-get-create", "guard-get-issue", "Guard get")),
    );
    const application = harness.fetch(
      `/api/workspaces/${workspace}/issues/guard-get-issue/assignee`,
      {
        ...jsonRequest("POST", { commandId: "guard-get-assign", assigneeId: "ada" }),
        headers: {
          "content-type": "application/json",
          "x-streamsy-test-failpoint": "pause-after-prearm-then-fail",
        },
      },
    );
    const failed = await harness.fetch(`/streams/workspaces/${workspace}/failed`, {
      method: "PUT",
      headers: { "x-streamsy-test-failpoint": "fail-alarm-reconcile-get" },
    });
    expect(failed.status).toBeGreaterThanOrEqual(500);
    const release = await harness.fetch(`/streams/workspaces/${workspace}/release`, {
      method: "PUT",
      headers: { "x-streamsy-test-failpoint": "release-paused-application" },
    });
    expect(release.status).toBe(201);
    expect((await application).status).toBe(503);
    const recovered = await harness.fetch(`/streams/workspaces/${workspace}/recovered`, {
      method: "PUT",
    });
    expect(recovered.headers.get("x-streamsy-test-guarded-operations")).toBe("0");

    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: `workspace:${workspace}`,
    });
    expect(
      await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"),
    ).toHaveLength(1);
    await harness.evictWorkspace(workspace);
    await eventually(
      async () =>
        (await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"))[0]
          ?.state === "delivered",
    );
  });

  test("maintenance begin preserves an earlier alarm installed by another phase", async () => {
    const harness = await fresh();
    const workspace = "minimum-alarm";
    await harness.fetch(`/streams/workspaces/${workspace}/initial`, { method: "PUT" });
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: `workspace:${workspace}`,
    });
    const earlier = Date.now() + 950;
    await workspaceControl(harness, workspace, `test-alarm=set&at=${earlier}`);
    await storage.exec(
      "CREATE TABLE IF NOT EXISTS issue_tracker_test_events (name TEXT PRIMARY KEY)",
    );
    const namespace = await harness.mf.getDurableObjectNamespace("WORKSPACES");
    const stub = namespace.get(namespace.idFromName(`workspace:${workspace}`));
    const maintenance = stub.fetch("http://workspace.internal/_streamsy/maintenance", {
      headers: {
        "x-streamsy-partition-key": `workspace:${workspace}`,
        "x-streamsy-test-failpoint": "pause-maintenance-after-begin",
      },
    });
    await eventually(
      async () =>
        (
          await storage.exec<{ present: number }>(
            "SELECT COUNT(*) present FROM issue_tracker_test_events" +
              " WHERE name = 'maintenance-begin-paused'",
          )
        )[0]?.present === 1,
    );
    expect((await workspaceControl(harness, workspace, "test-alarm=get")).alarm).toBe(earlier);
    await workspaceControl(harness, workspace, "test-alarm=release-maintenance-begin");
    await maintenance;
  });

  test("durable notification identity absorbs a post-accept interruption after eviction", async () => {
    const harness = await fresh();
    await harness.fetch(
      "/api/workspaces/notify/issues",
      jsonRequest("POST", create("notify-create", "notify-issue", "Notify issue")),
    );
    await harness.fetch("/api/workspaces/notify/issues/notify-issue/assignee", {
      ...jsonRequest("POST", { commandId: "notify-assign", assigneeId: "ada" }),
      headers: {
        "content-type": "application/json",
        "x-streamsy-test-failpoint": "after-application-commit",
      },
    });
    const interrupted = await harness.fetch("/api/workspaces/notify/notifications/drain", {
      method: "POST",
      headers: { "x-streamsy-test-failpoint": "notification-after-accept" },
    });
    expect(interrupted.status).toBe(503);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:notify",
    });
    expect(
      await storage.exec<{ state: string; attempts: number }>(
        "SELECT state, attempts FROM streamsy_effect_outbox",
      ),
    ).toEqual([{ state: "pending", attempts: 0 }]);
    expect(
      await storage.exec<{ accepted: number }>(
        "SELECT COUNT(*) accepted FROM issue_tracker_notification_acceptances",
      ),
    ).toEqual([{ accepted: 1 }]);
    await harness.evictWorkspace("notify");
    await eventually(
      async () =>
        (await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"))[0]
          ?.state === "delivered",
    );
    expect(
      await storage.exec<{ accepted: number }>(
        "SELECT COUNT(*) accepted FROM issue_tracker_notification_acceptances",
      ),
    ).toEqual([{ accepted: 1 }]);

    await harness.evictWorkspace("notify");
    const visible = await decodeResponse(
      await harness.fetch("/api/workspaces/notify/notifications"),
      NotificationsResponse,
    );
    expect(visible.notified).toHaveLength(1);
    expect(visible.outbox[0]?.state).toBe("delivered");
  });

  test("commits an initial expiry obligation atomically with stream creation", async () => {
    const harness = await fresh();
    const interrupted = await harness.fetch("/streams/workspaces/atomic-ttl/created", {
      method: "PUT",
      headers: {
        "stream-ttl": "1",
        "x-streamsy-test-failpoint": "after-stream-create-commit",
      },
    });
    expect(interrupted.status).toBe(500);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:atomic-ttl",
    });
    expect(
      await storage.exec<{ stream_id: string }>(
        "SELECT stream_id FROM issue_tracker_streams WHERE stream_id = ?",
        "workspaces/atomic-ttl/created",
      ),
    ).toEqual([{ stream_id: "workspaces/atomic-ttl/created" }]);
    expect(
      await storage.exec<{ stream_id: string }>(
        "SELECT stream_id FROM issue_tracker_stream_expiries WHERE stream_id = ?",
        "workspaces/atomic-ttl/created",
      ),
    ).toEqual([{ stream_id: "workspaces/atomic-ttl/created" }]);
    await harness.evictWorkspace("atomic-ttl");
    await eventually(
      async () =>
        (
          await storage.exec<{ present: number }>(
            "SELECT COUNT(*) present FROM issue_tracker_streams",
          )
        )[0]?.present === 0,
    );
  });

  test("a renewal commit wins against a stale due-expiry alarm decision", async () => {
    const harness = await fresh();
    const path = "/streams/workspaces/renew-race/sliding";
    expect(
      (
        await harness.fetch(path, {
          method: "PUT",
          headers: {
            "stream-ttl": "30",
            "x-streamsy-test-failpoint": "pause-next-expiry",
          },
        })
      ).status,
    ).toBe(201);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:renew-race",
    });
    const initial = await storage.exec<{ expires_at_ms: number }>(
      "SELECT expires_at_ms FROM issue_tracker_stream_expiries",
    );
    const forcedDeadline = Date.now() + 250;
    await storage.exec(
      "UPDATE issue_tracker_stream_expiries SET expires_at_ms = ?",
      forcedDeadline,
    );
    await storage.exec(
      "UPDATE issue_tracker_streams" +
        " SET record_json = json_set(record_json, '$.lifecycle.expiresAtMs', ?)",
      forcedDeadline,
    );
    await workspaceControl(harness, "renew-race", `test-alarm=set&at=${forcedDeadline}`);
    const renewed = await harness.fetch(`${path}?offset=now`, {
      headers: { "x-streamsy-test-failpoint": "pause-renewal-until-expiry" },
    });
    expect(renewed.status).toBe(200);
    const after = await storage.exec<{ expires_at_ms: number }>(
      "SELECT expires_at_ms FROM issue_tracker_stream_expiries",
    );
    expect(after[0]?.expires_at_ms).toBeGreaterThan(initial[0]?.expires_at_ms ?? 0);
    expect(
      await storage.exec<{ present: number }>(
        "SELECT COUNT(*) present FROM issue_tracker_streams WHERE stream_id = ?",
        "workspaces/renew-race/sliding",
      ),
    ).toEqual([{ present: 1 }]);
  });

  test("lazy expiry uses the same deadline-conditional delete boundary as alarms", async () => {
    const harness = await fresh();
    const path = "/streams/workspaces/lazy-renew/sliding";
    expect(
      (
        await harness.fetch(path, {
          method: "PUT",
          headers: { "stream-ttl": "30" },
        })
      ).status,
    ).toBe(201);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:lazy-renew",
    });
    await storage.exec(
      "CREATE TABLE IF NOT EXISTS issue_tracker_test_events (name TEXT PRIMARY KEY)",
    );
    const renewal = harness.fetch(`${path}?offset=now`, {
      headers: { "x-streamsy-test-failpoint": "pause-renewal-until-expiry" },
    });
    await eventually(
      async () =>
        (
          await storage.exec<{ present: number }>(
            "SELECT COUNT(*) present FROM issue_tracker_test_events WHERE name = 'renewal-append-paused'",
          )
        )[0]?.present === 1,
    );
    const due = Date.now() - 1;
    await storage.exec("UPDATE issue_tracker_stream_expiries SET expires_at_ms = ?", due);
    await storage.exec(
      "UPDATE issue_tracker_streams" +
        " SET record_json = json_set(record_json, '$.lifecycle.expiresAtMs', ?)",
      due,
    );
    const lazy = harness.fetch(`${path}?offset=now`, {
      headers: { "x-streamsy-test-failpoint": "pause-lazy-expiry-delete" },
    });
    expect((await renewal).status).toBe(200);
    expect((await lazy).status).toBe(200);
    const after = await storage.exec<{ expires_at_ms: number }>(
      "SELECT expires_at_ms FROM issue_tracker_stream_expiries",
    );
    expect(after[0]?.expires_at_ms).toBeGreaterThan(Date.now() + 20_000);
    expect(
      await storage.exec<{ present: number }>(
        "SELECT COUNT(*) present FROM issue_tracker_streams WHERE stream_id = ?",
        "workspaces/lazy-renew/sliding",
      ),
    ).toEqual([{ present: 1 }]);
  });

  test("delayed cancellation cannot erase a recreated stream expiry", async () => {
    const harness = await fresh();
    const path = "/streams/workspaces/cancel-generation/same";
    expect(
      (
        await harness.fetch(path, {
          method: "PUT",
          headers: { "stream-ttl": "30" },
        })
      ).status,
    ).toBe(201);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:cancel-generation",
    });
    await storage.exec(
      "CREATE TABLE IF NOT EXISTS issue_tracker_test_events (name TEXT PRIMARY KEY)",
    );
    const removing = harness.fetch(path, {
      method: "DELETE",
      headers: { "x-streamsy-test-failpoint": "pause-delete-cancellation" },
    });
    await eventually(
      async () =>
        (
          await storage.exec<{ present: number }>(
            "SELECT COUNT(*) present FROM issue_tracker_test_events WHERE name = 'delete-cancellation-paused'",
          )
        )[0]?.present === 1,
    );
    const replacement = await harness.fetch(path, {
      method: "PUT",
      headers: {
        "stream-ttl": "60",
        "x-streamsy-test-failpoint": "release-delete-cancellation-after-create",
      },
    });
    expect(replacement.status).toBe(201);
    expect((await removing).status).toBe(204);
    expect(
      await storage.exec<{ present: number }>(
        "SELECT COUNT(*) present FROM issue_tracker_streams WHERE stream_id = ?",
        "workspaces/cancel-generation/same",
      ),
    ).toEqual([{ present: 1 }]);
    const expiry = await storage.exec<{ expires_at_ms: number }>(
      "SELECT expires_at_ms FROM issue_tracker_stream_expiries WHERE stream_id = ?",
      "workspaces/cancel-generation/same",
    );
    expect(expiry[0]?.expires_at_ms).toBeGreaterThan(Date.now() + 50_000);
  });

  test("bounds due TTL work while delivering outbox work and re-arming the backlog", async () => {
    const harness = await fresh();
    for (let index = 0; index < 10; index += 1) {
      const headers = new Headers({ "stream-ttl": "1" });
      if (index === 9) {
        headers.set("x-streamsy-test-failpoint", "pause-next-maintenance-after-reconcile");
      }
      const response = await harness.fetch(`/streams/workspaces/fairness/ttl-${index}`, {
        method: "PUT",
        headers,
      });
      expect(response.status).toBe(201);
    }
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:fairness",
    });
    const earliest = await storage.exec<{ expires_at_ms: number }>(
      "SELECT MIN(expires_at_ms) expires_at_ms FROM issue_tracker_stream_expiries",
    );
    const commonDeadline = earliest[0]?.expires_at_ms;
    expect(commonDeadline).toBeNumber();
    await storage.exec(
      "UPDATE issue_tracker_stream_expiries SET expires_at_ms = ?",
      commonDeadline,
    );
    await storage.exec(
      "UPDATE issue_tracker_streams" +
        " SET record_json = json_set(record_json, '$.lifecycle.expiresAtMs', ?)",
      commonDeadline,
    );
    await harness.fetch(
      "/api/workspaces/fairness/issues",
      jsonRequest("POST", create("fair-create", "fair-issue", "Fair alarm")),
    );
    expect(
      (
        await harness.fetch("/api/workspaces/fairness/issues/fair-issue/assignee", {
          ...jsonRequest("POST", { commandId: "fair-assign", assigneeId: "ada" }),
          headers: {
            "content-type": "application/json",
            "x-streamsy-test-failpoint": "after-application-commit",
          },
        })
      ).status,
    ).toBe(503);
    await eventually(
      async () =>
        (
          await storage.exec<{ present: number }>(
            "SELECT COUNT(*) present FROM issue_tracker_test_events WHERE name = 'maintenance-paused'",
          )
        )[0]?.present === 1,
    );
    expect(
      await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"),
    ).toEqual([{ state: "delivered" }]);
    expect(
      await storage.exec<{ remaining: number }>(
        "SELECT COUNT(*) remaining FROM issue_tracker_stream_expiries",
      ),
    ).toEqual([{ remaining: 2 }]);
    const backlogAlarm = (await workspaceControl(harness, "fairness", "test-alarm=get")).alarm;
    expect(backlogAlarm).not.toBeNull();
    expect(backlogAlarm ?? Infinity).toBeLessThanOrEqual(Date.now() + 250);
    expect(
      (
        await harness.fetch("/streams/workspaces/fairness/release", {
          method: "PUT",
          headers: { "x-streamsy-test-failpoint": "release-maintenance-completion" },
        })
      ).status,
    ).toBe(201);
    await eventually(
      async () =>
        (
          await storage.exec<{ remaining: number }>(
            "SELECT COUNT(*) remaining FROM issue_tracker_stream_expiries",
          )
        )[0]?.remaining === 0,
    );
  });

  test("a failed alarm body leaves a replacement wake across eviction", async () => {
    const harness = await fresh();
    await harness.fetch(
      "/api/workspaces/alarm-body/issues",
      jsonRequest("POST", create("body-create", "body-issue", "Alarm body")),
    );
    expect(
      (
        await harness.fetch("/api/workspaces/alarm-body/issues/body-issue/assignee", {
          ...jsonRequest("POST", { commandId: "body-assign", assigneeId: "ada" }),
          headers: {
            "content-type": "application/json",
            "x-streamsy-test-failpoint": "after-application-commit-with-maintenance-failure",
          },
        })
      ).status,
    ).toBe(503);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:alarm-body",
    });
    await eventually(
      async () =>
        (
          await storage.exec<{ present: number }>(
            "SELECT COUNT(*) present FROM issue_tracker_test_events WHERE name = 'maintenance-failed'",
          )
        )[0]?.present === 1,
    );
    expect(
      await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"),
    ).toEqual([{ state: "pending" }]);
    await harness.evictWorkspace("alarm-body");

    // The first alarm already failed. No Worker/object fetch or maintenance RPC
    // follows eviction; its replacement guard must produce the second wake.
    await eventually(
      async () =>
        (await storage.exec<{ state: string }>("SELECT state FROM streamsy_effect_outbox"))[0]
          ?.state === "delivered",
    );
    expect(
      await storage.exec<{ present: number }>(
        "SELECT COUNT(*) present FROM issue_tracker_test_events" +
          " WHERE name = 'alarm-succeeded:0:false'",
      ),
    ).toEqual([{ present: 1 }]);
  });

  test("persists, renews, cancels, and fires raw stream expiry obligations", async () => {
    const harness = await fresh();
    const expiringPath = "/streams/workspaces/ttl/expiring";
    const cancelledPath = "/streams/workspaces/ttl/cancelled";
    expect(
      (
        await harness.fetch(expiringPath, {
          method: "PUT",
          headers: { "content-type": "text/plain", "stream-ttl": "1" },
          body: "first",
        })
      ).status,
    ).toBe(201);
    const storage = await harness.mf.unsafeGetDurableObjectStorage("", "WorkspacePartitionObject", {
      name: "workspace:ttl",
    });
    const initial = await storage.exec<{ stream_id: string; expires_at_ms: number }>(
      "SELECT stream_id, expires_at_ms FROM issue_tracker_stream_expiries",
    );
    expect(initial).toHaveLength(1);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect((await harness.fetch(`${expiringPath}?offset=now`)).status).toBe(200);
    const renewed = await storage.exec<{ expires_at_ms: number }>(
      "SELECT expires_at_ms FROM issue_tracker_stream_expiries WHERE stream_id = ?",
      "workspaces/ttl/expiring",
    );
    expect(renewed[0]?.expires_at_ms).toBeGreaterThan(initial[0]?.expires_at_ms ?? 0);

    expect(
      (
        await harness.fetch(cancelledPath, {
          method: "PUT",
          headers: { "content-type": "text/plain", "stream-ttl": "10" },
        })
      ).status,
    ).toBe(201);
    expect((await harness.fetch(cancelledPath, { method: "DELETE" })).status).toBe(204);
    expect(
      await storage.exec<{ stream_id: string }>(
        "SELECT stream_id FROM issue_tracker_stream_expiries ORDER BY stream_id",
      ),
    ).toEqual([{ stream_id: "workspaces/ttl/expiring" }]);

    await harness.evictWorkspace("ttl");
    await eventually(
      async () =>
        (
          await storage.exec<{ present: number }>(
            "SELECT COUNT(*) present FROM issue_tracker_streams WHERE stream_id = ?",
            "workspaces/ttl/expiring",
          )
        )[0]?.present === 0,
      5_000,
    );
    expect(
      await storage.exec<{ present: number }>(
        "SELECT COUNT(*) present FROM issue_tracker_stream_expiries",
      ),
    ).toEqual([{ present: 0 }]);
  });
});
