/* oxlint-disable effecttsgo/async-function -- bun:test owns the Promise-native workerd harness. */
import { afterEach, describe, expect, test } from "bun:test";
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
afterEach(async () => {
  await Promise.all(open.splice(0).map((harness) => harness.close()));
});

async function fresh(): Promise<WorkerdHarness> {
  const harness = await workerdHarness();
  open.push(harness);
  return harness;
}

const create = (commandId: string, issueId: string, title: string) => ({
  commandId,
  issueId,
  projectId: "streamsy",
  title,
  status: "todo",
});

describe("Cloudflare workspace placement on real workerd storage", () => {
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
});
