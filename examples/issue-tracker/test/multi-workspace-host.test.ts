/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow; what is under test is the host's partition lifecycle across real requests. */
/**
 * The keyed multi-workspace host.
 *
 * Two workspaces run in one process and share nothing. Every assertion below
 * is about that: a command in one workspace is invisible to the other, a
 * partition can be restarted or given up while its neighbour keeps serving,
 * and a reopened partition rebuilds every durable thing it owned — rows,
 * checkpoints, streams, receipts and the outbox's settled and pending work.
 *
 * Lifecycle is driven, never awaited: time is injected and the idle sweep is a
 * call, so nothing here depends on a timer firing.
 */
import { afterEach, describe, expect, test } from "bun:test";
import {
  CommandResponse,
  DrainResponse,
  IssuesResponse,
  NotificationsResponse,
  TransitionFeedResponse,
} from "../shared/api.ts";
import { streamNames } from "../domain/declaration.ts";
import { OutboxStore, OutboxUnavailable } from "@streamsy/effect-sink";
import { Effect, Layer } from "effect";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { partitionPath } from "../server/host.ts";
import { memoryLayer } from "../server/store.ts";
import { call, createIssueBody, host, json, temporaryDirectory, type Host } from "./support.ts";

/**
 * A partition whose delivery lane is broken at the store, not at the notifier.
 *
 * This is the one failure the sink runtime cannot absorb into a retry: it
 * cannot even read what is due. The host must count it and carry on.
 */
const unavailable = (operation: string) =>
  Effect.fail(new OutboxUnavailable({ operation, detail: "injected" }));

const failingOutboxStore = Layer.merge(
  memoryLayer(),
  Layer.succeed(
    OutboxStore,
    OutboxStore.of({
      enqueue: () => unavailable("enqueue"),
      claimDue: () => unavailable("claimDue"),
      markDelivered: () => unavailable("markDelivered"),
      reschedule: () => unavailable("reschedule"),
      deadLetter: () => unavailable("deadLetter"),
      list: () => unavailable("list"),
    }),
  ),
);

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function track(instance: Host): Host {
  open.push(instance);
  return instance;
}

async function seed(instance: Host, workspaceId: string): Promise<void> {
  const response = await call(instance, "POST", `/api/workspaces/${workspaceId}/seed`, {});
  expect(response.status).toBe(200);
}

const rowsOf = async (instance: Host, workspaceId: string) =>
  (await json(await call(instance, "GET", `/api/workspaces/${workspaceId}/issues`), IssuesResponse))
    .rows;

describe("workspace isolation", () => {
  test("two seeded workspaces run in one host and share no rows", async () => {
    const instance = track(host());
    await seed(instance, "left");
    await seed(instance, "right");

    await call(
      instance,
      "POST",
      "/api/workspaces/left/issues",
      createIssueBody("left-1", "only-left", "Left only", "todo"),
    );

    const left = await rowsOf(instance, "left");
    const right = await rowsOf(instance, "right");
    expect(left.map((row) => row.issueId)).toContain("only-left");
    expect(right.map((row) => row.issueId)).not.toContain("only-left");
    expect(right).toHaveLength(4);
    expect(instance.host.openWorkspaces().toSorted()).toEqual(["left", "right"]);
  });

  test("each partition owns its own storage, client and runtime", () => {
    const instance = track(host());
    const left = instance.host.partition("left");
    const right = instance.host.partition("right");
    if ("_tag" in left || "_tag" in right) throw new Error("expected two partitions");
    expect(left.adapter).not.toBe(right.adapter);
    expect(left.client).not.toBe(right.client);
    expect(left.runtime).not.toBe(right.runtime);
  });

  test("a fact appended outside the command path converges in its own workspace only", async () => {
    const instance = track(host());
    await rowsOf(instance, "left");
    await rowsOf(instance, "right");

    // Straight to `left`'s durable source, as another process would append it.
    const left = instance.host.partition("left");
    if ("_tag" in left) throw new Error("expected a partition for left");
    const appended = await left.client.stream(streamNames.issueEvents("left")).append(
      JSON.stringify({
        type: "IssueCreated",
        eventId: "out-of-band",
        workspaceId: "left",
        issueId: "out-of-band",
        sequence: 0,
        occurredAt: "2026-08-26T00:00:00.000Z",
        title: "Appended by another process",
        projectId: "streamsy",
        status: "todo",
      }),
      { contentType: "application/json" },
    );
    expect(appended.status).toBe("appended");

    // The next request brings the partition up to its durable tail...
    expect((await rowsOf(instance, "left")).map((row) => row.issueId)).toContain("out-of-band");
    // ...and the identically named stream in the neighbour's partition is a
    // different stream, in different storage, that never saw the fact.
    expect(await rowsOf(instance, "right")).toHaveLength(0);
  });

  test("feeds, summaries and notifications are answered per workspace", async () => {
    const instance = track(host());
    await seed(instance, "left");
    await call(instance, "POST", "/api/workspaces/left/issues/seed-plan/assignee", {
      commandId: "assign-left",
      assigneeId: "ada",
    });

    const leftFeed = await json(
      await call(instance, "GET", "/feed/workspaces/left/issue-transitions"),
      TransitionFeedResponse,
    );
    const rightFeed = await json(
      await call(instance, "GET", "/feed/workspaces/right/issue-transitions"),
      TransitionFeedResponse,
    );
    expect(leftFeed.events.length).toBeGreaterThan(0);
    expect(rightFeed.events).toHaveLength(0);

    const leftNotifications = await json(
      await call(instance, "GET", "/api/workspaces/left/notifications"),
      NotificationsResponse,
    );
    const rightNotifications = await json(
      await call(instance, "GET", "/api/workspaces/right/notifications"),
      NotificationsResponse,
    );
    expect(leftNotifications.pending).toBe(1);
    expect(rightNotifications.pending).toBe(0);

    const summary = await call(instance, "GET", "/document/workspaces/right/summary");
    expect(summary.status).toBe(200);
  });
});

describe("partition lifecycle", () => {
  test("restarting one partition leaves the other live and converged", async () => {
    const directory = temporaryDirectory("issue-tracker-partition-restart");
    const instance = track(host({ databaseDirectory: directory }));
    await seed(instance, "left");
    await seed(instance, "right");
    const rightBefore = instance.host.partition("right");

    expect(await instance.host.restart("left")).toBe(true);
    expect(instance.host.openWorkspaces()).toEqual(["right"]);
    // The neighbour was not touched: same partition object, same runtime.
    expect(instance.host.partition("right")).toBe(rightBefore);
    expect(await rowsOf(instance, "right")).toHaveLength(4);

    const reopened = await rowsOf(instance, "left");
    expect(reopened).toHaveLength(4);
    // Each partition's durable state is its own pair of databases.
    for (const workspaceId of ["left", "right"]) {
      const directoryFor = partitionPath(directory, workspaceId);
      expect(existsSync(join(directoryFor, "streams.sqlite"))).toBe(true);
      expect(existsSync(join(directoryFor, "view.sqlite"))).toBe(true);
    }
    expect(instance.host.metrics().restarted).toBe(1);
    expect(
      instance.host.metrics().workspaces.find((entry) => entry.workspaceId === "left")?.opens,
    ).toBe(2);
  });

  test("a reopened partition recovers rows, receipts, streams and outbox work", async () => {
    const directory = temporaryDirectory("issue-tracker-partition-reopen");
    const instance = track(host({ databaseDirectory: directory }));
    const created = await json(
      await call(
        instance,
        "POST",
        "/api/workspaces/left/issues",
        createIssueBody("cmd-1", "issue-1", "Survives a restart", "todo"),
      ),
      CommandResponse,
    );
    await call(instance, "POST", "/api/workspaces/left/issues/issue-1/assignee", {
      commandId: "assign-1",
      assigneeId: "ada",
    });
    const drained = await json(
      await call(instance, "POST", "/api/workspaces/left/notifications/drain", {}),
      DrainResponse,
    );
    expect(drained.delivered).toBe(1);
    await call(instance, "POST", "/api/workspaces/left/issues/issue-1/assignee", {
      commandId: "assign-2",
      assigneeId: "grace",
    });

    await instance.host.restart("left");

    const rows = await rowsOf(instance, "left");
    expect(rows.find((row) => row.issueId === "issue-1")?.assigneeId).toBe("grace");

    // The receipt survived, so the retry reconciles to the original offset.
    const retried = await json(
      await call(
        instance,
        "POST",
        "/api/workspaces/left/issues",
        createIssueBody("cmd-1", "issue-1", "Survives a restart", "todo"),
      ),
      CommandResponse,
    );
    expect(retried.reconciled).toBe(true);
    expect(retried.ack.offset).toBe(created.ack.offset);

    // The outbox survived with both its settled and its pending work.
    const notifications = await json(
      await call(instance, "GET", "/api/workspaces/left/notifications"),
      NotificationsResponse,
    );
    expect(notifications.delivered).toBe(1);
    expect(notifications.pending).toBe(1);
    const redrained = await json(
      await call(instance, "POST", "/api/workspaces/left/notifications/drain", {}),
      DrainResponse,
    );
    expect(redrained.claimed).toBe(1);
    expect(redrained.delivered).toBe(1);

    // The durable feed replays from the start, in arrival order, across the restart.
    const feed = await json(
      await call(instance, "GET", "/feed/workspaces/left/issue-transitions"),
      TransitionFeedResponse,
    );
    // `enter` then one `update` per assignment. A restart may re-publish the
    // suffix it had committed but not yet published, so the feed is
    // at-least-once: the assertion is on order and content, not on a count.
    expect(feed.events[0]?.change).toBe("enter");
    expect(feed.events.every((event) => event.issueId === "issue-1")).toBe(true);
    expect(feed.events.filter((event) => event.change === "update").length).toBeGreaterThanOrEqual(
      2,
    );
    expect(feed.events.at(-1)?.change).toBe("update");
  });

  test("an idle sweep gives up a partition, and the next request rebuilds it", async () => {
    const directory = temporaryDirectory("issue-tracker-partition-idle");
    let clock = 1_000;
    const instance = track(
      host({
        databaseDirectory: directory,
        partitions: { idleMillis: 60_000 },
        now: () => clock,
      }),
    );
    await seed(instance, "left");
    await seed(instance, "right");

    clock += 30_000;
    await call(instance, "GET", "/api/workspaces/right/issues");
    clock += 40_000;

    // Only `left` has been idle for the full window.
    expect(await instance.host.sweepIdle()).toEqual(["left"]);
    expect(instance.host.openWorkspaces()).toEqual(["right"]);
    expect(instance.host.metrics().idled).toBe(1);

    expect(await rowsOf(instance, "left")).toHaveLength(4);
    expect(
      instance.host.metrics().workspaces.find((entry) => entry.workspaceId === "left")?.opens,
    ).toBe(2);
  });

  test("a full host evicts the least recently used partition to make room", async () => {
    let clock = 1_000;
    const instance = track(host({ partitions: { maxOpen: 1 }, now: () => clock }));
    await call(instance, "GET", "/api/workspaces/left/issues");
    clock += 1_000;
    await call(instance, "GET", "/api/workspaces/right/issues");

    expect(instance.host.openWorkspaces()).toEqual(["right"]);
    expect(instance.host.metrics().evicted).toBe(1);
    expect(instance.host.metrics().closed).toBe(1);
  });

  test("a full host with nothing evictable refuses, typed", async () => {
    const instance = track(host({ partitions: { maxOpen: 1 } }));
    // Started but not awaited: the partition is open with a request in flight,
    // so it is not a candidate for eviction.
    const pending = call(instance, "GET", "/api/workspaces/left/issues");
    const refused = await call(instance, "GET", "/api/workspaces/right/issues");

    expect(refused.status).toBe(503);
    expect(await refused.json()).toMatchObject({ error: "partition-limit-reached" });
    expect((await pending).status).toBe(200);
    expect(instance.host.metrics().failures.PartitionLimitReached).toBe(1);
  });

  test("closing the host closes every partition exactly once", async () => {
    const instance = host();
    await call(instance, "GET", "/api/workspaces/left/issues");
    await call(instance, "GET", "/api/workspaces/right/issues");
    expect(instance.host.metrics().open).toBe(2);

    await instance.close();
    await instance.close();
    await instance.host.close();

    expect(instance.host.metrics().closed).toBe(2);
    expect(instance.host.openWorkspaces()).toEqual([]);

    const refused = await call(instance, "GET", "/api/workspaces/left/issues");
    expect(refused.status).toBe(503);
    expect(await refused.json()).toMatchObject({ error: "host-closed" });
  });
});

describe("host failures and metrics", () => {
  test("health is answered by the host, without opening a partition", async () => {
    const instance = track(host());
    const health = await call(instance, "GET", "/health");
    expect(health.status).toBe(200);
    expect(await health.json()).toMatchObject({ status: "ok", view: "issue-tracker.issues" });
    expect(instance.host.openWorkspaces()).toEqual([]);
    expect(instance.host.metrics().hostRequests).toBe(1);
  });

  test("metrics count partitions, requests and typed refusals", async () => {
    const instance = track(host());
    await call(instance, "GET", "/api/workspaces/left/issues");
    await call(instance, "GET", "/api/workspaces/left/issues");
    await call(instance, "GET", "/api/workspaces/right/issues");
    const unroutable = await call(instance, "GET", "/api/nothing");
    expect(unroutable.status).toBe(404);

    const metrics = await (await call(instance, "GET", "/host/metrics")).json();
    expect(metrics).toMatchObject({
      open: 2,
      opened: 2,
      closed: 0,
      requests: 3,
      failures: { UnroutableRequest: 1 },
    });
    const left = instance.host.metrics().workspaces.find((entry) => entry.workspaceId === "left");
    expect(left?.requests).toBe(2);
    expect(left?.inFlight).toBe(0);
  });

  test("a workspace id the domain refuses never opens a partition", async () => {
    const instance = track(host());
    const refused = await call(instance, "GET", "/api/workspaces/..%2Fescape/issues");
    expect(refused.status).toBe(400);
    expect(await refused.json()).toMatchObject({ error: "invalid-workspace-id" });
    expect(instance.host.openWorkspaces()).toEqual([]);
  });

  test("an undecodable sink parameter still answers with the sink's own contract", async () => {
    const instance = track(host());
    const refused = await call(instance, "GET", "/state/workspaces/NO!/issues");
    expect(refused.status).toBe(400);
    expect(await refused.json()).toMatchObject({
      _tag: "InvalidSinkParams",
      sink: "issue-tracker.board-issues",
      parameter: "workspaceId",
    });
    expect(instance.host.openWorkspaces()).toEqual([]);
  });
});

describe("managed effect delivery", () => {
  test("one pass drains every open partition's lane", async () => {
    const instance = track(host());
    for (const workspaceId of ["left", "right"]) {
      await seed(instance, workspaceId);
      await call(instance, "POST", `/api/workspaces/${workspaceId}/issues/seed-plan/assignee`, {
        commandId: `assign-${workspaceId}`,
        assigneeId: "ada",
      });
    }

    const reports = await instance.host.drainDue();
    expect(reports.map((report) => report.workspaceId).toSorted()).toEqual(["left", "right"]);
    expect(reports.every((report) => report.delivered === 1 && !report.failed)).toBe(true);
    expect(instance.host.metrics().delivery).toMatchObject({ delivered: 2, failures: 0 });

    // A second pass finds nothing due: delivery is once per idempotency key.
    expect((await instance.host.drainDue()).every((report) => report.claimed === 0)).toBe(true);
  });

  test("a notifier that fails is isolated from commands and view maintenance", async () => {
    const instance = track(
      host({
        notifications: {
          refuse: () => {
            throw new Error("notifier exploded");
          },
        },
      }),
    );
    await seed(instance, "left");
    await call(instance, "POST", "/api/workspaces/left/issues/seed-plan/assignee", {
      commandId: "assign-left",
      assigneeId: "ada",
    });

    // The sink runtime absorbs the defect into its own retry policy, so the
    // pass reports a rescheduled delivery rather than a failed pass.
    const [report] = await instance.host.drainDue();
    expect(report).toMatchObject({ workspaceId: "left", failed: false, delivered: 0, retried: 1 });
    expect(instance.host.metrics().delivery).toMatchObject({ delivered: 0, retried: 1 });

    // The command path and the maintained rows are untouched by the failure.
    const accepted = await call(
      instance,
      "POST",
      "/api/workspaces/left/issues",
      createIssueBody("after-failure", "issue-after", "Still accepted", "todo"),
    );
    expect(accepted.status).toBe(201);
    expect((await rowsOf(instance, "left")).map((row) => row.issueId)).toContain("issue-after");
  });

  test("a delivery pass that fails outright is counted, not propagated", async () => {
    const instance = track(host({ store: failingOutboxStore }));
    await seed(instance, "main");

    const [report] = await instance.host.drainDue();
    expect(report).toMatchObject({ workspaceId: "main", failed: true });
    expect(report?.detail).toContain("OutboxUnavailable");
    expect(instance.host.metrics().delivery.failures).toBe(1);

    // The partition is still serving: only its delivery lane is broken.
    expect(await rowsOf(instance, "main")).toHaveLength(4);
  });
});
