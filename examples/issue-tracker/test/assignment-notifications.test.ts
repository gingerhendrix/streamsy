/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is the effect sink driven through the same host the executable edge builds. */
/**
 * The effect sink, end to end.
 *
 * An assignment command becomes one canonical fact, one durable outbox entry
 * written with the command's receipt, and — when the lane is drained — exactly
 * one external effect. The properties that matter are the ones a duplicate
 * delivery or a broken notifier would violate: one effect per accepted command,
 * a bounded retry budget with a terminus, and a delivery path that cannot stall
 * the maintained view behind it.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { OutboxStore } from "@streamsy/effect-sink";
import { assignmentNotifications } from "../domain/declaration.ts";
import type { AssignmentNotification } from "../domain/notifications.ts";
import { assignmentDrafts } from "../server/notifications.ts";
import { memoryLayer } from "../server/store.ts";
import { sqliteLayer } from "../server/store-sqlite.ts";
import { IssueStore, type CommandReceipt } from "../server/store.ts";
import {
  CommandResponse,
  DrainResponse,
  IssuesResponse,
  NotificationsResponse,
} from "../shared/api.ts";
import { call, createIssueBody, host, json, temporaryDirectory, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function track(instance: Host): Host {
  open.push(instance);
  return instance;
}

/** A workspace with one issue, ready to be assigned. */
async function seeded(instance: Host, issueId = "issue-1"): Promise<Host> {
  await call(
    instance,
    "POST",
    "/api/workspaces/main/issues",
    createIssueBody(`create-${issueId}`, issueId, "Declare the issue view", "todo"),
  );
  return instance;
}

const assign = (instance: Host, issueId: string, commandId: string, assigneeId: string) =>
  call(instance, "POST", `/api/workspaces/main/issues/${issueId}/assignee`, {
    commandId,
    assigneeId,
  });

const notifications = async (instance: Host) =>
  json(await call(instance, "GET", "/api/workspaces/main/notifications"), NotificationsResponse);

const drain = async (instance: Host) =>
  json(await call(instance, "POST", "/api/workspaces/main/notifications/drain"), DrainResponse);

describe("the assignment command path", () => {
  test("an accepted assignment maintains the row and enqueues one delivery", async () => {
    const instance = await seeded(track(host()));

    const response = await assign(instance, "issue-1", "assign-1", "ada");
    const accepted = await json(response, CommandResponse);
    expect(response.status).toBe(200);
    expect(accepted.reconciled).toBe(false);
    expect(accepted.row?.assigneeId).toBe("ada");

    // The fact is durable and the delivery is queued, but nothing has been
    // delivered: enqueue and delivery are separate durable steps on purpose.
    const queued = await notifications(instance);
    expect(queued.sink).toBe("issue-tracker.assignment-notifications");
    expect(queued.contractFingerprint).toBe(assignmentNotifications.fingerprint);
    expect(queued).toMatchObject({ pending: 1, delivered: 0, dead: 0 });
    expect(queued.outbox[0]?.idempotencyKey).toBe("main/assign-1");
    expect(queued.outbox[0]?.payload).toMatchObject({
      issueId: "issue-1",
      assigneeId: "ada",
      title: "Declare the issue view",
      status: "todo",
      eventId: "assign-1",
    });
    expect(queued.notified).toHaveLength(0);

    expect(await drain(instance)).toMatchObject({ claimed: 1, delivered: 1, deadLettered: 0 });
    const settled = await notifications(instance);
    expect(settled).toMatchObject({ pending: 0, delivered: 1, dead: 0 });
    expect(settled.notified.map((entry) => entry.assigneeId)).toEqual(["ada"]);

    // A second drain has nothing due: a delivered effect is never repeated.
    expect(await drain(instance)).toMatchObject({ claimed: 0, delivered: 0 });
    expect((await notifications(instance)).notified).toHaveLength(1);
  });

  test("a retried assign command reports the original acceptance and adds no delivery", async () => {
    const instance = await seeded(track(host()));
    await assign(instance, "issue-1", "assign-1", "ada");
    const original = await json(
      await call(instance, "GET", "/api/workspaces/main/notifications"),
      NotificationsResponse,
    );

    const retried = await json(
      await assign(instance, "issue-1", "assign-1", "ada"),
      CommandResponse,
    );
    expect(retried.reconciled).toBe(true);
    expect(retried.maintenance.folded).toBe(0);

    const after = await notifications(instance);
    expect(after.outbox).toHaveLength(1);
    expect(after.outbox[0]?.id).toBe(original.outbox[0]?.id ?? -1);

    await drain(instance);
    expect((await notifications(instance)).notified).toHaveLength(1);
  });

  test("re-assigning to the same user appends a fact but implies no second notification", async () => {
    const instance = await seeded(track(host()));
    await assign(instance, "issue-1", "assign-1", "ada");
    await drain(instance);

    // A distinct command, so a distinct durable fact — but the row's assignee
    // does not change, so there is no assignment to notify anyone about.
    expect((await assign(instance, "issue-1", "assign-again", "ada")).status).toBe(200);
    const after = await notifications(instance);
    expect(after.outbox).toHaveLength(1);
    expect(after.notified).toHaveLength(1);
  });

  test("assigning an unknown issue is rejected before any fact is appended", async () => {
    const instance = track(host());
    const response = await assign(instance, "ghost", "assign-ghost", "ada");
    expect(response.status).toBe(404);
    expect((await notifications(instance)).outbox).toHaveLength(0);
  });
});

describe("a notifier that refuses", () => {
  test("retries on the declared backoff, dead-letters at the limit, and blocks nothing", async () => {
    const instance = await seeded(
      track(
        host({ notifications: { refuse: () => ({ detail: "notifier down", permanent: false }) } }),
      ),
    );
    await assign(instance, "issue-1", "assign-1", "ada");

    expect(await drain(instance)).toMatchObject({ delivered: 0, retried: 1, deadLettered: 0 });
    const first = await notifications(instance);
    expect(first.outbox[0]).toMatchObject({ state: "pending", attempts: 1 });
    expect(first.outbox[0]?.lastError).toBe("notifier down");

    // Nothing is due until the declared backoff has elapsed, so the retry is
    // paced rather than spun.
    expect(await drain(instance)).toMatchObject({ claimed: 0 });

    await Bun.sleep(300);
    expect(await drain(instance)).toMatchObject({ retried: 1 });
    expect((await notifications(instance)).outbox[0]).toMatchObject({ attempts: 2 });

    await Bun.sleep(1_100);
    expect(await drain(instance)).toMatchObject({ retried: 0, deadLettered: 1 });
    const dead = await notifications(instance);
    expect(dead).toMatchObject({ pending: 0, delivered: 0, dead: 1 });
    expect(dead.outbox[0]).toMatchObject({
      state: "dead",
      attempts: 3,
      deadLetterReason: "attempts-exhausted",
    });

    // The maintained view never noticed. Commands still succeed, rows are still
    // maintained, and the failed delivery is not holding a lane open.
    const moved = await json(
      await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
        commandId: "move-1",
        status: "done",
      }),
      CommandResponse,
    );
    expect(moved.row?.status).toBe("done");
    expect(moved.row?.assigneeId).toBe("ada");
    const rows = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(rows.rows.find((row) => row.issueId === "issue-1")?.assigneeId).toBe("ada");
  });

  test("a permanent refusal dead-letters immediately and leaves healthy deliveries alone", async () => {
    const instance = await seeded(
      track(
        host({
          notifications: {
            refuse: (notification: AssignmentNotification) =>
              notification.assigneeId === "ghost"
                ? { detail: "no such recipient", permanent: true }
                : undefined,
          },
        }),
      ),
    );
    await seeded(instance, "issue-2");
    await assign(instance, "issue-1", "assign-bad", "ghost");
    await assign(instance, "issue-2", "assign-good", "ada");

    expect(await drain(instance)).toMatchObject({ claimed: 2, delivered: 1, deadLettered: 1 });
    const settled = await notifications(instance);
    expect(settled).toMatchObject({ pending: 0, delivered: 1, dead: 1 });
    expect(settled.outbox[0]).toMatchObject({
      state: "dead",
      attempts: 1,
      deadLetterReason: "permanent",
    });
    expect(settled.notified.map((entry) => entry.assigneeId)).toEqual(["ada"]);
  });
});

describe("the durable outbox across a restart", () => {
  test("pending work survives, and the retry budget is not restarted with it", async () => {
    const directory = temporaryDirectory("issue-tracker-effect-sink");
    const failing = await seeded(
      track(
        host({
          databaseDirectory: directory,
          notifications: { refuse: () => ({ detail: "notifier down", permanent: false }) },
        }),
      ),
    );
    await assign(failing, "issue-1", "assign-1", "ada");
    expect(await drain(failing)).toMatchObject({ retried: 1 });
    await failing.close();
    open.length = 0;

    const recovered = track(host({ databaseDirectory: directory }));
    const pending = await notifications(recovered);
    expect(pending).toMatchObject({ pending: 1, delivered: 0, dead: 0 });
    // The attempt already spent is still spent: the budget is durable state,
    // not something a process holds.
    expect(pending.outbox[0]).toMatchObject({ attempts: 1, idempotencyKey: "main/assign-1" });

    await Bun.sleep(300);
    expect(await drain(recovered)).toMatchObject({ delivered: 1 });
    expect((await notifications(recovered)).notified.map((entry) => entry.issueId)).toEqual([
      "issue-1",
    ]);
  });
});

describe("the receipt-and-enqueue boundary", () => {
  const receipt: CommandReceipt = {
    workspaceId: "main",
    commandId: "assign-1",
    commandKind: "assign-issue",
    targetId: "issue-1",
    requestHash: "hash",
    eventId: "assign-1",
    eventSequence: 1,
    eventOffset: "1",
  };
  const drafts = assignmentDrafts(
    {
      type: "IssueAssigned",
      eventId: "assign-1",
      workspaceId: "main",
      issueId: "issue-1",
      sequence: 1,
      occurredAt: "2026-08-25T00:00:00.000Z",
      status: "todo",
      assigneeId: "ada",
    },
    {
      issueId: "issue-1",
      workspaceId: "main",
      projectId: "streamsy",
      title: "Declare the issue view",
      status: "todo",
      updatedAt: "2026-08-24T00:00:00.000Z",
    },
    1_000,
  );

  test("the accepted fact lowers to exactly one delivery draft", () => {
    expect(drafts).toEqual([
      {
        sink: "issue-tracker.assignment-notifications",
        partitionId: "main",
        idempotencyKey: "main/assign-1",
        payload: JSON.stringify({
          workspaceId: "main",
          issueId: "issue-1",
          assigneeId: "ada",
          title: "Declare the issue view",
          status: "todo",
          eventId: "assign-1",
          occurredAt: "2026-08-25T00:00:00.000Z",
        }),
        enqueuedAtMs: 1_000,
      },
    ]);
  });

  for (const [name, layer] of [
    ["memory", () => memoryLayer()],
    [
      "sqlite",
      () => sqliteLayer({ filename: `${temporaryDirectory("issue-tracker-outbox")}/view.sqlite` }),
    ],
  ] as const) {
    test(`${name} records a repeated receipt without enqueuing a second delivery`, async () => {
      const entries = await Effect.gen(function* () {
        const store = yield* IssueStore;
        const outbox = yield* OutboxStore;
        yield* store.recordReceipt(receipt, drafts);
        yield* store.recordReceipt(receipt, drafts);
        return yield* outbox.list(assignmentNotifications.name, "main");
      }).pipe(Effect.provide(layer()), Effect.scoped, Effect.runPromise);

      expect(entries).toHaveLength(1);
      expect(entries[0]).toMatchObject({ idempotencyKey: "main/assign-1", state: "pending" });
    });
  }
});
