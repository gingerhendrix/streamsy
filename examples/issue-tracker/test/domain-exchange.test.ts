/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow, and the restart fixtures need real on-disk databases, so they use the Node-compatible filesystem and path APIs. */
/**
 * The cross-domain exchange, driven end to end.
 *
 * Two workspace domains feed one user domain through a host-level component
 * that holds its own cursor in a third. Every assertion below is about a
 * property the *keying* is supposed to give: a user sees exactly the
 * assignments made to them, in a deterministic order; a record applied once is
 * applied once however many passes run over it; a restart of either side, or
 * of the whole host, resumes from the cursor rather than replaying into
 * duplicates; and a pass that cannot finish moves no position at all.
 *
 * Nothing here waits. The exchange is an explicit call, the idle sweep is an
 * explicit call, and the clock is injected — the same rule B3 set for delivery.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { Effect, Layer } from "effect";
import { ExchangeStatusResponse, InboxResponse, IssuesResponse } from "../shared/api.ts";
import { globalKey, userKey, workspaceKey } from "../domain/domains.ts";
import { assignmentInbox, EXCHANGE_CURSOR_DOMAIN } from "../domain/exchange.ts";
import { ExchangeCursorStore } from "../server/exchange-store.ts";
import { InboxStore } from "../server/inbox-store.ts";
import { partitionPath } from "../server/host.ts";
import { call, host, json, temporaryDirectory, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function track(instance: Host): Host {
  open.push(instance);
  return instance;
}

async function seed(instance: Host, workspaceId: string): Promise<void> {
  expect((await call(instance, "POST", `/api/workspaces/${workspaceId}/seed`, {})).status).toBe(
    200,
  );
}

async function assign(
  instance: Host,
  workspaceId: string,
  issueId: string,
  assigneeId: string,
  commandId: string,
): Promise<void> {
  const response = await call(
    instance,
    "POST",
    `/api/workspaces/${workspaceId}/issues/${issueId}/assignee`,
    { commandId, assigneeId },
  );
  expect(response.status).toBe(200);
}

const inboxOf = async (instance: Host, userId: string) =>
  (await json(await call(instance, "GET", `/api/users/${userId}/inbox`), InboxResponse)).rows;

const cursorsOf = async (instance: Host) =>
  (await json(await call(instance, "GET", "/api/global/exchange"), ExchangeStatusResponse)).cursors;

/** Open both workspace partitions, which is what makes them exchange sources. */
async function touch(instance: Host, ...workspaceIds: readonly string[]): Promise<void> {
  for (const workspaceId of workspaceIds) {
    await json(
      await call(instance, "GET", `/api/workspaces/${workspaceId}/issues`),
      IssuesResponse,
    );
  }
}

/** Two workspaces, three assignments, two assignees. The fixture every group starts from. */
async function seedTwoWorkspaces(instance: Host): Promise<void> {
  await seed(instance, "left");
  await seed(instance, "right");
  await assign(instance, "left", "seed-plan", "ada", "assign-left-1");
  await assign(instance, "left", "seed-publish", "grace", "assign-left-2");
  await assign(instance, "right", "seed-plan", "ada", "assign-right-1");
}

describe("cross-domain exchange", () => {
  test("two workspace domains feed one user inbox without cross-user leakage", async () => {
    const instance = track(host());
    await seedTwoWorkspaces(instance);

    const reports = await instance.host.exchange();
    expect(reports.map((report) => report.source.id).toSorted()).toEqual(["left", "right"]);
    expect(reports.every((report) => !report.failed)).toBe(true);
    expect(reports.reduce((sum, report) => sum + report.applied, 0)).toBe(3);

    const ada = await inboxOf(instance, "ada");
    expect(ada.map((row) => `${row.workspaceId}/${row.issueId}`)).toEqual([
      "left/seed-plan",
      "right/seed-plan",
    ]);
    // Every row ada holds is an assignment to ada, from a workspace ada's
    // partition has no other access to.
    expect(ada.every((row) => row.userId === "ada")).toBe(true);

    const grace = await inboxOf(instance, "grace");
    expect(grace.map((row) => row.issueId)).toEqual(["seed-publish"]);
    expect(grace.every((row) => row.userId === "grace")).toBe(true);

    // One partition per live domain identity: two workspaces, two users, the
    // global partition that holds the cursors.
    expect(
      instance.host
        .openPartitions()
        .map((key) => `${key.kind}:${key.id}`)
        .toSorted(),
    ).toEqual(["global:global", "user:ada", "user:grace", "workspace:left", "workspace:right"]);
  });

  test("an inbox is ordered by the fact, not by when it was exchanged", async () => {
    const instance = track(host());
    await seed(instance, "left");
    await seed(instance, "right");
    // `right` is assigned first, then `left`; both land in ada's inbox.
    await assign(instance, "right", "seed-plan", "ada", "assign-right");
    await instance.host.exchange();
    await assign(instance, "left", "seed-plan", "ada", "assign-left");
    await instance.host.exchange();

    const rows = await inboxOf(instance, "ada");
    const ordered = [...rows].toSorted((left, right) =>
      left.occurredAt === right.occurredAt
        ? left.workspaceId.localeCompare(right.workspaceId)
        : left.occurredAt.localeCompare(right.occurredAt),
    );
    expect(rows).toEqual(ordered);
    expect(rows).toHaveLength(2);
  });

  test("a second pass finds nothing, and a replayed pass writes no duplicate", async () => {
    const instance = track(host());
    await seedTwoWorkspaces(instance);
    await instance.host.exchange();
    const first = await inboxOf(instance, "ada");

    const second = await instance.host.exchange();
    expect(second.every((report) => report.scanned === 0 && report.applied === 0)).toBe(true);
    expect(await inboxOf(instance, "ada")).toEqual(first);
  });

  test("a pass replayed from an unadvanced cursor is a repeat, never a duplicate", async () => {
    // A host whose cursor writes are dropped is exactly a host that dies
    // between applying a record and recording that it did: every pass re-reads
    // the same records and re-applies them.
    const instance = track(
      host({
        exchangeStore: () =>
          Layer.sync(ExchangeCursorStore, () =>
            ExchangeCursorStore.of({
              read: (exchange, version, source) =>
                Effect.succeed({
                  domain: EXCHANGE_CURSOR_DOMAIN,
                  exchange,
                  version,
                  source,
                  arrival: 0,
                  applied: 0,
                }),
              advance: () => Effect.void,
              list: () => Effect.succeed([]),
            }),
          ),
      }),
    );
    await seedTwoWorkspaces(instance);

    await instance.host.exchange();
    const once = await inboxOf(instance, "ada");
    await instance.host.exchange();
    await instance.host.exchange();

    expect(await inboxOf(instance, "ada")).toEqual(once);
    expect(once).toHaveLength(2);
  });
});

describe("exchange positions", () => {
  test("the cursor is an exchange position, and says so", async () => {
    const instance = track(host());
    await seedTwoWorkspaces(instance);
    await instance.host.exchange();

    const cursors = await cursorsOf(instance);
    expect(cursors.map((cursor) => cursor.source.id).toSorted()).toEqual(["left", "right"]);
    for (const cursor of cursors) {
      expect(cursor.domain).toBe(EXCHANGE_CURSOR_DOMAIN);
      expect(cursor.exchange).toBe(assignmentInbox.name);
      expect(cursor.source.kind).toBe("workspace");
      // The position is a count of consumed source records, not an offset token.
      expect(Number.isInteger(cursor.arrival)).toBe(true);
      expect(cursor.arrival).toBeGreaterThan(0);
    }
    expect(cursors.reduce((sum, cursor) => sum + cursor.applied, 0)).toBe(3);
  });

  test("each domain's durable state is its own file, under its own domain directory", async () => {
    const directory = temporaryDirectory("issue-tracker-exchange-layout");
    const instance = track(host({ databaseDirectory: directory }));
    await seedTwoWorkspaces(instance);
    await instance.host.exchange();

    expect(existsSync(join(partitionPath(directory, workspaceKey("left")), "view.sqlite"))).toBe(
      true,
    );
    expect(existsSync(join(partitionPath(directory, userKey("ada")), "inbox.sqlite"))).toBe(true);
    expect(existsSync(join(partitionPath(directory, userKey("grace")), "inbox.sqlite"))).toBe(true);
    expect(existsSync(join(partitionPath(directory, globalKey()), "exchange.sqlite"))).toBe(true);
    // B3's workspace layout is unchanged, so a pre-B4 data directory still reads.
    expect(partitionPath(directory, workspaceKey("left"))).toBe(
      join(directory, "workspaces", "left"),
    );
  });
});

describe("exchange across restarts", () => {
  test("restarting the source partition resumes from the cursor", async () => {
    const directory = temporaryDirectory("issue-tracker-exchange-source-restart");
    const instance = track(host({ databaseDirectory: directory }));
    await seedTwoWorkspaces(instance);
    await instance.host.exchange();
    const before = await inboxOf(instance, "ada");

    expect(await instance.host.restart(workspaceKey("left"))).toBe(true);
    await touch(instance, "left");

    const reports = await instance.host.exchange();
    const left = reports.find((report) => report.source.id === "left");
    expect(left).toMatchObject({ failed: false, scanned: 0, applied: 0 });
    expect(await inboxOf(instance, "ada")).toEqual(before);
  });

  test("restarting the destination partition keeps its rows and adds none", async () => {
    const directory = temporaryDirectory("issue-tracker-exchange-user-restart");
    const instance = track(host({ databaseDirectory: directory }));
    await seedTwoWorkspaces(instance);
    await instance.host.exchange();
    const before = await inboxOf(instance, "ada");
    expect(before).toHaveLength(2);

    expect(await instance.host.restart(userKey("ada"))).toBe(true);
    expect(
      instance.host.openPartitions().some((key) => key.kind === "user" && key.id === "ada"),
    ).toBe(false);

    // Rebuilt from its own durable state, and a further pass adds nothing.
    expect(await inboxOf(instance, "ada")).toEqual(before);
    await instance.host.exchange();
    expect(await inboxOf(instance, "ada")).toEqual(before);
  });

  test("a whole-host restart restores the cursor and the inbox", async () => {
    const directory = temporaryDirectory("issue-tracker-exchange-host-restart");
    const first = host({ databaseDirectory: directory });
    await seedTwoWorkspaces(first);
    await first.host.exchange();
    const before = await inboxOf(first, "ada");
    const cursorsBefore = await cursorsOf(first);
    await first.close();

    const second = track(host({ databaseDirectory: directory }));
    // The inbox is durable state of the user partition, reachable with no
    // workspace partition open at all.
    expect(await inboxOf(second, "ada")).toEqual(before);
    expect(await cursorsOf(second)).toEqual(cursorsBefore);

    await touch(second, "left", "right");
    const reports = await second.host.exchange();
    expect(reports.every((report) => !report.failed && report.applied === 0)).toBe(true);
    expect(await inboxOf(second, "ada")).toEqual(before);

    // New work after the restart still flows, from the restored position.
    await assign(second, "left", "seed-maintain", "ada", "assign-after-restart");
    await second.host.exchange();
    expect(await inboxOf(second, "ada")).toHaveLength(before.length + 1);
  });
});

describe("key alignment and fail-stop", () => {
  test("a pass that cannot open its destination advances no cursor", async () => {
    // Two slots: the source workspace and the global partition fill them, and
    // both are leased by the pass, so the destination has nowhere to open.
    const instance = track(host({ partitions: { maxOpen: 2 } }));
    await seed(instance, "left");
    await assign(instance, "left", "seed-plan", "ada", "assign-left");

    const [report] = await instance.host.exchange();
    expect(report).toMatchObject({ failed: true, applied: 0 });
    expect(report?.detail).toContain("PartitionLimitReached");
    expect(report?.fromArrival).toBe(report?.toArrival);
    expect(instance.host.metrics().exchange.failures).toBe(1);

    // No position moved, so the work is still owed.
    expect(await cursorsOf(instance)).toEqual([]);
  });

  test("a record placed at another user is refused before it is written", async () => {
    const instance = track(host());
    await seed(instance, "left");
    await assign(instance, "left", "seed-plan", "ada", "assign-left");
    await instance.host.exchange();

    // The store is the last boundary: a row for `ada` handed to `grace`'s
    // partition is refused rather than written, whatever routed it there.
    const graceInbox = await inboxOf(instance, "grace");
    expect(graceInbox).toHaveLength(0);
    const rows = await inboxOf(instance, "ada");
    const stray = rows[0];
    if (stray === undefined) throw new Error("expected one exchanged row");
    const grace = instance.host.partition(userKey("grace"));
    if ("_tag" in grace) throw new Error("expected a user partition");
    const refused = await grace.runtime
      .runPromise(
        Effect.gen(function* () {
          const store = yield* InboxStore;
          return yield* store.upsert("grace", [stray]);
        }),
      )
      .then(
        () => undefined,
        (cause: unknown) => String(cause),
      );
    expect(refused).toContain("InboxUnavailable");
    expect(await inboxOf(instance, "grace")).toHaveLength(0);
  });
});

describe("exchange leases", () => {
  test("an idle sweep will not close a leased partition", async () => {
    let clock = 1_000;
    const instance = track(host({ partitions: { idleMillis: 60_000 }, now: () => clock }));
    await seed(instance, "left");
    const lease = instance.host.lease(workspaceKey("left"));
    if ("_tag" in lease) throw new Error("expected a lease");

    clock += 120_000;
    expect(await instance.host.sweepIdle()).toEqual([]);
    expect(instance.host.openWorkspaces()).toEqual(["left"]);
    expect(instance.host.metrics().partitions.find((entry) => entry.id === "left")?.leases).toBe(1);

    lease.release();
    expect(await instance.host.sweepIdle()).toEqual([workspaceKey("left")]);
    expect(instance.host.openWorkspaces()).toEqual([]);
  });

  test("a leased partition is not evicted to make room, and the host refuses instead", async () => {
    const instance = track(host({ partitions: { maxOpen: 1 } }));
    await call(instance, "GET", "/api/workspaces/left/issues");
    const lease = instance.host.lease(workspaceKey("left"));
    if ("_tag" in lease) throw new Error("expected a lease");

    const refused = await call(instance, "GET", "/api/workspaces/right/issues");
    expect(refused.status).toBe(503);
    expect(await refused.json()).toMatchObject({ error: "partition-limit-reached" });
    expect(instance.host.openWorkspaces()).toEqual(["left"]);

    // Released, the same slot is reclaimable again.
    lease.release();
    expect((await call(instance, "GET", "/api/workspaces/right/issues")).status).toBe(200);
    expect(instance.host.openWorkspaces()).toEqual(["right"]);
  });

  test("releasing a lease twice releases it once", async () => {
    const instance = track(host());
    const lease = instance.host.lease(workspaceKey("left"));
    if ("_tag" in lease) throw new Error("expected a lease");
    const second = instance.host.lease(workspaceKey("left"));
    if ("_tag" in second) throw new Error("expected a second lease");

    lease.release();
    lease.release();
    expect(instance.host.metrics().partitions.find((entry) => entry.id === "left")?.leases).toBe(1);
    second.release();
    expect(instance.host.metrics().partitions.find((entry) => entry.id === "left")?.leases).toBe(0);
  });
});

describe("the user and global domain routes", () => {
  test("an empty inbox is a served product, not a 404", async () => {
    const instance = track(host());
    const body = await json(await call(instance, "GET", "/api/users/ada/inbox"), InboxResponse);
    expect(body).toMatchObject({ userId: "ada", exchange: assignmentInbox.name, rows: [] });
  });

  test("a user id the domain refuses never opens a partition", async () => {
    const instance = track(host());
    const refused = await call(instance, "GET", "/api/users/..%2Fescape/inbox");
    expect(refused.status).toBe(400);
    expect(await refused.json()).toMatchObject({ error: "invalid-domain-id" });
    expect(instance.host.openPartitions()).toEqual([]);
  });

  test("the exchange surface is read-only", async () => {
    const instance = track(host());
    expect((await call(instance, "POST", "/api/global/exchange", {})).status).toBe(405);
    expect((await call(instance, "GET", "/api/global/nothing")).status).toBe(404);
    expect((await call(instance, "GET", "/api/global")).status).toBe(404);
  });
});
