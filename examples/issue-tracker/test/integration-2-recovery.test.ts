/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow; what is under test is crash and replay behaviour across the assembled application. */
/**
 * Crash and replay across the whole Integration 2 application.
 *
 * Each test names one place a process can die and asserts what survives it.
 * Two of them are new laws rather than restatements of B3/B4:
 *
 * - **The transition feed is now atomic with the rows it describes.** A crash
 *   between the row commit and the feed append leaves the batch *owed*, and the
 *   next pass publishes it from the committed change history. A crash after the
 *   append and before the marker replays the same producer sequence, which the
 *   protocol refuses as a duplicate. Both directions are driven here.
 * - **The inbox converges for a workspace nobody is looking at.** The exchange
 *   reads a durable registry of sources rather than whatever the host happens
 *   to hold open, so a workspace that has been idled out is still exchanged.
 */
import { afterEach, describe, expect, test } from "bun:test";
import type {
  AppendJsonBatchOptions,
  AppendStreamOptions,
  ClientAppendResult,
  JsonValue,
  StreamProtocolClient,
  StreamProtocolHandle,
} from "@streamsy/core";
import { globalKey, userKey, workspaceKey } from "../domain/domains.ts";
import { ExchangeStatusResponse, InboxResponse, TransitionFeedResponse } from "../shared/api.ts";
import { LabelCountsResponse } from "../shared/api.ts";
import { call, host, json, temporaryDirectory, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});
const track = (instance: Host): Host => {
  open.push(instance);
  return instance;
};

const feed = async (instance: Host, workspaceId = "main") =>
  json(
    await call(instance, "GET", `/feed/workspaces/${workspaceId}/issue-transitions`),
    TransitionFeedResponse,
  );
const counts = async (instance: Host, workspaceId = "main") =>
  json(
    await call(instance, "GET", `/api/workspaces/${workspaceId}/label-counts`),
    LabelCountsResponse,
  );
const inbox = async (instance: Host, userId: string) =>
  json(await call(instance, "GET", `/api/users/${userId}/inbox`), InboxResponse);

const move = (instance: Host, issueId: string, status: string, commandId: string) =>
  call(instance, "POST", `/api/workspaces/main/issues/${issueId}/status`, { commandId, status });

describe("transition feed atomicity", () => {
  test("a batch lost between the row commit and the feed append is published by the next pass", async () => {
    const dropped = dropFeedAppends();
    const instance = track(host({ applicationClient: (client) => dropped.wrap(client) }));
    await call(instance, "POST", "/api/workspaces/main/seed");

    // Everything appended while the feed was unwritable is still owed.
    dropped.dropping = true;
    // The rows commit and the feed append is lost, so the command fails *after*
    // its fold landed — which is the crash window, seen from a client. The next
    // command is refused too, because a pass that still owes transitions does
    // not advance past them: fail-stop, not a silent hole.
    expect((await move(instance, "seed-plan", "todo", "lost-1")).status).toBe(503);
    expect((await move(instance, "seed-maintain", "done", "lost-2")).status).toBe(503);
    dropped.dropping = false;

    const recovered = await feed(instance);
    const moves = recovered.events.filter((event) => event.change === "update");
    expect(moves.map((event) => event.issueId)).toEqual(["seed-plan"]);
    expect(moves[0]?.status).toBe("todo");
    expect(moves[0]?.previousStatus).toBe("done");

    // The maintained row and the last transition agree, and new work appends
    // after the recovered batch rather than before it.
    expect((await move(instance, "seed-maintain", "done", "after-1")).status).toBe(200);
    const after = await feed(instance);
    expect(
      after.events.filter((event) => event.change === "update").map((event) => event.issueId),
    ).toEqual(["seed-plan", "seed-maintain"]);
  });

  test("the recovered batch is published exactly once, however many passes run", async () => {
    const dropped = dropFeedAppends();
    const instance = track(host({ applicationClient: (client) => dropped.wrap(client) }));
    await call(instance, "POST", "/api/workspaces/main/seed");
    dropped.dropping = true;
    expect((await move(instance, "seed-plan", "todo", "lost-1")).status).toBeGreaterThan(499);
    dropped.dropping = false;

    const first = await feed(instance);
    await feed(instance);
    const third = await feed(instance);
    expect(third.events).toEqual(first.events);
  });

  test("a marker lost after the append replays the producer sequence and writes no duplicate", async () => {
    /**
     * The other side of the crash window: the feed append *landed* and the
     * response was lost, so the publication marker never moved and the next
     * pass replays the same batch. The producer lane is what makes that a
     * repeat rather than a duplicate — the protocol answers `duplicate` on a
     * sequence it has already accepted, and the feed is unchanged.
     */
    const lost = loseFeedAppendResponse();
    const instance = track(host({ applicationClient: (client) => lost.wrap(client) }));
    await call(instance, "POST", "/api/workspaces/main/seed");
    const seeded = await feed(instance);

    lost.lose = true;
    expect((await move(instance, "seed-plan", "todo", "landed-1")).status).toBe(503);

    const first = await feed(instance);
    const moves = first.events.filter((event) => event.change === "update");
    expect(moves).toHaveLength(1);
    expect(first.events).toHaveLength(seeded.events.length + 1);

    // Every later pass replays the same sequence and appends nothing.
    await feed(instance);
    expect((await feed(instance)).events).toEqual(first.events);
  });
});

describe("membership and label counts across restarts", () => {
  test("a source-partition restart resumes the membership fold from its own checkpoint", async () => {
    const directory = temporaryDirectory("i2-membership");
    const instance = track(host({ databaseDirectory: directory }));
    await call(instance, "POST", "/api/workspaces/main/seed");
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/labels", {
      commandId: "attach-1",
      labelId: "bug",
    });
    const before = await counts(instance);

    expect(await instance.host.restart(workspaceKey("main"))).toBe(true);
    expect(await counts(instance)).toEqual(before);

    // New work after the restart still lands on the resumed relation.
    await call(instance, "POST", "/api/workspaces/main/issues/seed-maintain/labels", {
      commandId: "attach-2",
      labelId: "bug",
    });
    expect((await counts(instance)).rows.find((row) => row.labelId === "bug")?.issueCount).toBe(3);
  });

  test("a whole-host restart rebuilds the board, the counts and the feed together", async () => {
    const directory = temporaryDirectory("i2-host");
    const first = track(host({ databaseDirectory: directory }));
    await call(first, "POST", "/api/workspaces/main/seed");
    await call(first, "POST", "/api/workspaces/main/issues/seed-plan/labels", {
      commandId: "attach-1",
      labelId: "bug",
    });
    await move(first, "seed-plan", "todo", "move-1");
    const beforeCounts = await counts(first);
    const beforeFeed = await feed(first);
    await first.close();
    open.length = 0;

    const second = track(host({ databaseDirectory: directory }));
    expect(await counts(second)).toEqual(beforeCounts);
    // The feed is a durable log, so a restart adds nothing to it.
    expect((await feed(second)).events).toEqual(beforeFeed.events);
  });
});

describe("exchange recovery across three domains", () => {
  test("an idled-out source workspace is still exchanged into the user inbox", async () => {
    const directory = temporaryDirectory("i2-cold");
    let clock = 1_000;
    const instance = track(
      host({
        databaseDirectory: directory,
        now: () => clock,
        partitions: { idleMillis: 10 },
      }),
    );
    await call(instance, "POST", "/api/workspaces/main/seed");
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/assignee", {
      commandId: "assign-1",
      assigneeId: "ada",
    });

    clock += 1_000;
    expect(await instance.host.sweepIdle()).toContainEqual(workspaceKey("main"));
    expect(instance.host.openPartitions()).toHaveLength(0);

    // Nothing is open, so B4's exchange would have had no source at all.
    const reports = await instance.host.exchange();
    expect(reports.some((report) => report.source.id === "main" && report.applied === 1)).toBe(
      true,
    );
    expect((await inbox(instance, "ada")).rows).toHaveLength(1);
  });

  test("the registry survives a whole-host restart and keeps the source", async () => {
    const directory = temporaryDirectory("i2-registry");
    let clock = 1_000;
    const first = track(host({ databaseDirectory: directory, now: () => clock }));
    await call(first, "POST", "/api/workspaces/main/seed");
    await call(first, "POST", "/api/workspaces/main/issues/seed-plan/assignee", {
      commandId: "assign-1",
      assigneeId: "ada",
    });
    await first.host.exchange();
    await first.close();
    open.length = 0;

    clock += 1_000;
    const second = track(host({ databaseDirectory: directory, now: () => clock }));
    // Nothing has opened the workspace in this process, so the source can only
    // come from the durable registry.
    const sources = await call(second, "GET", "/api/global/sources");
    expect(await sources.json()).toMatchObject({
      sources: [{ partition: "workspace:main", kind: "workspace", id: "main" }],
    });

    const reports = await second.host.exchange();
    expect(reports.some((report) => report.source.id === "main")).toBe(true);
    // The cursor survived too, so nothing is re-applied.
    expect(reports.every((report) => report.applied === 0)).toBe(true);
    expect((await inbox(second, "ada")).rows).toHaveLength(1);
  });

  test("restarting the destination and the global partition preserves inbox and cursor", async () => {
    const directory = temporaryDirectory("i2-domains");
    const instance = track(host({ databaseDirectory: directory }));
    await call(instance, "POST", "/api/workspaces/main/seed");
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/assignee", {
      commandId: "assign-1",
      assigneeId: "ada",
    });
    await instance.host.exchange();
    const before = await inbox(instance, "ada");
    const cursorBefore = await json(
      await call(instance, "GET", "/api/global/exchange"),
      ExchangeStatusResponse,
    );

    expect(await instance.host.restart(userKey("ada"))).toBe(true);
    expect(await instance.host.restart(globalKey())).toBe(true);

    expect(await inbox(instance, "ada")).toEqual(before);
    expect(
      await json(await call(instance, "GET", "/api/global/exchange"), ExchangeStatusResponse),
    ).toEqual(cursorBefore);
    const again = await instance.host.exchange();
    expect(again.every((report) => report.applied === 0)).toBe(true);
  });

  test("the cursor domain is still its own, with three products in the same host", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/assignee", {
      commandId: "assign-1",
      assigneeId: "ada",
    });
    await instance.host.exchange();
    const status = await json(
      await call(instance, "GET", "/api/global/exchange"),
      ExchangeStatusResponse,
    );
    const cursor = status.cursors[0];
    expect(cursor?.domain).toBe("issue-tracker.exchange-cursor/1");
    expect(Number.isInteger(cursor?.arrival)).toBe(true);
    // A native offset would not be an integer arrival count, and a store
    // checkpoint would carry `epoch`/`sequence` instead.
    expect(Object.keys(cursor ?? {})).not.toContain("epoch");
  });
});

/**
 * A client that fails every append to a workspace's transition feed.
 *
 * The state lives in a closure rather than on an instance: these fixtures are
 * one flag and one wrapper, and a class would only add a `this` to get wrong.
 */
function dropFeedAppends() {
  const state = { dropping: false };
  return {
    get dropping() {
      return state.dropping;
    },
    set dropping(value: boolean) {
      state.dropping = value;
    },
    wrap: (inner: StreamProtocolClient): StreamProtocolClient => ({
      stream: (streamId: string): StreamProtocolHandle => {
        const handle = inner.stream(streamId);
        const isFeed = streamId.endsWith("/issue-transitions");
        return {
          id: handle.id,
          head: (options) => handle.head(options),
          create: (options) => handle.create(options),
          append: (data: Uint8Array | string, options?: AppendStreamOptions) =>
            handle.append(data, options),
          appendJsonBatch: (
            items: readonly JsonValue[],
            options?: AppendJsonBatchOptions,
          ): Promise<ClientAppendResult> =>
            // A transport failure on the feed is what a crash between the row
            // commit and the feed append looks like from here: the rows are
            // already durable, and the publication marker never moves.
            state.dropping && isFeed
              ? Promise.resolve({
                  status: "error",
                  code: "transport",
                  message: "feed append lost",
                  retryable: true,
                } satisfies ClientAppendResult)
              : handle.appendJsonBatch(items, options),
          close: (options) => handle.close(options),
          read: (options) => handle.read(options),
        };
      },
      close: (cause?: unknown) => inner.close(cause),
    }),
  };
}

/**
 * A client whose first feed append commits and then reports a transport error.
 *
 * That is a crash *after* the durable write and before the marker moved, which
 * is the one window a producer lane exists to make safe.
 */
function loseFeedAppendResponse() {
  const state = { lose: false };
  return {
    get lose() {
      return state.lose;
    },
    set lose(value: boolean) {
      state.lose = value;
    },
    wrap: (inner: StreamProtocolClient): StreamProtocolClient => ({
      stream: (streamId: string): StreamProtocolHandle => {
        const handle = inner.stream(streamId);
        const isFeed = streamId.endsWith("/issue-transitions");
        return {
          id: handle.id,
          head: (options) => handle.head(options),
          create: (options) => handle.create(options),
          append: (data: Uint8Array | string, options?: AppendStreamOptions) =>
            handle.append(data, options),
          appendJsonBatch: async (
            items: readonly JsonValue[],
            options?: AppendJsonBatchOptions,
          ): Promise<ClientAppendResult> => {
            const result = await handle.appendJsonBatch(items, options);
            if (state.lose && isFeed && result.status === "appended") {
              state.lose = false;
              return {
                status: "error",
                code: "transport",
                message: "feed response lost after commit",
                retryable: true,
              } satisfies ClientAppendResult;
            }
            return result;
          },
          close: (options) => handle.close(options),
          read: (options) => handle.read(options),
        };
      },
      close: (cause?: unknown) => inner.close(cause),
    }),
  };
}
