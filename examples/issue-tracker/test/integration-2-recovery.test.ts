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
import type { OutboxStore } from "@streamsy/effect-sink";
import type { SourceChanges } from "@streamsy/views-engine";
import { Effect, Layer, Schema } from "effect";
import { issueLabelMemberships, labels } from "../domain/declaration.ts";
import { globalKey, userKey, workspaceKey } from "../domain/domains.ts";
import { ExchangeStatusResponse, InboxResponse, TransitionFeedResponse } from "../shared/api.ts";
import { IssueLabelsResponse, IssuesResponse, LabelCountsResponse } from "../shared/api.ts";
import { StoreUnavailable } from "../server/errors.ts";
import { IssueStore, memoryLayer } from "../server/store.ts";
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

/**
 * Graph-product input delivery across a mid-pass failure.
 *
 * A maintenance pass commits its source relations one at a time and only then
 * folds them into the two operator graphs. Everything in between — the
 * transition feed, the catalog catch-up, the sink appends — can fail, and when
 * it did the changes those graphs still owed existed nowhere but in the pass's
 * local variables. The next pass read an empty suffix, so a published product
 * stayed wrong until the same key happened to change again.
 *
 * Each test here commits one input leg, fails the pass before the graph that
 * consumes it, and then reads the *published product* rather than the relation
 * the pass folded. The relations always recovered; these are the assertions
 * that were missing.
 */
describe("graph products recover inputs committed by a failed pass", () => {
  test("the board sink serves an issue change committed before a mid-pass failure", async () => {
    const dropped = dropFeedAppends();
    const instance = track(host({ applicationClient: (client) => dropped.wrap(client) }));
    await call(instance, "POST", "/api/workspaces/main/seed");

    // The row commit lands, the feed append does not, and the pass dies before
    // the board graph has folded anything.
    dropped.dropping = true;
    expect((await move(instance, "seed-plan", "todo", "lost-1")).status).toBe(503);
    dropped.dropping = false;

    // The relation recovered — it always did.
    const rows = await json(
      await call(instance, "GET", "/api/workspaces/main/issues"),
      IssuesResponse,
    );
    expect(rows.rows.find((row) => row.issueId === "seed-plan")?.status).toBe("todo");

    // The published product recovers too, which is the new law.
    const card = latest(await stateMessages(instance, BOARD_SINK), "issue", "seed-plan");
    expect(card?.value?.status).toBe("todo");
  });

  test("the label-count sink serves a membership committed before a mid-pass failure", async () => {
    /**
     * The membership relation commits on its own checkpoint, several steps
     * before either graph folds anything. Refusing the label-count commit for
     * exactly the pass that is delivering that membership puts the failure in
     * the window the product used to lose it in.
     */
    const faulty: LabelCountFault = {};
    const instance = track(host({ store: faultyLabelCountStore(faulty) }));
    await call(instance, "POST", "/api/workspaces/main/seed");
    expect(countOf(await counts(instance), "docs")).toBe(1);

    faulty.refuse = delivering(issueLabelMemberships.name);
    const attached = await call(
      instance,
      "POST",
      "/api/workspaces/main/issues/seed-maintain/labels",
      { commandId: "lost-attach-1", labelId: "docs" },
    );
    expect(attached.status).toBe(503);

    // The membership relation itself is committed and complete.
    faulty.refuse = undefined;
    const memberships = await json(
      await call(instance, "GET", "/api/workspaces/main/issue-labels"),
      IssueLabelsResponse,
    );
    expect(
      memberships.rows.some(
        (row) => row.issueId === "seed-maintain" && row.labelId === "docs" && row.attached,
      ),
    ).toBe(true);

    // And the product that never saw it catches up from committed history.
    expect(countOf(await counts(instance), "docs")).toBe(2);
    const published = latest(
      await stateMessages(instance, LABEL_COUNT_SINK),
      "label-count",
      "docs",
    );
    expect(published?.value?.issueCount).toBe(2);
  });

  test("the label-count sink serves catalog data ingested before a mid-pass failure", async () => {
    /**
     * The catalog leg's window is the one the other two fixtures cannot reach:
     * ingestion is the *last* thing a pass commits before the graphs fold, so
     * the failure has to sit between that commit and the label-count graph's
     * own. The store seam the host already exposes is what puts it there.
     */
    const faulty: LabelCountFault = {};
    const instance = track(host({ store: faultyLabelCountStore(faulty) }));
    await call(instance, "POST", "/api/workspaces/main/seed");
    expect(nameOf(await counts(instance), "docs")).toBe("Docs");

    faulty.refuse = delivering(labels.name);
    const renamed = await call(instance, "POST", "/api/workspaces/main/catalog/labels", {
      key: "docs",
      value: {
        labelId: "docs",
        workspaceId: "main",
        name: "Documentation",
        color: "#4573d6",
        updatedAt: "2026-08-26T00:00:00.000Z",
      },
    });
    expect(renamed.status).toBe(503);

    // The catalog collection itself is committed and serves the new name.
    faulty.refuse = undefined;
    const catalogRows = await call(instance, "GET", "/api/workspaces/main/catalog/labels");
    expect(await catalogRows.json()).toMatchObject({
      rows: expect.arrayContaining([
        expect.objectContaining({ labelId: "docs", name: "Documentation" }),
      ]),
    });

    // And so does the joined product, which never saw the ingestion at all.
    expect(nameOf(await counts(instance), "docs")).toBe("Documentation");
    const published = latest(
      await stateMessages(instance, LABEL_COUNT_SINK),
      "label-count",
      "docs",
    );
    expect(published?.value?.labelName).toBe("Documentation");
  });

  test("a product only re-reads what it has not consumed, so a quiet pass publishes nothing", async () => {
    /**
     * At-least-once delivery is only safe if it converges. The positions are
     * what make it converge: a pass with no new committed input delivers no
     * input, so the graph revision does not move and the sink is not appended
     * to. Two consecutive quiet passes leave the published stream byte-equal.
     */
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    const first = await stateMessages(instance, LABEL_COUNT_SINK);
    await counts(instance);
    await counts(instance);
    expect(await stateMessages(instance, LABEL_COUNT_SINK)).toEqual(first);
  });
});

const BOARD_SINK = "/state/workspaces/main/issues";
const LABEL_COUNT_SINK = "/state/workspaces/main/label-counts";

const PublishedStateMessage = Schema.Struct({
  type: Schema.optionalKey(Schema.String),
  key: Schema.optionalKey(Schema.String),
  value: Schema.optionalKey(
    Schema.Struct({
      status: Schema.optionalKey(Schema.String),
      labelName: Schema.optionalKey(Schema.String),
      issueCount: Schema.optionalKey(Schema.Finite),
    }),
  ),
});
interface PublishedStateMessage extends Schema.Schema.Type<typeof PublishedStateMessage> {}

/** Every message the sink's own State stream currently carries. */
async function stateMessages(
  instance: Host,
  path: string,
): Promise<readonly PublishedStateMessage[]> {
  const response = await call(instance, "GET", path);
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  return Schema.decodePromise(Schema.fromJsonString(Schema.Array(PublishedStateMessage)))(
    await response.text(),
  );
}

/** The current value of one key: the last message the stream carries for it. */
const latest = (
  messages: readonly PublishedStateMessage[],
  type: string,
  key: string,
): PublishedStateMessage | undefined =>
  messages.filter((message) => message.type === type && message.key === key).at(-1);

const countOf = (listed: LabelCountsResponse, labelId: string): number | undefined =>
  listed.rows.find((row) => row.labelId === labelId)?.issueCount;
const nameOf = (listed: LabelCountsResponse, labelId: string): string | undefined =>
  listed.rows.find((row) => row.labelId === labelId)?.labelName;

/**
 * A memory store whose label-count graph maintenance can be made to fail.
 *
 * It wraps the real store rather than replacing it, so everything the pass does
 * before the graph — the issue commit, the membership commit, the catalog
 * commit — is the production path, and only the one step under test refuses.
 */
interface LabelCountFault {
  /** Refuse the label-count graph commit for exactly the delivery under test. */
  refuse?: (inputs: readonly SourceChanges[]) => boolean;
}

function faultyLabelCountStore(state: LabelCountFault): Layer.Layer<IssueStore | OutboxStore> {
  const wrapped = Layer.effect(
    IssueStore,
    Effect.gen(function* () {
      const inner = yield* IssueStore;
      return IssueStore.of({
        ...inner,
        maintainLabelCounts: (workspaceId, inputs, positions) =>
          state.refuse?.(inputs) === true
            ? Effect.fail(
                new StoreUnavailable({
                  operation: "maintainLabelCounts",
                  detail: "injected mid-pass failure",
                }),
              )
            : inner.maintainLabelCounts(workspaceId, inputs, positions),
      });
    }),
  );
  return wrapped.pipe(Layer.provideMerge(memoryLayer()));
}

/** Refuse only a pass that is actually delivering this source's changes. */
const delivering =
  (sourceId: string) =>
  (inputs: readonly SourceChanges[]): boolean =>
    inputs.some((input) => input.sourceId === sourceId && input.changes.length > 0);
