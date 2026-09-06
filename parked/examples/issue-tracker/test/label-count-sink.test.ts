/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is the second checked State route behind the host's Web `fetch` handler. */
/**
 * The label-count `stateSink`'s public contract.
 *
 * It is the tracker's second checked State product, and the point of this
 * suite is that it is a *contract* rather than an endpoint that happens to
 * return counts: its own route, its own collection and wire type, its own
 * fingerprint, native-offset resume, and the declared reset-first fallback for
 * an offset the transport cannot honour.
 *
 * The board sink's own suite is unchanged and still passes, which is the other
 * half of the claim: adding a second sink moved nothing about the first.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { Schema } from "effect";
import { boardIssues, boardLabelCounts } from "../domain/declaration.ts";
import { LabelCountsResponse, SinkSessionResponse } from "../shared/api.ts";
import { call, host, json, type Host } from "./support.ts";

const SINK = "/state/workspaces/main/label-counts";
const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});
function fresh(): Host {
  const created = host();
  open.push(created);
  return created;
}

const StateMessage = Schema.Struct({
  type: Schema.optionalKey(Schema.String),
  key: Schema.optionalKey(Schema.String),
  value: Schema.optionalKey(
    Schema.Struct({
      labelName: Schema.optionalKey(Schema.String),
      issueCount: Schema.optionalKey(Schema.Finite),
    }),
  ),
  headers: Schema.optionalKey(
    Schema.Struct({
      control: Schema.optionalKey(Schema.String),
      operation: Schema.optionalKey(Schema.String),
    }),
  ),
});
interface StateMessage extends Schema.Schema.Type<typeof StateMessage> {}
const StateMessages = Schema.Array(StateMessage);

async function read(
  instance: Host,
  query: string,
): Promise<{
  status: number;
  messages: readonly StateMessage[];
  offset: string | undefined;
  body: string;
}> {
  const response = await instance.fetch(new Request(`http://localhost${SINK}${query}`));
  const body = await response.text();
  return {
    status: response.status,
    messages: response.ok ? Schema.decodeSync(Schema.fromJsonString(StateMessages))(body) : [],
    offset: response.headers.get("stream-next-offset") ?? undefined,
    body,
  };
}

describe("the board-label-counts state sink", () => {
  test("it is a second contract, not a second view of the first", () => {
    expect(boardLabelCounts.route).toBe("/state/workspaces/:workspaceId/label-counts");
    expect(boardLabelCounts.collection).toEqual({
      name: "labelCounts",
      type: "label-count",
      primaryKey: "labelId",
    });
    expect(boardLabelCounts.protocol).toEqual({
      sessionVersion: 1,
      durableStateVersion: 1,
      transport: "durable-state",
      resume: true,
      fallback: "snapshot-then-live",
    });
    expect(boardLabelCounts.fingerprint).toMatch(/^[0-9a-f]{8}$/);
    expect(boardLabelCounts.fingerprint).not.toBe(boardIssues.fingerprint);
  });

  test("a fresh session reads a snapshot of the maintained counts", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");

    const snapshot = await read(instance, "");
    expect(snapshot.status).toBe(200);
    const upserts = snapshot.messages.filter((message) => message.type === "label-count");
    const infra = upserts.findLast((message) => message.key === "infra");
    expect(infra?.value?.issueCount).toBe(2);
    expect(snapshot.offset).toBeString();
  });

  test("a native offset replays exactly the suffix a command appended", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const before = await read(instance, "");
    const offset = before.offset;
    expect(offset).toBeString();

    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/labels", {
      commandId: "attach-1",
      labelId: "bug",
    });

    const suffix = await read(instance, `?offset=${encodeURIComponent(offset ?? "")}`);
    expect(suffix.status).toBe(200);
    const counted = suffix.messages.filter((message) => message.type === "label-count");
    expect(counted).toHaveLength(1);
    expect(counted[0]?.key).toBe("bug");
    expect(counted[0]?.value?.issueCount).toBe(2);
  });

  test("resuming at the tail replays nothing", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const tail = (await read(instance, "")).offset;
    const again = await read(instance, `?offset=${encodeURIComponent(tail ?? "")}`);
    expect(again.messages.filter((message) => message.type === "label-count")).toHaveLength(0);
  });

  test("an offset the transport cannot honour declares the reset-first fallback", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const rejected = await read(instance, "?offset=not-an-offset");
    expect(rejected.status).toBeGreaterThanOrEqual(400);
    expect(rejected.body).toContain("snapshot-then-live");
  });

  test("an undecodable workspace parameter is the sink's own typed refusal", async () => {
    const instance = fresh();
    const response = await instance.fetch(
      new Request("http://localhost/state/workspaces/not%20a%20workspace/label-counts"),
    );
    expect(response.status).toBe(400);
    // The refusal names the sink the caller asked for, not whichever route
    // shape was tried first.
    expect(await response.text()).toContain(boardLabelCounts.name);
  });

  test("each workspace's counts are served from its own partition", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    await call(instance, "POST", "/api/workspaces/other/seed");
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/labels", {
      commandId: "attach-main",
      labelId: "bug",
    });

    const mine = await read(instance, "");
    const theirs = await instance.fetch(
      new Request("http://localhost/state/workspaces/other/label-counts"),
    );
    const theirMessages = await Schema.decodePromise(Schema.fromJsonString(StateMessages))(
      await theirs.text(),
    );
    expect(mine.messages.filter((m) => m.key === "bug").at(-1)?.value?.issueCount).toBe(2);
    expect(theirMessages.filter((m) => m.key === "bug").at(-1)?.value?.issueCount).toBe(1);
  });

  test("the sink session names both of the workspace's checked State products", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const session = await json(
      await call(instance, "GET", "/api/workspaces/main/sink-session"),
      SinkSessionResponse,
    );
    expect(session.contractFingerprint).toBe(boardIssues.fingerprint);
    expect(session.labelCounts.sink).toBe(boardLabelCounts.name);
    expect(session.labelCounts.route).toBe("/state/workspaces/main/label-counts");
    expect(session.labelCounts.contractFingerprint).toBe(boardLabelCounts.fingerprint);
    expect(session.labelCounts.offset).toBeString();
  });

  test("the checked sink and the read model report the same counts", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/labels", {
      commandId: "attach-1",
      labelId: "bug",
    });

    const published = new Map<string, number>();
    for (const message of (await read(instance, "")).messages) {
      if (message.type !== "label-count" || message.key === undefined) continue;
      published.set(message.key, message.value?.issueCount ?? 0);
    }
    const model = await json(
      await call(instance, "GET", "/api/workspaces/main/label-counts"),
      LabelCountsResponse,
    );
    const { rows } = model;
    for (const row of rows) expect(published.get(row.labelId)).toBe(row.issueCount);
  });
});
