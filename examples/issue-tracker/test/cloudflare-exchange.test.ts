/* oxlint-disable effecttsgo/async-function -- bun:test and workerd expose Promise-native edges. */
import { afterEach, describe, expect, test } from "bun:test";
import { Schema } from "effect";
import { workerdHarness, jsonRequest, type WorkerdHarness } from "./cloudflare-support.ts";

const open: WorkerdHarness[] = [];
afterEach(async () => { await Promise.all(open.splice(0).map((harness) => harness.close())); });
const fresh = async () => { const harness = await workerdHarness(); open.push(harness); return harness; };
const InboxBody = Schema.Struct({ userId: Schema.String, rows: Schema.Array(Schema.Struct({
  workspaceId: Schema.String, issueId: Schema.String, userId: Schema.String,
})) });
const CursorBody = Schema.Struct({ cursors: Schema.Array(Schema.Struct({
  source: Schema.Struct({ kind: Schema.String, id: Schema.String }), applied: Schema.Number,
})) });
const decode = <S extends Schema.ConstraintDecoder<unknown>>(schema: S, value: unknown): S["Type"] =>
  Schema.decodeUnknownSync(schema)(value);
async function eventually<A>(read: () => Promise<A | undefined>, timeoutMs = 4_000): Promise<A> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (value !== undefined) return value;
    if (Date.now() >= deadline) throw new Error(`condition not met within ${timeoutMs}ms`);
    await Bun.sleep(25);
  }
}

async function assignment(harness: WorkerdHarness, workspaceId: string, issueId: string, userId: string) {
  expect((await harness.fetch(`/api/workspaces/${workspaceId}/issues`, jsonRequest("POST", {
    commandId: `create-${issueId}`, issueId, projectId: "streamsy", title: issueId, status: "todo",
  }))).status).toBe(201);
  expect((await harness.fetch(`/api/workspaces/${workspaceId}/issues/${issueId}/assignee`, jsonRequest("POST", {
    commandId: `assign-${issueId}`, assigneeId: userId,
  }))).status).toBe(200);
}

describe("Integration 3B cross-object exchange", () => {
  test("registers a cold workspace and moves its assignment through the global alarm owner", async () => {
    const harness = await fresh();
    await assignment(harness, "cold", "issue-cold", "ada");
    await harness.evictWorkspace("cold");
    await harness.evictGlobal();
    const inbox = await eventually(async () => {
      const current = decode(InboxBody, await (await harness.fetch("/api/users/ada/inbox")).json());
      return current.rows.length === 1 ? current : undefined;
    });
    expect(inbox).toMatchObject({ userId: "ada", rows: [{ workspaceId: "cold", issueId: "issue-cold", userId: "ada" }] });
    const sources = await (await harness.fetch("/api/global/sources")).json();
    expect(sources).toMatchObject({ sources: [{ partition: "workspace:cold", kind: "workspace", id: "cold" }] });
  });

  test("source, user, and global eviction replay to one inbox row and one cursor advance", async () => {
    const harness = await fresh();
    await assignment(harness, "main", "issue-retry", "ada");
    await harness.runGlobalExchange();
    await harness.evictWorkspace("main");
    await harness.evictUser("ada");
    await harness.evictGlobal();
    await harness.runGlobalExchange();
    const inbox = decode(InboxBody, await (await harness.fetch("/api/users/ada/inbox")).json());
    expect(inbox.rows).toHaveLength(1);
    const state = decode(CursorBody, await (await harness.fetch("/api/global/exchange")).json());
    expect(state.cursors).toMatchObject([{ source: { kind: "workspace", id: "main" }, applied: 1 }]);
  });

  test("isolates two workspace sources while converging into one user partition", async () => {
    const harness = await fresh();
    await assignment(harness, "left", "issue-left", "ada");
    await assignment(harness, "right", "issue-right", "ada");
    await harness.runGlobalExchange();
    const inbox = decode(InboxBody, await (await harness.fetch("/api/users/ada/inbox")).json());
    expect(inbox.rows.map((row: { workspaceId: string }) => row.workspaceId).sort()).toEqual(["left", "right"]);
    const ids = await harness.mf.listDurableObjectIds("WorkspacePartitionObject");
    expect(ids).toHaveLength(2);
  });
});
