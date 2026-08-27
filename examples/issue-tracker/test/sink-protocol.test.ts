/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is the sink route behind the host's Web `fetch` handler. */
/**
 * The `stateSink`'s public contract.
 *
 * Everything the declaration promises — the route, the Durable State
 * transport, native offset resume, and `fallback: "snapshot-then-live"` — is checked
 * here against the real route, not a description of it.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { Schema } from "effect";
import { SinkSessionResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const SINK = "/state/workspaces/main/issues";
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
      status: Schema.optionalKey(Schema.String),
      title: Schema.optionalKey(Schema.String),
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

async function read(
  instance: Host,
  query: string,
  headers: HeadersInit = {},
): Promise<{
  status: number;
  messages: readonly StateMessage[];
  offset: string | undefined;
  body: string;
}> {
  const response = await instance.fetch(
    new Request(`http://localhost${SINK}${query}`, { headers }),
  );
  const body = await response.text();
  return {
    status: response.status,
    messages: response.ok
      ? Schema.decodeSync(Schema.fromJsonString(Schema.Array(StateMessage)))(body)
      : [],
    offset: response.headers.get("stream-next-offset") ?? undefined,
    body,
  };
}

describe("the board-issues state sink", () => {
  test("a fresh session reads a snapshot bounded by control messages", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");

    const snapshot = await read(instance, "");
    expect(snapshot.status).toBe(200);
    const controls = snapshot.messages
      .map((message) => message.headers?.control)
      .filter((control) => control !== undefined);
    expect(controls).toContain("snapshot-start");
    expect(controls).toContain("snapshot-end");

    const keys = snapshot.messages
      .filter((message) => message.type === "issue")
      .map((message) => message.key);
    expect(keys).toContain("seed-plan");
    expect(snapshot.offset).toBeString();
  });

  test("a native offset replays only the suffix appended after it", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Already seen"),
    );

    const first = await read(instance, "");
    expect(first.messages.some((message) => message.key === "issue-1")).toBe(true);
    expect(first.offset).toBeString();

    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-2", "issue-2", "Appended later"),
    );

    const suffix = await read(instance, `?offset=${encodeURIComponent(first.offset ?? "")}`);
    expect(suffix.status).toBe(200);
    const keys = suffix.messages
      .filter((message) => message.type === "issue")
      .map((message) => message.key);
    expect(keys).toEqual(["issue-2"]);
    expect(suffix.offset).not.toBe(first.offset);
  });

  test("an invalid offset declares snapshot-then-live fallback", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");

    const refused = await read(instance, "?offset=not-an-offset");
    expect(refused.status).toBe(409);
    expect(JSON.parse(refused.body)).toMatchObject({
      _tag: "ResumeRejected",
      reason: "invalid-offset",
      recovery: "snapshot-then-live",
    });

    const rebuilt = await read(instance, "?offset=-1", {
      "x-streamsy-state-sink-reset": "snapshot",
    });
    expect(rebuilt.status).toBe(200);
    expect(rebuilt.messages[0]?.headers?.control).toBe("reset");
    expect(rebuilt.messages.some((message) => message.key === "seed-plan")).toBe(true);
  });

  test("protocol and contract changes declare reset policy", async () => {
    const instance = fresh();
    const protocol = await read(instance, "", {
      "x-streamsy-state-sink-version": "2",
    });
    expect(protocol.status).toBe(409);
    expect(JSON.parse(protocol.body)).toMatchObject({
      _tag: "ProtocolVersionUnsupported",
      supported: 1,
      recovery: "snapshot-then-live",
    });

    const contract = await read(instance, "", {
      "x-streamsy-state-sink-contract": "retired-contract",
    });
    expect(contract.status).toBe(409);
    expect(JSON.parse(contract.body)).toEqual({
      _tag: "ResumeRejected",
      sink: "issue-tracker.board-issues",
      reason: "contract-changed",
      recovery: "snapshot-then-live",
    });
  });

  test("the session contract reports the declaration, not a restatement of it", async () => {
    const instance = fresh();
    const session = await json(
      await instance.fetch(new Request("http://localhost/api/workspaces/main/sink-session")),
      SinkSessionResponse,
    );
    expect(session).toMatchObject({
      sink: "issue-tracker.board-issues",
      route: SINK,
      transport: "durable-state",
      fallback: "snapshot-then-live",
      protocolVersion: 1,
      durableStateVersion: 1,
    });
  });
});
