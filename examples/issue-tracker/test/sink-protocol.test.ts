/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is the sink route behind the host's Web `fetch` handler. */
/**
 * The `stateSink`'s public contract.
 *
 * Everything the declaration promises — the route, the scope, the Durable State
 * transport, native offset resume, and `fallback: "snapshot-then-live"` — is checked
 * here against the real route, not a description of it.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { SinkSessionResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const SINK = "/state/workspaces/main/issues";
const SCOPE = "issue-tracker:workspace";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function fresh(): Host {
  const created = host();
  open.push(created);
  return created;
}

interface StateMessage {
  readonly type?: string;
  readonly key?: string;
  readonly value?: { readonly status?: string; readonly title?: string };
  readonly headers?: { readonly control?: string; readonly operation?: string };
}

async function read(
  instance: Host,
  query: string,
  headers: HeadersInit = { "x-streamsy-scope": SCOPE },
): Promise<{ status: number; messages: StateMessage[]; offset: string | undefined; body: string }> {
  const response = await instance.fetch(
    new Request(`http://localhost${SINK}${query}`, { headers }),
  );
  const body = await response.text();
  return {
    status: response.status,
    // SAFETY: a 2xx from the sink route is a Durable State message array, and
    // `StateMessage` names only the optional fields these assertions read.
    // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
    messages: response.ok ? (JSON.parse(body) as StateMessage[]) : [],
    offset: response.headers.get("stream-next-offset") ?? undefined,
    body,
  };
}

describe("the board-issues state sink", () => {
  test("the declared scope is required", async () => {
    const instance = fresh();
    const denied = await read(instance, "", {});
    expect(denied.status).toBe(403);
    expect(JSON.parse(denied.body)).toMatchObject({
      _tag: "SinkUnauthorized",
      required: SCOPE,
    });
  });

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
      "x-streamsy-scope": SCOPE,
      "x-streamsy-state-sink-reset": "snapshot",
    });
    expect(rebuilt.status).toBe(200);
    expect(rebuilt.messages[0]?.headers?.control).toBe("reset");
    expect(rebuilt.messages.some((message) => message.key === "seed-plan")).toBe(true);
  });

  test("protocol and authorization-generation changes declare reset policy", async () => {
    const instance = fresh();
    const protocol = await read(instance, "", {
      "x-streamsy-scope": SCOPE,
      "x-streamsy-state-sink-version": "2",
    });
    expect(protocol.status).toBe(409);
    expect(JSON.parse(protocol.body)).toMatchObject({
      _tag: "ProtocolVersionUnsupported",
      supported: 1,
      recovery: "snapshot-then-live",
    });

    const generation = await read(instance, "?offset=0_0", {
      "x-streamsy-scope": SCOPE,
      "x-streamsy-authorization-generation": "retired",
    });
    expect(generation.status).toBe(409);
    expect(JSON.parse(generation.body)).toMatchObject({
      _tag: "ResumeRejected",
      reason: "authorization-generation-changed",
    });

    const contract = await read(instance, "", {
      "x-streamsy-scope": SCOPE,
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
      await instance.fetch(
        new Request("http://localhost/api/workspaces/main/sink-session", {
          headers: { "x-streamsy-scope": SCOPE },
        }),
      ),
      SinkSessionResponse,
    );
    expect(session).toMatchObject({
      sink: "issue-tracker.board-issues",
      route: SINK,
      transport: "durable-state",
      fallback: "snapshot-then-live",
      required: SCOPE,
      protocolVersion: 1,
      durableStateVersion: 1,
      authorizationGeneration: "local-v1",
    });
  });
});
