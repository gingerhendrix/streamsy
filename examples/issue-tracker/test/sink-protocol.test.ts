/* oxlint-disable effecttsgo/async-function -- `bun:test` owns this file's control flow; the behaviour under test is the sink route behind the host's Web `fetch` handler. */
/**
 * The `stateSink`'s public contract.
 *
 * Everything the declaration promises — the route, the scope, the Durable State
 * transport, `resume: true`, and `fallback: "snapshot-then-live"` — is checked
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

function fresh(options: { readonly resumeTokenTtlSeconds?: number } = {}): Host {
  const created = host(options);
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
): Promise<{ status: number; messages: StateMessage[]; resume: string | undefined; body: string }> {
  const response = await instance.fetch(new Request(`http://localhost${SINK}${query}`));
  const body = await response.text();
  return {
    status: response.status,
    // SAFETY: a 2xx from the sink route is a Durable State message array, and
    // `StateMessage` names only the optional fields these assertions read.
    // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
    messages: response.ok ? (JSON.parse(body) as StateMessage[]) : [],
    resume: response.headers.get("x-streamsy-resume") ?? undefined,
    body,
  };
}

describe("the board-issues state sink", () => {
  test("the declared scope is required", async () => {
    const instance = fresh();
    const denied = await read(instance, "");
    expect(denied.status).toBe(403);
    expect(JSON.parse(denied.body)).toMatchObject({ error: "unauthorized", detail: SCOPE });
  });

  test("a fresh session reads a snapshot bounded by control messages", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");

    const snapshot = await read(instance, `?scope=${SCOPE}`);
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
    expect(snapshot.resume).toBeString();
  });

  test("a resume token replays only the suffix appended after it", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Already seen"),
    );

    const first = await read(instance, `?scope=${SCOPE}`);
    expect(first.messages.some((message) => message.key === "issue-1")).toBe(true);
    const token = first.resume;
    expect(token).toBeString();

    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-2", "issue-2", "Appended later"),
    );

    const suffix = await read(
      instance,
      `?scope=${SCOPE}&resume=${encodeURIComponent(token ?? "")}`,
    );
    expect(suffix.status).toBe(200);
    const keys = suffix.messages
      .filter((message) => message.type === "issue")
      .map((message) => message.key);
    expect(keys).toEqual(["issue-2"]);
    expect(suffix.resume).not.toBe(token);
  });

  test("an expired token is a typed failure carrying the declared fallback", async () => {
    // A zero-second lifetime makes the token stale the moment it is minted, so
    // the expiry path is reachable without waiting on a clock.
    const instance = fresh({ resumeTokenTtlSeconds: 0 });
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Present"),
    );
    const session = await json(
      await call(instance, "GET", "/api/workspaces/main/sink-session"),
      SinkSessionResponse,
    );

    const rejected = await read(
      instance,
      `?scope=${SCOPE}&resume=${encodeURIComponent(session.resume)}`,
    );
    expect(rejected.status).toBe(409);
    expect(JSON.parse(rejected.body)).toMatchObject({
      error: "resume-expired",
      detail: "expired",
      fallback: "snapshot-then-live",
    });

    // The declared fallback works: reading without the token rebuilds the whole
    // product.
    const rebuilt = await read(instance, `?scope=${SCOPE}`);
    expect(rebuilt.status).toBe(200);
    expect(rebuilt.messages.some((message) => message.key === "issue-1")).toBe(true);
  });

  test("a tampered token is refused rather than trusted", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const session = await json(
      await call(instance, "GET", "/api/workspaces/main/sink-session"),
      SinkSessionResponse,
    );
    const tampered = `${session.resume.split(".")[0] ?? ""}.not-the-signature`;
    const refused = await read(instance, `?scope=${SCOPE}&resume=${encodeURIComponent(tampered)}`);
    expect(refused.status).toBe(409);
    expect(JSON.parse(refused.body)).toMatchObject({ detail: "malformed" });
  });

  test("a token minted for another workspace is refused", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/other/seed");
    const session = await json(
      await call(instance, "GET", "/api/workspaces/other/sink-session"),
      SinkSessionResponse,
    );
    const refused = await read(
      instance,
      `?scope=${SCOPE}&resume=${encodeURIComponent(session.resume)}`,
    );
    expect(refused.status).toBe(409);
    expect(JSON.parse(refused.body)).toMatchObject({ detail: "wrong-sink" });
  });

  test("the session contract reports the declaration, not a restatement of it", async () => {
    const instance = fresh();
    const session = await json(
      await call(instance, "GET", "/api/workspaces/main/sink-session"),
      SinkSessionResponse,
    );
    expect(session).toMatchObject({
      sink: "issue-tracker.board-issues",
      route: SINK,
      transport: "durable-state",
      fallback: "snapshot-then-live",
      scope: SCOPE,
    });
  });
});
