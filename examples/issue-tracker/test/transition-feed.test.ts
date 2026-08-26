/**
 * The `streamSink`'s public contract.
 *
 * The feed's whole promise is that it is in the order the fold observed, so the
 * load-bearing case here is an out-of-domain-sequence arrival: facts whose
 * `sequence` fields disagree with the order they were appended in. The feed
 * must follow the appends, and the maintained row must agree with the last
 * transition the feed published.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { issueTransitions, streamNames } from "../domain/declaration.ts";
import { TransitionFeedResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const FEED = "/feed/workspaces/main/issue-transitions";
const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function fresh(): Host {
  const created = host();
  open.push(created);
  return created;
}

async function feed(instance: Host, query = ""): Promise<Response> {
  return instance.fetch(new Request(`http://localhost${FEED}${query}`));
}

/** Append a canonical fact straight to the durable source, bypassing the command path. */
async function appendFact(
  instance: Host,
  event: Readonly<Record<string, string | number>>,
): Promise<void> {
  const stream = instance.client.stream(streamNames.issueEvents("main"));
  await stream.create({ contentType: "application/json" });
  const appended = await stream.append(JSON.stringify(event), {
    contentType: "application/json",
  });
  expect(appended.status).toBe("appended");
}

const created = (issueId: string, sequence: number, status: string, occurredAt: string) => ({
  type: "IssueCreated",
  eventId: `e-${issueId}-${sequence}`,
  workspaceId: "main",
  issueId,
  sequence,
  occurredAt,
  title: `Issue ${issueId}`,
  projectId: "streamsy",
  status,
});

const moved = (issueId: string, sequence: number, status: string, occurredAt: string) => ({
  type: "IssueStatusChanged",
  eventId: `e-${issueId}-${sequence}`,
  workspaceId: "main",
  issueId,
  sequence,
  occurredAt,
  status,
});

describe("the issue-transitions stream sink", () => {
  /**
   * Wave B-i cross-track contract.
   *
   * The feed publishes changes to `issue-tracker.issues`, not status
   * transitions. B2's assignment fact changes the row without changing its
   * status, so it reaches the feed as an `update` whose `previousStatus` equals
   * its `status`. That is the honest signal for a row-change feed: nothing is
   * dropped, and a consumer that wants only status moves filters on
   * `previousStatus !== status`, which this test also pins.
   *
   * The transition deliberately does not carry the assignee. Naming *what*
   * changed is an enrichment of the wire contract, and it is recorded for the
   * Integration 2 review rather than taken here.
   */
  test("an assignment reaches the feed as a row change that moved no status", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("c-1", "issue-1", "First", "todo"),
    );
    await call(instance, "POST", "/api/workspaces/main/issues/issue-1/assignee", {
      commandId: "c-2",
      assigneeId: "ada",
    });

    const body = await json(await feed(instance), TransitionFeedResponse);
    const events = body.events.filter((event) => event.issueId === "issue-1");
    expect(events.map((event) => event.change)).toEqual(["enter", "update"]);

    const assignment = events[1];
    expect(assignment?.status).toBe("todo");
    expect(assignment?.previousStatus).toBe("todo");

    // The status-move projection a consumer builds from the same feed.
    expect(
      events.filter((event) => event.change === "update" && event.previousStatus !== event.status),
    ).toEqual([]);
  });

  test("publishes one transition per maintained change, in command order", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "First", "backlog"),
    );
    await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
      commandId: "cmd-2",
      status: "todo",
    });
    await call(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
      commandId: "cmd-3",
      status: "done",
    });

    const body = await json(await feed(instance), TransitionFeedResponse);
    expect(body.sink).toBe(issueTransitions.name);
    expect(body.order).toBe("arrival");
    expect(body.feed).toMatchObject({ name: "issue-transitions", subjectKey: "issueId" });
    expect(body.events.map((event) => `${event.change}:${event.status}`)).toEqual([
      "enter:backlog",
      "update:todo",
      "update:done",
    ]);
    expect(body.events[1]?.previousStatus).toBe("backlog");
    expect(body.events.every((event) => event.issueId === "issue-1")).toBe(true);
  });

  test("follows fact arrival order, not domain sequence order", async () => {
    const instance = fresh();
    await call(instance, "GET", "/api/workspaces/main/issues");

    await appendFact(instance, created("issue-1", 0, "backlog", "2026-08-25T10:00:00.000Z"));
    await call(instance, "GET", "/api/workspaces/main/issues");
    // Sequence 2 arrives before sequence 1. The fold observes arrival order, so
    // the feed must show `done` and then `todo`, and the row must end at `todo`.
    await appendFact(instance, moved("issue-1", 2, "done", "2026-08-25T10:02:00.000Z"));
    await call(instance, "GET", "/api/workspaces/main/issues");
    await appendFact(instance, moved("issue-1", 1, "todo", "2026-08-25T10:01:00.000Z"));

    const body = await json(await feed(instance), TransitionFeedResponse);
    expect(body.events.map((event) => event.status)).toEqual(["backlog", "done", "todo"]);
    expect(body.events.map((event) => event.occurredAt)).toEqual([
      "2026-08-25T10:00:00.000Z",
      "2026-08-25T10:02:00.000Z",
      "2026-08-25T10:01:00.000Z",
    ]);

    const rows = await (await call(instance, "GET", "/api/workspaces/main/issues")).json();
    expect(rows).toMatchObject({ rows: [{ issueId: "issue-1", status: "todo" }] });
  });

  test("a native offset replays exactly the suffix appended after it", async () => {
    const instance = fresh();
    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "First"),
    );
    const first = await json(await feed(instance), TransitionFeedResponse);
    expect(first.upToDate).toBe(true);

    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-2", "issue-2", "Second"),
    );
    const suffix = await json(
      await feed(instance, `?offset=${encodeURIComponent(first.nextOffset)}`),
      TransitionFeedResponse,
    );
    expect(suffix.events.map((event) => event.issueId)).toEqual(["issue-2"]);
    expect(suffix.nextOffset).not.toBe(first.nextOffset);

    const tail = await json(
      await feed(instance, `?offset=${encodeURIComponent(suffix.nextOffset)}`),
      TransitionFeedResponse,
    );
    expect(tail.events).toEqual([]);
  });

  test("an unusable resume position declares replay-from-start", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const refused = await feed(instance, "?offset=not-an-offset");
    expect(refused.status).toBe(409);
    expect(await refused.json()).toMatchObject({
      _tag: "ResumeRejected",
      sink: issueTransitions.name,
      reason: "invalid-offset",
      recovery: "replay-from-start",
    });

    const rebuilt = await json(await feed(instance, "?offset=-1"), TransitionFeedResponse);
    expect(rebuilt.events).toHaveLength(4);
  });

  test("the feed rejects a retired contract and a write method", async () => {
    const instance = fresh();
    const retired = await instance.fetch(
      new Request(`http://localhost${FEED}`, {
        headers: { "x-streamsy-stream-sink-contract": "retired" },
      }),
    );
    expect(retired.status).toBe(409);
    expect(await retired.json()).toMatchObject({ reason: "contract-changed" });

    const written = await instance.fetch(
      new Request(`http://localhost${FEED}`, { method: "POST" }),
    );
    expect(written.status).toBe(405);
  });

  test("the declared route and contract are what the host serves", async () => {
    expect(issueTransitions.compiledRoute.build({ workspaceId: "main" })).toBe(FEED);
    expect(issueTransitions.key).toBe("issueId");
    expect(issueTransitions.from.of.name).toBe("issue-tracker.issues");
    expect(issueTransitions.fingerprint).toMatch(/^[0-9a-f]{8}$/);
    const instance = fresh();
    const response = await feed(instance);
    expect(response.headers.get("x-streamsy-stream-sink-contract")).toBe(
      issueTransitions.fingerprint,
    );
  });
});
