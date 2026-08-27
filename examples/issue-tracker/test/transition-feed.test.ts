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
import { Effect, Schema } from "effect";
import { issueTransitions, streamNames } from "../domain/declaration.ts";
import { TransitionFeedResponse } from "../shared/api.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const FEED = "/feed/workspaces/main/issue-transitions";
const encodeFact = Schema.encodeSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Finite]))),
);
const open: Host[] = [];
afterEach(() =>
  Promise.all(open.splice(0).map((instance) => instance.close())).then(() => undefined),
);

function fresh(): Host {
  const created = host();
  open.push(created);
  return created;
}

const feed = (instance: Host, query = ""): Effect.Effect<Response> =>
  Effect.promise(() => instance.fetch(new Request(`http://localhost${FEED}${query}`)));

const request = (
  instance: Host,
  method: string,
  path: string,
  body?: Readonly<Record<string, string>> | ReturnType<typeof createIssueBody>,
): Effect.Effect<Response> => Effect.promise(() => call(instance, method, path, body));

const decodeFeed = (response: Response) =>
  Effect.promise(() => json(response, TransitionFeedResponse));

/** Append a canonical fact straight to the durable source, bypassing the command path. */
const appendFact = Effect.fn("TransitionFeedTest.appendFact")(function* (
  instance: Host,
  event: Readonly<Record<string, string | number>>,
) {
  const stream = instance.client.stream(streamNames.issueEvents("main"));
  yield* Effect.promise(() => stream.create({ contentType: "application/json" }));
  const appended = yield* Effect.promise(() =>
    stream.append(encodeFact(event), { contentType: "application/json" }),
  );
  expect(appended.status).toBe("appended");
});

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
  test("an assignment reaches the feed as a row change that moved no status", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(
          instance,
          "POST",
          "/api/workspaces/main/issues",
          createIssueBody("c-1", "issue-1", "First", "todo"),
        );
        yield* request(instance, "POST", "/api/workspaces/main/issues/issue-1/assignee", {
          commandId: "c-2",
          assigneeId: "ada",
        });

        const body = yield* decodeFeed(yield* feed(instance));
        const events = body.events.filter((event) => event.issueId === "issue-1");
        expect(events.map((event) => event.change)).toEqual(["enter", "update"]);

        const assignment = events[1];
        expect(assignment?.status).toBe("todo");
        expect(assignment?.previousStatus).toBe("todo");

        // The status-move projection a consumer builds from the same feed.
        expect(
          events.filter(
            (event) => event.change === "update" && event.previousStatus !== event.status,
          ),
        ).toEqual([]);
      }),
    ));

  test("publishes one transition per maintained change, in command order", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(
          instance,
          "POST",
          "/api/workspaces/main/issues",
          createIssueBody("cmd-1", "issue-1", "First", "backlog"),
        );
        yield* request(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
          commandId: "cmd-2",
          status: "todo",
        });
        yield* request(instance, "POST", "/api/workspaces/main/issues/issue-1/status", {
          commandId: "cmd-3",
          status: "done",
        });

        const body = yield* decodeFeed(yield* feed(instance));
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
      }),
    ));

  test("follows fact arrival order, not domain sequence order", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(instance, "GET", "/api/workspaces/main/issues");

        yield* appendFact(instance, created("issue-1", 0, "backlog", "2026-08-25T10:00:00.000Z"));
        yield* request(instance, "GET", "/api/workspaces/main/issues");
        // Sequence 2 arrives before sequence 1. The fold observes arrival order, so
        // the feed must show `done` and then `todo`, and the row must end at `todo`.
        yield* appendFact(instance, moved("issue-1", 2, "done", "2026-08-25T10:02:00.000Z"));
        yield* request(instance, "GET", "/api/workspaces/main/issues");
        yield* appendFact(instance, moved("issue-1", 1, "todo", "2026-08-25T10:01:00.000Z"));

        const body = yield* decodeFeed(yield* feed(instance));
        expect(body.events.map((event) => event.status)).toEqual(["backlog", "done", "todo"]);
        expect(body.events.map((event) => event.occurredAt)).toEqual([
          "2026-08-25T10:00:00.000Z",
          "2026-08-25T10:02:00.000Z",
          "2026-08-25T10:01:00.000Z",
        ]);

        const response = yield* request(instance, "GET", "/api/workspaces/main/issues");
        const rows = yield* Effect.promise(() => response.json());
        expect(rows).toMatchObject({ rows: [{ issueId: "issue-1", status: "todo" }] });
      }),
    ));

  test("a native offset replays exactly the suffix appended after it", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(
          instance,
          "POST",
          "/api/workspaces/main/issues",
          createIssueBody("cmd-1", "issue-1", "First"),
        );
        const first = yield* decodeFeed(yield* feed(instance));
        expect(first.upToDate).toBe(true);

        yield* request(
          instance,
          "POST",
          "/api/workspaces/main/issues",
          createIssueBody("cmd-2", "issue-2", "Second"),
        );
        const suffix = yield* decodeFeed(
          yield* feed(instance, `?offset=${encodeURIComponent(first.nextOffset)}`),
        );
        expect(suffix.events.map((event) => event.issueId)).toEqual(["issue-2"]);
        expect(suffix.nextOffset).not.toBe(first.nextOffset);

        const tail = yield* decodeFeed(
          yield* feed(instance, `?offset=${encodeURIComponent(suffix.nextOffset)}`),
        );
        expect(tail.events).toEqual([]);
      }),
    ));

  test("an unusable resume position declares replay-from-start", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(instance, "POST", "/api/workspaces/main/seed");
        const refused = yield* feed(instance, "?offset=not-an-offset");
        expect(refused.status).toBe(409);
        expect(yield* Effect.promise(() => refused.json())).toMatchObject({
          _tag: "ResumeRejected",
          sink: issueTransitions.name,
          reason: "invalid-offset",
          recovery: "replay-from-start",
        });

        const rebuilt = yield* decodeFeed(yield* feed(instance, "?offset=-1"));
        expect(rebuilt.events).toHaveLength(4);
      }),
    ));

  test("the feed rejects a retired contract and a write method", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        const retired = yield* Effect.promise(() =>
          instance.fetch(
            new Request(`http://localhost${FEED}`, {
              headers: { "x-streamsy-stream-sink-contract": "retired" },
            }),
          ),
        );
        expect(retired.status).toBe(409);
        expect(yield* Effect.promise(() => retired.json())).toMatchObject({
          reason: "contract-changed",
        });

        const written = yield* Effect.promise(() =>
          instance.fetch(new Request(`http://localhost${FEED}`, { method: "POST" })),
        );
        expect(written.status).toBe(405);
      }),
    ));

  test("the declared route and contract are what the host serves", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        expect(issueTransitions.compiledRoute.build({ workspaceId: "main" })).toBe(FEED);
        expect(issueTransitions.key).toBe("issueId");
        expect(issueTransitions.from.of.name).toBe("issue-tracker.issues");
        expect(issueTransitions.fingerprint).toMatch(/^[0-9a-f]{8}$/);
        const instance = fresh();
        const response = yield* feed(instance);
        expect(response.headers.get("x-streamsy-stream-sink-contract")).toBe(
          issueTransitions.fingerprint,
        );
      }),
    ));
});
