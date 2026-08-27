/**
 * The `documentSink`'s public contract.
 *
 * The summary is a cache-shaped product, so the properties under test are the
 * ones a cache rests on: the document decodes through its declared schema, its
 * entity tag is a deterministic function of its content, a conditional request
 * on the current tag is answered without a body, and the cache policy the
 * declaration wrote is the header the host sends.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { workspaceSummary } from "../domain/declaration.ts";
import { WorkspaceSummary } from "../domain/issue.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const SUMMARY = "/document/workspaces/main/summary";
const open: Host[] = [];
afterEach(() =>
  Promise.all(open.splice(0).map((instance) => instance.close())).then(() => undefined),
);

function fresh(): Host {
  const created = host();
  open.push(created);
  return created;
}

function summary(
  instance: Host,
  headers: HeadersInit = {},
  method = "GET",
): Effect.Effect<Response> {
  return Effect.promise(() =>
    instance.fetch(new Request(`http://localhost${SUMMARY}`, { method, headers })),
  );
}

const request = (
  instance: Host,
  method: string,
  path: string,
  body?: ReturnType<typeof createIssueBody>,
): Effect.Effect<Response> => Effect.promise(() => call(instance, method, path, body));

const decodeSummary = (response: Response) =>
  Effect.promise(() => json(response, WorkspaceSummary));

describe("the workspace-summary document sink", () => {
  test("serves counts derived from the maintained rows and the catalog", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(instance, "POST", "/api/workspaces/main/seed");
        const document = yield* decodeSummary(yield* summary(instance));

        expect(document.workspaceId).toBe("main");
        expect(document.planHash).toMatch(/^[0-9a-f]{8}$/);
        expect(document.issues.total).toBe(4);
        expect(document.issues.byStatus).toEqual({
          backlog: 1,
          todo: 1,
          in_progress: 1,
          done: 1,
        });
        expect(document.catalog.projects).toBe(1);
        expect(document.latestActivityAt).toBeString();
      }),
    ));

  test("an empty workspace still carries every declared column", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        const document = yield* decodeSummary(yield* summary(instance));
        expect(document.issues).toEqual({
          total: 0,
          byStatus: { backlog: 0, todo: 0, in_progress: 0, done: 0 },
        });
        expect(document.latestActivityAt).toBeNull();
      }),
    ));

  test("the entity tag is stable while nothing changes and moves when something does", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(instance, "POST", "/api/workspaces/main/seed");
        const first = yield* summary(instance);
        const again = yield* summary(instance);
        const etag = first.headers.get("etag");
        expect(etag).toMatch(/^"[0-9a-f]{16}"$/);
        expect(again.headers.get("etag")).toBe(etag);

        yield* request(
          instance,
          "POST",
          "/api/workspaces/main/issues",
          createIssueBody("cmd-1", "issue-1", "Changes the summary", "todo"),
        );
        const changed = yield* summary(instance);
        expect(changed.headers.get("etag")).not.toBe(etag);
        expect((yield* decodeSummary(changed.clone())).issues.byStatus.todo).toBe(2);
      }),
    ));

  test("a conditional request on the current tag is a bodiless 304", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        yield* request(instance, "POST", "/api/workspaces/main/seed");
        const first = yield* summary(instance);
        const etag = first.headers.get("etag") ?? "";

        const conditional = yield* summary(instance, { "if-none-match": etag });
        expect(conditional.status).toBe(304);
        expect(yield* Effect.promise(() => conditional.text())).toBe("");
        expect(conditional.headers.get("etag")).toBe(etag);

        yield* request(
          instance,
          "POST",
          "/api/workspaces/main/issues",
          createIssueBody("cmd-1", "issue-1", "Invalidates the cache"),
        );
        const revalidated = yield* summary(instance, { "if-none-match": etag });
        expect(revalidated.status).toBe(200);
        expect(revalidated.headers.get("etag")).not.toBe(etag);
      }),
    ));

  test("the declared cache policy is the header the host sends", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        const response = yield* summary(instance);
        expect(workspaceSummary.cacheControl).toBe("private, max-age=0, must-revalidate");
        expect(response.headers.get("cache-control")).toBe(workspaceSummary.cacheControl);
        expect(response.headers.get("x-streamsy-document-sink-contract")).toBe(
          workspaceSummary.fingerprint,
        );
        expect(response.headers.get("content-type")).toBe("application/json");
      }),
    ));

  test("HEAD reports the validator without the body, and a write method is refused", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const instance = fresh();
        const head = yield* summary(instance, {}, "HEAD");
        expect(head.status).toBe(200);
        expect(yield* Effect.promise(() => head.text())).toBe("");
        expect(head.headers.get("etag")).toMatch(/^"[0-9a-f]{16}"$/);

        const written = yield* summary(instance, {}, "POST");
        expect(written.status).toBe(405);
      }),
    ));

  test("the document names the relations it is derived from", () => {
    expect(workspaceSummary.from.map((source) => source.name)).toEqual([
      "issue-tracker.issues",
      "issue-tracker.projects",
      "issue-tracker.users",
      "issue-tracker.labels",
    ]);
    expect(workspaceSummary.compiledRoute.build({ workspaceId: "main" })).toBe(SUMMARY);
  });
});
