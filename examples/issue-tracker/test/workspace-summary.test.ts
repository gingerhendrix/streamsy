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
import { workspaceSummary } from "../domain/declaration.ts";
import { WorkspaceSummary } from "../domain/issue.ts";
import { call, createIssueBody, host, json, type Host } from "./support.ts";

const SUMMARY = "/document/workspaces/main/summary";
const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});

function fresh(): Host {
  const created = host();
  open.push(created);
  return created;
}

async function summary(
  instance: Host,
  headers: HeadersInit = {},
  method = "GET",
): Promise<Response> {
  return instance.fetch(new Request(`http://localhost${SUMMARY}`, { method, headers }));
}

describe("the workspace-summary document sink", () => {
  test("serves counts derived from the maintained rows and the catalog", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const document = await json(await summary(instance), WorkspaceSummary);

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
  });

  test("an empty workspace still carries every declared column", async () => {
    const instance = fresh();
    const document = await json(await summary(instance), WorkspaceSummary);
    expect(document.issues).toEqual({
      total: 0,
      byStatus: { backlog: 0, todo: 0, in_progress: 0, done: 0 },
    });
    expect(document.latestActivityAt).toBeNull();
  });

  test("the entity tag is stable while nothing changes and moves when something does", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const first = await summary(instance);
    const again = await summary(instance);
    const etag = first.headers.get("etag");
    expect(etag).toMatch(/^"[0-9a-f]{16}"$/);
    expect(again.headers.get("etag")).toBe(etag);

    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Changes the summary", "todo"),
    );
    const changed = await summary(instance);
    expect(changed.headers.get("etag")).not.toBe(etag);
    expect((await json(changed.clone(), WorkspaceSummary)).issues.byStatus.todo).toBe(2);
  });

  test("a conditional request on the current tag is a bodiless 304", async () => {
    const instance = fresh();
    await call(instance, "POST", "/api/workspaces/main/seed");
    const first = await summary(instance);
    const etag = first.headers.get("etag") ?? "";

    const conditional = await summary(instance, { "if-none-match": etag });
    expect(conditional.status).toBe(304);
    expect(await conditional.text()).toBe("");
    expect(conditional.headers.get("etag")).toBe(etag);

    await call(
      instance,
      "POST",
      "/api/workspaces/main/issues",
      createIssueBody("cmd-1", "issue-1", "Invalidates the cache"),
    );
    const revalidated = await summary(instance, { "if-none-match": etag });
    expect(revalidated.status).toBe(200);
    expect(revalidated.headers.get("etag")).not.toBe(etag);
  });

  test("the declared cache policy is the header the host sends", async () => {
    const instance = fresh();
    const response = await summary(instance);
    expect(workspaceSummary.cacheControl).toBe("private, max-age=0, must-revalidate");
    expect(response.headers.get("cache-control")).toBe(workspaceSummary.cacheControl);
    expect(response.headers.get("x-streamsy-document-sink-contract")).toBe(
      workspaceSummary.fingerprint,
    );
    expect(response.headers.get("content-type")).toBe("application/json");
  });

  test("HEAD reports the validator without the body, and a write method is refused", async () => {
    const instance = fresh();
    const head = await summary(instance, {}, "HEAD");
    expect(head.status).toBe(200);
    expect(await head.text()).toBe("");
    expect(head.headers.get("etag")).toMatch(/^"[0-9a-f]{16}"$/);

    const written = await summary(instance, {}, "POST");
    expect(written.status).toBe(405);
  });

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
