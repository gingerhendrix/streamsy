/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- `bun:test` owns this file's control flow; what is under test is the membership fact family across real HTTP requests. */
/**
 * Issue-label membership, as a canonical fact family.
 *
 * Two claims are under test and they are separate. The first is that membership
 * is *durable behaviour*: attaching and detaching are commands, they append
 * facts, they fold into a maintained relation, and they survive a restart with
 * the same rows. The second is that removal needs no State delete — a detached
 * membership stays in the relation as `attached: false`, and the label-count
 * plan is what excludes it.
 *
 * Nothing here reads a count out of a command response. Counts come from the
 * maintained product, because that is what the browser binds to.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { Schema } from "effect";
import { existsSync } from "node:fs";
import { join } from "node:path";
import {
  IssueLabelsResponse,
  LabelCommandResponse,
  LabelCountsResponse,
  SeedResponse,
} from "../shared/api.ts";
import { membershipIdOf } from "../domain/issue.ts";
import { workspaceKey } from "../domain/domains.ts";
import { partitionPath } from "../server/host.ts";
import { call, host, json, temporaryDirectory, type Host } from "./support.ts";

const open: Host[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((instance) => instance.close()));
});
const track = (instance: Host): Host => {
  open.push(instance);
  return instance;
};

const counts = async (instance: Host, workspaceId: string) =>
  json(
    await call(instance, "GET", `/api/workspaces/${workspaceId}/label-counts`),
    LabelCountsResponse,
  );
const memberships = async (instance: Host, workspaceId: string) =>
  json(
    await call(instance, "GET", `/api/workspaces/${workspaceId}/issue-labels`),
    IssueLabelsResponse,
  );
const countOf = (body: Schema.Schema.Type<typeof LabelCountsResponse>, labelId: string): number =>
  body.rows.find((row) => row.labelId === labelId)?.issueCount ?? 0;

const attach = (instance: Host, issueId: string, labelId: string, commandId: string) =>
  call(instance, "POST", `/api/workspaces/main/issues/${issueId}/labels`, { commandId, labelId });
const detach = (instance: Host, issueId: string, labelId: string, commandId: string) =>
  call(instance, "POST", `/api/workspaces/main/issues/${issueId}/labels/detach`, {
    commandId,
    labelId,
  });

describe("issue-label membership", () => {
  test("a seeded workspace opens onto catalog labels and live counts", async () => {
    const instance = track(host());
    const seeded = await json(
      await call(instance, "POST", "/api/workspaces/main/seed"),
      SeedResponse,
    );
    expect([...seeded.labels].toSorted()).toEqual(["bug", "docs", "infra"]);

    const body = await counts(instance, "main");
    expect(body.view).toBe("issue-tracker.label-counts");
    expect(countOf(body, "infra")).toBe(2);
    expect(countOf(body, "docs")).toBe(1);
    expect(countOf(body, "bug")).toBe(1);
  });

  test("attaching moves the count and names the membership it created", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    const attached = await json(
      await attach(instance, "seed-plan", "bug", "c1"),
      LabelCommandResponse,
    );

    expect(attached.membershipId).toBe(membershipIdOf("seed-plan", "bug"));
    expect(attached.attached).toBe(true);
    expect(attached.reconciled).toBe(false);
    expect(attached.ack.stream).toBe("workspaces/main/issue-label-events");
    expect(attached.row?.attached).toBe(true);
    expect(countOf(await counts(instance, "main"), "bug")).toBe(2);
  });

  test("detaching removes the count without removing the row", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    await attach(instance, "seed-plan", "bug", "c1");
    const detached = await json(
      await detach(instance, "seed-plan", "bug", "c2"),
      LabelCommandResponse,
    );

    expect(detached.attached).toBe(false);
    expect(countOf(await counts(instance, "main"), "bug")).toBe(1);

    // The membership is still in the relation. That is what makes removal
    // expressible without a Durable State delete.
    const rows = await memberships(instance, "main");
    const row = rows.rows.find((candidate) => candidate.membershipId === "seed-plan.bug");
    expect(row).toBeDefined();
    expect(row?.attached).toBe(false);
  });

  test("re-attaching a detached membership brings the count back", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    await attach(instance, "seed-plan", "bug", "c1");
    await detach(instance, "seed-plan", "bug", "c2");
    await attach(instance, "seed-plan", "bug", "c3");

    expect(countOf(await counts(instance, "main"), "bug")).toBe(2);
    const rows = await memberships(instance, "main");
    expect(rows.rows.filter((row) => row.membershipId === "seed-plan.bug")).toHaveLength(1);
  });

  test("a retried membership command appends nothing and returns the original offset", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    const first = await json(
      await attach(instance, "seed-plan", "bug", "same"),
      LabelCommandResponse,
    );
    const retry = await json(
      await attach(instance, "seed-plan", "bug", "same"),
      LabelCommandResponse,
    );

    expect(retry.reconciled).toBe(true);
    expect(retry.ack.offset).toBe(first.ack.offset);
    expect(retry.sequence).toBe(first.sequence);
    expect(countOf(await counts(instance, "main"), "bug")).toBe(2);
  });

  test("one command id cannot mean two different memberships", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    await attach(instance, "seed-plan", "bug", "shared");
    const conflicting = await attach(instance, "seed-maintain", "bug", "shared");
    expect(conflicting.status).toBe(409);
  });

  test("two labels on one issue are two memberships, not one overwritten", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    await attach(instance, "seed-plan", "bug", "c1");
    await attach(instance, "seed-plan", "infra", "c2");

    const rows = await memberships(instance, "main");
    const forIssue = rows.rows.filter((row) => row.issueId === "seed-plan" && row.attached);
    expect(forIssue.map((row) => row.labelId).toSorted()).toEqual(["bug", "docs", "infra"]);
    expect(countOf(await counts(instance, "main"), "infra")).toBe(3);
  });

  test("a membership on an unknown label is refused, and a membership on an unknown issue too", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");

    const badLabel = await attach(instance, "seed-plan", "nonexistent", "c1");
    expect(badLabel.status).toBe(404);
    expect(await badLabel.json()).toMatchObject({ error: "unknown-label" });

    const badIssue = await attach(instance, "nonexistent", "bug", "c2");
    expect(badIssue.status).toBe(404);
    expect(await badIssue.json()).toMatchObject({ error: "unknown-issue" });

    // Neither refusal left a fact behind.
    expect(countOf(await counts(instance, "main"), "bug")).toBe(1);
  });

  test("memberships and counts survive a partition restart", async () => {
    const directory = temporaryDirectory("label-membership");
    const instance = track(host({ databaseDirectory: directory }));
    await call(instance, "POST", "/api/workspaces/main/seed");
    await attach(instance, "seed-plan", "bug", "c1");
    await detach(instance, "seed-maintain", "infra", "c2");
    const before = await counts(instance, "main");
    const beforeRows = await memberships(instance, "main");

    expect(await instance.host.restart(workspaceKey("main"))).toBe(true);

    expect(await counts(instance, "main")).toEqual(before);
    expect(await memberships(instance, "main")).toEqual(beforeRows);
    expect(existsSync(join(partitionPath(directory, workspaceKey("main")), "view.sqlite"))).toBe(
      true,
    );
  });

  test("a whole-host restart rebuilds membership from durable facts alone", async () => {
    const directory = temporaryDirectory("label-membership-host");
    const first = track(host({ databaseDirectory: directory }));
    await call(first, "POST", "/api/workspaces/main/seed");
    await attach(first, "seed-plan", "bug", "c1");
    const before = await counts(first, "main");
    await first.close();
    open.length = 0;

    const second = track(host({ databaseDirectory: directory }));
    expect(await counts(second, "main")).toEqual(before);

    // A retried command against the restarted host still reconciles.
    const retry = await json(await attach(second, "seed-plan", "bug", "c1"), LabelCommandResponse);
    expect(retry.reconciled).toBe(true);
    expect(countOf(await counts(second, "main"), "bug")).toBe(2);
  });

  test("membership is workspace-scoped, like every other relation", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    await call(instance, "POST", "/api/workspaces/other/seed");
    await attach(instance, "seed-plan", "bug", "c1");

    expect(countOf(await counts(instance, "main"), "bug")).toBe(2);
    expect(countOf(await counts(instance, "other"), "bug")).toBe(1);
    const otherRows = await memberships(instance, "other");
    expect(otherRows.rows.every((row) => row.workspaceId === "other")).toBe(true);
  });

  test("the membership routes refuse the methods they do not serve", async () => {
    const instance = track(host());
    await call(instance, "POST", "/api/workspaces/main/seed");
    expect(
      (await call(instance, "GET", "/api/workspaces/main/issues/seed-plan/labels")).status,
    ).toBe(405);
    expect((await call(instance, "POST", "/api/workspaces/main/label-counts")).status).toBe(405);
    expect(
      (await call(instance, "POST", "/api/workspaces/main/issues/seed-plan/labels/nope")).status,
    ).toBe(404);
  });
});
