/**
 * Durable target State is decoded, not trusted.
 *
 * A row that carries the right collection tag but a malformed application value
 * must never be restored into typed state. Both kernels turn the restore throw
 * into a typed `StateRestorePoison`, which is what these tests assert — first
 * directly on the restore functions, then through the real projection passes
 * against a fabricated but lineage-valid durable history.
 */
import type { JsonValue } from "@streamsy/core";
import { AppendStreams } from "@streamsy/experimental/effect";
import {
  createFanInCheckpoint,
  createLineageEvent,
  FanInRecovery,
  recoverDerivedStateHistory,
} from "@streamsy/experimental/ivm-mesh";
import { Effect } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { LaneRegistry } from "../server/bindings.ts";
import { createLocalHost } from "../server/local.ts";
import {
  projectionContext,
  restoreBoard,
  restoreDetail,
  runIssueDetail,
  runProjectBoard,
} from "../server/projections.ts";

type Host = ReturnType<typeof createLocalHost>;

const hosts: Host[] = [];

afterEach(async () => {
  await Promise.all(hosts.splice(0).map((host) => host.close()));
});

function newHost(): Host {
  const host = createLocalHost();
  hosts.push(host);
  return host;
}

const WORKSPACE = "poison";
const PROJECT = "launch";
const ISSUE = "issue-1";

async function call(host: Host, method: string, path: string, body?: unknown): Promise<Response> {
  return host.fetch(
    new Request(`http://localhost${path}`, {
      method,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), headers: { "content-type": "application/json" } }),
    }),
  );
}

/** A seeded workspace with one issue, so both targets carry valid lineage. */
async function seeded(host: Host): Promise<void> {
  await call(host, "POST", `/api/workspaces/${WORKSPACE}/projects`, {
    projectId: PROJECT,
    projectKey: "SHIP",
    name: "Launch",
  });
  const created = await call(host, "POST", `/api/workspaces/${WORKSPACE}/issues`, {
    commandId: "cmd-1",
    issueId: ISSUE,
    projectId: PROJECT,
    title: "Restore me",
  });
  expect(created.status).toBe(201);
}

const validDetail = {
  issueId: ISSUE,
  issueKey: "SHIP-100",
  projectId: PROJECT,
  title: "Restore me",
  status: "backlog",
  priority: "medium",
  assigneeId: null,
  comments: [],
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
};

const validRow = {
  issueId: ISSUE,
  issueKey: "SHIP-100",
  title: "Restore me",
  status: "backlog",
  priority: "medium",
  assigneeId: null,
  commentCount: 0,
  updatedAt: "2026-01-01T00:00:00.000Z",
};

const detailFact = (value: unknown): JsonValue =>
  ({
    type: "issue-detail",
    key: ISSUE,
    value,
    headers: { operation: "upsert" },
  }) as JsonValue;

const boardFact = (value: unknown): JsonValue =>
  ({
    type: "board-issue",
    key: ISSUE,
    value,
    headers: { operation: "upsert" },
  }) as JsonValue;

describe("schema-backed restore", () => {
  test("a correctly tagged issue-detail row with a malformed value is rejected", () => {
    expect(restoreDetail(undefined, [detailFact(validDetail)])).toMatchObject({
      issueKey: "SHIP-100",
    });
    // Right collection, wrong value: an unknown status, a missing field, and a
    // wrong-typed field must each fail rather than pass through.
    expect(() =>
      restoreDetail(undefined, [detailFact({ ...validDetail, status: "shipped" })]),
    ).toThrow();
    expect(() =>
      restoreDetail(undefined, [detailFact({ ...validDetail, comments: undefined })]),
    ).toThrow();
    expect(() => restoreDetail(undefined, [detailFact({ ...validDetail, title: 42 })])).toThrow();
  });

  test("a correctly tagged board row with a malformed value is rejected", () => {
    expect(restoreBoard({}, [boardFact(validRow)])[ISSUE]).toMatchObject({ issueKey: "SHIP-100" });
    expect(() => restoreBoard({}, [boardFact({ ...validRow, priority: "blocker" })])).toThrow();
    expect(() => restoreBoard({}, [boardFact({ ...validRow, commentCount: "two" })])).toThrow();
    expect(() => restoreBoard({}, [boardFact({ ...validRow, issueKey: "" })])).toThrow();
  });
});

describe("typed restore poison from durable history", () => {
  test("a malformed issue-detail row poisons the next detail pass", async () => {
    const host = newHost();
    await seeded(host);
    const lanes = new LaneRegistry();
    const ctx = projectionContext(host.client, WORKSPACE, lanes);
    const target = ctx.bindings.issueDetail(WORKSPACE, ISSUE);

    // Commit a malformed but correctly tagged row at a real lineage boundary,
    // so recovery succeeds and only the restore can reject it.
    await host.runtime.runPromise(
      Effect.gen(function* () {
        const lane = yield* Effect.promise(() => lanes.issueDetail(WORKSPACE, ISSUE));
        const recovered = yield* recoverDerivedStateHistory(target, lane);
        if (recovered.status !== "ready") throw new Error("target history must recover");
        const lineage = createLineageEvent(lane, {
          sourceThrough: recovered.checkpoint.sourceThrough ?? "0_0",
          nextProducerSeq: recovered.checkpoint.nextProducerSeq + 1,
        }) as unknown as JsonValue;
        const appends = yield* AppendStreams;
        const appended = yield* appends.appendJsonBatch(
          target,
          [detailFact({ ...validDetail, status: "shipped" }), lineage],
          {
            expectedOffset: recovered.checkpoint.targetOffset,
            producer: {
              producerId: lane.producerId,
              producerEpoch: lane.producerEpoch,
              producerSeq: recovered.checkpoint.nextProducerSeq,
            },
          },
        );
        if (appended.status !== "appended") {
          throw new Error(`malformed detail row must commit, got ${appended.status}`);
        }
      }),
    );

    const failure = await host.runtime.runPromise(Effect.flip(runIssueDetail(ctx, ISSUE)));
    expect(failure._tag).toBe("StateRestorePoison");
  });

  test("a malformed board row poisons the next fan-in pass", async () => {
    const host = newHost();
    await seeded(host);
    const lanes = new LaneRegistry();
    const ctx = projectionContext(host.client, WORKSPACE, lanes);
    const target = ctx.bindings.board(WORKSPACE, PROJECT);

    await host.runtime.runPromise(
      Effect.gen(function* () {
        const lane = yield* Effect.promise(() => lanes.projectBoard(WORKSPACE, PROJECT));
        const recovery = yield* FanInRecovery;
        const recovered = yield* recovery.recoverFanIn(target, lane);
        if (recovered.status !== "ready") throw new Error("board must recover");
        const checkpoint = createFanInCheckpoint(lane, {
          membershipThrough: recovered.checkpoint.membershipThrough,
          nextProducerSeq: recovered.checkpoint.nextProducerSeq + 1,
        }) as unknown as JsonValue;
        const appends = yield* AppendStreams;
        const appended = yield* appends.appendJsonBatch(
          target,
          [boardFact({ ...validRow, priority: "blocker" }), checkpoint],
          {
            expectedOffset: recovered.checkpoint.targetOffset,
            producer: {
              producerId: lane.producerId,
              producerEpoch: lane.producerEpoch,
              producerSeq: recovered.checkpoint.nextProducerSeq,
            },
          },
        );
        if (appended.status !== "appended") {
          throw new Error(`malformed board row must commit, got ${appended.status}`);
        }
      }),
    );

    const failure = await host.runtime.runPromise(Effect.flip(runProjectBoard(ctx, PROJECT)));
    expect(failure._tag).toBe("StateRestorePoison");
  });

  test("the board endpoint names a malformed durable row instead of serving it", async () => {
    const host = newHost();
    await seeded(host);
    const lanes = new LaneRegistry();
    const ctx = projectionContext(host.client, WORKSPACE, lanes);
    const target = ctx.bindings.board(WORKSPACE, PROJECT);

    await host.runtime.runPromise(
      Effect.gen(function* () {
        const lane = yield* Effect.promise(() => lanes.projectBoard(WORKSPACE, PROJECT));
        const recovery = yield* FanInRecovery;
        const recovered = yield* recovery.recoverFanIn(target, lane);
        if (recovered.status !== "ready") throw new Error("board must recover");
        const checkpoint = createFanInCheckpoint(lane, {
          membershipThrough: recovered.checkpoint.membershipThrough,
          nextProducerSeq: recovered.checkpoint.nextProducerSeq + 1,
        }) as unknown as JsonValue;
        const appends = yield* AppendStreams;
        yield* appends.appendJsonBatch(
          target,
          [boardFact({ ...validRow, commentCount: "two" }), checkpoint],
          {
            expectedOffset: recovered.checkpoint.targetOffset,
            producer: {
              producerId: lane.producerId,
              producerEpoch: lane.producerEpoch,
              producerSeq: recovered.checkpoint.nextProducerSeq,
            },
          },
        );
      }),
    );

    const response = await call(
      host,
      "GET",
      `/api/workspaces/${WORKSPACE}/projects/${PROJECT}/board`,
    );
    expect(response.status).toBe(500);
    expect(((await response.json()) as { error: string }).error).toBe("state-restore-poison");
  });
});
