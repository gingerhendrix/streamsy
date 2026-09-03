/* oxlint-disable effecttsgo/async-function -- Vitest owns this file's control flow: every `test` and `afterEach` callback is a Promise the runner awaits, and the seeding helpers are Promise-native drivers over the host's Web `fetch` handler. The restore and projection passes under test stay Effect values, run on the host's runtime and read through `Effect.flip`. */
/**
 * Durable target State is decoded, not trusted.
 *
 * A row that carries the right collection tag but a malformed application value
 * must never be restored into typed state. Both kernels turn the restore throw
 * into a typed `StateRestorePoison`, which is what these tests assert — first
 * directly on the restore functions, then through the real projection passes
 * against a fabricated but lineage-valid durable history.
 *
 * The projection passes are plain Effect descriptions, so the tests run them on
 * the host's runtime and read their typed error channel with `Effect.flip`.
 */
import type { JsonValue } from "@streamsy/core";
import { AppendStreams } from "@streamsy/streams";
import {
  createFanInCheckpoint,
  createLineageEvent,
  FanInRecovery,
  recoverDerivedStateHistory,
} from "@streamsy/projection/mesh";
import { Effect, Schema } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import { ProjectionLanes } from "../server/lanes.ts";
import { createLocalHost } from "../server/local.ts";
import {
  restoreBoard,
  restoreDetail,
  runIssueDetail,
  runProjectBoard,
} from "../server/projections.ts";
import { Streams } from "../server/streams.ts";
import { ApiError } from "../shared/api.ts";
import type { BoardRow, IssueDetail } from "../shared/model.ts";

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

async function call(host: Host, method: string, path: string, body?: JsonValue): Promise<Response> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "content-type": "application/json" };
  }
  return host.fetch(new Request(`http://localhost${path}`, init));
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

/** A JSON record as it appears in a durable State stream. */
const JsonRecord = Schema.Record(Schema.String, Schema.Json);
const decodeJsonRecord = Schema.decodeUnknownSync(JsonRecord);

/**
 * Restate a typed mesh event as the durable JSON record the append API takes.
 *
 * The round trip is the encoding the transport performs anyway, so the row
 * these tests commit is the row the kernel itself would have written.
 */
function meshFact(event: JsonValue): JsonValue {
  return decodeJsonRecord(JSON.parse(JSON.stringify(event)));
}

const validDetail: IssueDetail = {
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

const validRow: BoardRow = {
  issueId: ISSUE,
  issueKey: "SHIP-100",
  title: "Restore me",
  status: "backlog",
  priority: "medium",
  assigneeId: null,
  commentCount: 0,
  updatedAt: "2026-01-01T00:00:00.000Z",
};

const detailFact = (value: JsonValue): JsonValue => ({
  type: "issue-detail",
  key: ISSUE,
  value,
  headers: { operation: "upsert" },
});

const boardFact = (value: JsonValue): JsonValue => ({
  type: "board-issue",
  key: ISSUE,
  value,
  headers: { operation: "upsert" },
});

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
    const { comments: _comments, ...withoutComments } = validDetail;
    expect(() => restoreDetail(undefined, [detailFact(withoutComments)])).toThrow();
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

    // Commit a malformed but correctly tagged row at a real lineage boundary,
    // so recovery succeeds and only the restore can reject it.
    await host.runtime.runPromise(
      Effect.gen(function* () {
        const streams = yield* Streams;
        const lanes = yield* ProjectionLanes;
        const target = streams.bindings.issueDetail(WORKSPACE, ISSUE);
        const lane = yield* lanes.issueDetail(WORKSPACE, ISSUE);
        const recovered = yield* recoverDerivedStateHistory(target, lane);
        if (recovered.status !== "ready") throw new Error("target history must recover");
        const lineage = meshFact(
          createLineageEvent(lane, {
            sourceThrough: recovered.checkpoint.sourceThrough ?? "0_0",
            nextProducerSeq: recovered.checkpoint.nextProducerSeq + 1,
          }),
        );
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

    const failure = await host.runtime.runPromise(Effect.flip(runIssueDetail(WORKSPACE, ISSUE)));
    const { _tag: tag } = failure;
    expect(tag).toBe("StateRestorePoison");
  });

  test("a malformed board row poisons the next fan-in pass", async () => {
    const host = newHost();
    await seeded(host);

    await host.runtime.runPromise(
      Effect.gen(function* () {
        const streams = yield* Streams;
        const lanes = yield* ProjectionLanes;
        const target = streams.bindings.board(WORKSPACE, PROJECT);
        const lane = yield* lanes.projectBoard(WORKSPACE, PROJECT);
        const recovery = yield* FanInRecovery;
        const recovered = yield* recovery.recoverFanIn(target, lane);
        if (recovered.status !== "ready") throw new Error("board must recover");
        const checkpoint = meshFact(
          createFanInCheckpoint(lane, {
            membershipThrough: recovered.checkpoint.membershipThrough,
            nextProducerSeq: recovered.checkpoint.nextProducerSeq + 1,
          }),
        );
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

    const failure = await host.runtime.runPromise(Effect.flip(runProjectBoard(WORKSPACE, PROJECT)));
    const { _tag: tag } = failure;
    expect(tag).toBe("StateRestorePoison");
  });

  test("the board endpoint names a malformed durable row instead of serving it", async () => {
    const host = newHost();
    await seeded(host);

    await host.runtime.runPromise(
      Effect.gen(function* () {
        const streams = yield* Streams;
        const lanes = yield* ProjectionLanes;
        const target = streams.bindings.board(WORKSPACE, PROJECT);
        const lane = yield* lanes.projectBoard(WORKSPACE, PROJECT);
        const recovery = yield* FanInRecovery;
        const recovered = yield* recovery.recoverFanIn(target, lane);
        if (recovered.status !== "ready") throw new Error("board must recover");
        const checkpoint = meshFact(
          createFanInCheckpoint(lane, {
            membershipThrough: recovered.checkpoint.membershipThrough,
            nextProducerSeq: recovered.checkpoint.nextProducerSeq + 1,
          }),
        );
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
    const body = Schema.decodeUnknownSync(ApiError)(await response.json());
    expect(body.error).toBe("state-restore-poison");
  });
});
