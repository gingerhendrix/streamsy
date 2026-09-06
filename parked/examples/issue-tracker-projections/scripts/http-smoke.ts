/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/node-builtin-import -- This Bun smoke executable is a Promise-native driver over the local host's public HTTP surface; it creates its temporary SQLite database with Node-compatible filesystem and path APIs and reports its single result line to the invoking terminal. */
/**
 * HTTP smoke over the local host.
 *
 * It creates a project and issue, changes every editable field, adds a comment,
 * moves the card across statuses, reads the board through the public stream
 * endpoint, and proves the original issue acknowledgement through both
 * projection hops. It then restarts the host over the same SQLite database and
 * verifies recovered detail, membership, board state, and coverage.
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { JsonValue } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage/sqlite";
import { Schema } from "effect";
import {
  BoardResponse,
  CoverageResponse,
  HealthResponse,
  MutationResponse,
  RepairResponse,
} from "../shared/api.ts";
import {
  type BoardResponse as BoardResponseValue,
  type CoverageResponse as CoverageResponseValue,
  type MutationResponse as MutationResponseValue,
  type RepairResponse as RepairResponseValue,
} from "../shared/api.ts";
import { IssueDetailSchema } from "../shared/model.ts";
import { createLocalHost } from "../server/local.ts";

const filename = join(mkdtempSync(join(tmpdir(), "issue-tracker-projections-")), "state.sqlite");
const workspaceId = "smoke";
const projectId = "smokeproj";
const issueId = "smokeissue";

let host = createLocalHost({ adapter: createSqliteStorageAdapter({ filename }) });

try {
  const health = await json(await call("GET", "/health"), HealthResponse);
  assert(health.status === "ok", "health must be ok");

  await call("POST", `/api/workspaces/${workspaceId}/projects`, {
    projectId,
    projectKey: "SMK",
    name: "Smoke",
  });

  const created: MutationResponseValue = await json(
    await call("POST", `/api/workspaces/${workspaceId}/issues`, {
      commandId: "smoke-create",
      issueId,
      projectId,
      title: "Smoke the vertical path",
      priority: "high",
      creatorId: "ada",
    }),
    MutationResponse,
  );
  assert(created.ack.position.length > 0, "create must return an exact source acknowledgement");
  assert(
    created.coverage.status === "proven",
    `create coverage must be proven, got ${created.coverage.status}`,
  );

  // A repeated command reconciles through its producer tuple to the same offset.
  const repeated: MutationResponseValue = await json(
    await call("POST", `/api/workspaces/${workspaceId}/issues`, {
      commandId: "smoke-create",
      issueId,
      projectId,
      title: "Smoke the vertical path",
      priority: "high",
      creatorId: "ada",
    }),
    MutationResponse,
  );
  assert(repeated.reconciled, "a repeated create must be reconciled, not appended again");
  assert(
    repeated.ack.position === created.ack.position,
    "reconciliation must return the original exact offset",
  );

  const commands = [
    { commandId: "smoke-rename", type: "rename", title: "Smoke the projection path" },
    { commandId: "smoke-priority", type: "priority", priority: "urgent" },
    { commandId: "smoke-assign", type: "assign", assigneeId: "grace" },
    {
      commandId: "smoke-comment",
      type: "comment",
      commentId: "smokecomment",
      authorId: "lin",
      body: "Coverage proven through both hops.",
    },
    { commandId: "smoke-progress", type: "status", status: "in-progress" },
    { commandId: "smoke-done", type: "status", status: "done" },
  ] as const;

  const applied: MutationResponseValue[] = [];
  for (const command of commands) {
    const response: MutationResponseValue = await json(
      await call("POST", `/api/workspaces/${workspaceId}/issues/${issueId}/commands`, command),
      MutationResponse,
    );
    assert(
      response.coverage.status === "proven",
      `${command.type} coverage must be proven, got ${response.coverage.status}`,
    );
    applied.push(response);
  }
  const settled = applied.at(-1);
  assert(settled !== undefined, "at least one command must run");
  assert(settled.detail?.title === "Smoke the projection path", "rename must be durable");
  assert(settled.detail?.priority === "urgent", "priority must be durable");
  assert(settled.detail?.assigneeId === "grace", "assignment must be durable");
  assert(settled.detail?.comments.length === 1, "comment must be durable");
  assert(settled.detail?.status === "done", "status movement must be durable");

  const board: BoardResponseValue = await json(
    await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
    BoardResponse,
  );
  assert(board.rows.length === 1, "board must contain the created issue");
  assert(board.rows[0]!.status === "done", "board must reflect the final status");
  assert(board.rows[0]!.commentCount === 1, "board must reflect the comment count");

  // The browser path: board State read directly through the stream endpoint.
  const streamed = await call("GET", `/streams/${board.boardStream}`);
  assert(streamed.status === 200, `board stream read must succeed, got ${streamed.status}`);
  const streamedBody = await streamed.text();
  assert(streamedBody.includes(issueId), "board stream must carry the issue row");

  await host.close();
  host = createLocalHost({ adapter: createSqliteStorageAdapter({ filename }) });

  const afterRestart: BoardResponseValue = await json(
    await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
    BoardResponse,
  );
  assert(afterRestart.rows.length === 1, "board must survive a host restart");
  assert(afterRestart.rows[0]!.status === "done", "restarted board must keep the final status");

  const repaired = await json(
    await call("POST", `/api/workspaces/${workspaceId}/projects/${projectId}/repair`),
    RepairResponse,
  );
  assert(
    Array.isArray(repaired.repaired) && repaired.repaired.includes(issueId),
    "repair must cover active membership",
  );

  const detailAfterRepair = await json(
    await call("GET", `/api/workspaces/${workspaceId}/issues/${issueId}`),
    IssueDetailSchema,
  );
  assert(detailAfterRepair.issueId === issueId, "recovered detail must survive restart");

  // Convergence without the mutation request. `?projections=deferred` skips the
  // immediate passes, so this is exactly the shape of a lost immediate pass:
  // the command is durable and unproven, and only repair can prove it.
  const deferred: MutationResponseValue = await json(
    await call(
      "POST",
      `/api/workspaces/${workspaceId}/issues/${issueId}/commands?projections=deferred`,
      { commandId: "smoke-deferred", type: "priority", priority: "low" },
    ),
    MutationResponse,
  );
  assert(
    deferred.coverage.status !== "proven",
    "a deferred command must not report proven coverage",
  );
  assert(
    deferred.projections.every((pass) => pass.outcome === "deferred"),
    "a deferred command must classify both passes as deferred",
  );

  const coveragePath = `/api/workspaces/${workspaceId}/issues/${issueId}/coverage?position=${encodeURIComponent(
    deferred.ack.position,
  )}`;
  const beforeRepair: CoverageResponseValue = await json(
    await call("GET", coveragePath),
    CoverageResponse,
  );
  assert(
    beforeRepair.coverage.status !== "proven",
    "a read-only probe must not prove an uncaught-up chain",
  );

  const converged: RepairResponseValue = await json(
    await call("POST", `/api/workspaces/${workspaceId}/projects/${projectId}/repair`),
    RepairResponse,
  );
  assert(
    converged.projections.every((pass) => pass.outcome === "caught-up"),
    `repair must catch every pass up, got ${JSON.stringify(converged.projections)}`,
  );
  const afterRepair: CoverageResponseValue = await json(
    await call("GET", coveragePath),
    CoverageResponse,
  );
  assert(
    afterRepair.coverage.status === "proven",
    `coverage.status must be proven after repair, got ${afterRepair.coverage.status}`,
  );

  console.log(`issue-tracker-projections http smoke passed at ${created.ack.position}`);
} finally {
  await host.close();
}

function call(method: string, path: string, body?: JsonValue): Promise<Response> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "content-type": "application/json" };
  }
  return host.fetch(new Request(`http://localhost${path}`, init));
}

/**
 * Read a JSON body.
 *
 * The type parameter names the response contract the caller expects; the
 * server builds that contract from the shared Schemas, and the assertions in
 * this smoke are what check it.
 */
async function json<S extends Schema.ConstraintDecoder<unknown>>(
  response: Response,
  schema: S,
): Promise<S["Type"]> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 400)}`);
  return Schema.decodeUnknownSync(schema)(JSON.parse(text));
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
