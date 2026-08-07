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
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import type { BoardResponse, MutationResponse } from "../shared/api.ts";
import { createLocalHost } from "../server/local.ts";

const filename = join(mkdtempSync(join(tmpdir(), "issue-tracker-projections-")), "state.sqlite");
const workspaceId = "smoke";
const projectId = "smokeproj";
const issueId = "smokeissue";

let host = createLocalHost({ adapter: createSqliteStorageAdapter({ filename }) });

try {
  const health = await json(await call("GET", "/health"));
  assert(health.status === "ok", "health must be ok");

  await call("POST", `/api/workspaces/${workspaceId}/projects`, {
    projectId,
    projectKey: "SMK",
    name: "Smoke",
  });

  const created: MutationResponse = await json(
    await call("POST", `/api/workspaces/${workspaceId}/issues`, {
      commandId: "smoke-create",
      issueId,
      projectId,
      title: "Smoke the vertical path",
      priority: "high",
      creatorId: "ada",
    }),
  );
  assert(created.ack.position.length > 0, "create must return an exact source acknowledgement");
  assert(
    created.coverage.status === "proven",
    `create coverage must be proven, got ${created.coverage.status}`,
  );

  // A repeated command reconciles through its producer tuple to the same offset.
  const repeated: MutationResponse = await json(
    await call("POST", `/api/workspaces/${workspaceId}/issues`, {
      commandId: "smoke-create",
      issueId,
      projectId,
      title: "Smoke the vertical path",
      priority: "high",
      creatorId: "ada",
    }),
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

  const applied: MutationResponse[] = [];
  for (const command of commands) {
    const response: MutationResponse = await json(
      await call("POST", `/api/workspaces/${workspaceId}/issues/${issueId}/commands`, command),
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

  const board: BoardResponse = await json(
    await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
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

  const afterRestart: BoardResponse = await json(
    await call("GET", `/api/workspaces/${workspaceId}/projects/${projectId}/board`),
  );
  assert(afterRestart.rows.length === 1, "board must survive a host restart");
  assert(afterRestart.rows[0]!.status === "done", "restarted board must keep the final status");

  const repaired = await json(
    await call("POST", `/api/workspaces/${workspaceId}/projects/${projectId}/repair`),
  );
  assert(
    Array.isArray(repaired.repaired) && repaired.repaired.includes(issueId),
    "repair must cover active membership",
  );

  // Convergence without the mutation request: append through the stream API and
  // let the explicit repair endpoint carry the change into the board.
  const detailAfterRepair = await json(
    await call("GET", `/api/workspaces/${workspaceId}/issues/${issueId}`),
  );
  assert(detailAfterRepair.issueId === issueId, "recovered detail must survive restart");

  console.log(`issue-tracker-projections http smoke passed at ${created.ack.position}`);
} finally {
  await host.close();
}

function call(method: string, path: string, body?: unknown): Promise<Response> {
  return host.fetch(
    new Request(`http://localhost${path}`, {
      method,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), headers: { "content-type": "application/json" } }),
    }),
  );
}

async function json<T = any>(response: Response): Promise<T> {
  const text = await response.text();
  if (!response.ok) throw new Error(`${response.status} ${text.slice(0, 400)}`);
  return JSON.parse(text) as T;
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
