import { Http } from "@streamsy/core";
import { Checkpoints, Projection } from "@streamsy/projection";
import { Serve } from "@streamsy/serve";
import { Effect, Layer, Option, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { SqlClient } from "effect/unstable/sql";
import { CommandRequest } from "../shared/api.ts";
import { issueRows } from "./projection.ts";
import { tracker } from "./outputs.ts";
import { transact } from "./state.ts";

export const outputRoutes = Layer.mergeAll(
  Serve.state(tracker.outputs.board, "/state/workspaces/:workspaceId/issues"),
  Serve.state(tracker.outputs.labelCounts, "/state/workspaces/:workspaceId/label-counts"),
  Serve.stream(tracker.outputs.transitions, "/feed/workspaces/:workspaceId/issue-transitions"),
  Serve.document(tracker.outputs.summary, "/document/workspaces/:workspaceId/summary"),
);

export const app = (workspaces: ReadonlyArray<string>) => {
  const api = Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const params = yield* HttpRouter.params;
    const workspaceId = params.workspaceId ?? "";
    const resource = params.resource;
    const json = HttpServerResponse.jsonUnsafe;
    if (!workspaces.includes(workspaceId))
      return json({ error: "Unknown workspace" }, { status: 404 });
    if (request.method === "POST" && resource === "commands") {
      const decoded = yield* request.json.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(CommandRequest)),
        Effect.result,
      );
      if (decoded._tag === "Failure") return json({ error: "Invalid command" }, { status: 400 });
      const command = decoded.success;
      const result = yield* transact(workspaceId, command).pipe(Effect.result);
      if (result._tag === "Failure") {
        if (result.failure._tag === "UnknownIssue")
          return json({ error: "Unknown issue" }, { status: 400 });
        if (result.failure._tag === "OffsetMismatch")
          return json({ error: "Concurrent command conflict" }, { status: 409 });
        return json({ error: "Internal server error" }, { status: 500 });
      }
      yield* Projection.serialized(issueRows.member({ workspaceId }));
      yield* Projection.serialized(tracker.member({ workspaceId }));
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql.unsafe<{
        readonly issueId: string;
        readonly projectId: string;
        readonly title: string;
        readonly status: string;
        readonly sequence: number;
        readonly updatedAt: string;
        readonly assigneeId: string | null;
        readonly labelIds: string;
      }>(
        `SELECT i.issue_id AS issueId,i.project_id AS projectId,i.title,i.status,i.sequence,i.updated_at AS updatedAt,i.assignee_id AS assigneeId,COALESCE(GROUP_CONCAT(CASE WHEN il.attached=1 THEN il.label_id END), '') AS labelIds FROM issues i LEFT JOIN issue_labels il ON il.workspace_id=i.workspace_id AND il.issue_id=i.issue_id WHERE i.workspace_id=? AND i.issue_id=? GROUP BY i.issue_id`,
        [workspaceId, command.issueId],
      );
      const row = rows[0];
      return json({
        commandId: command.commandId,
        event: result.success.event,
        row:
          row === undefined
            ? undefined
            : {
                ...row,
                assigneeId: row.assigneeId ?? undefined,
                labelIds: row.labelIds.split(",").filter(Boolean).sort(),
              },
      });
    }
    if (request.method !== "GET") return json({ error: "Method not allowed" }, { status: 405 });
    if (resource === "drafts") {
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql.unsafe(
        `SELECT * FROM notification_drafts WHERE workspace_id=? ORDER BY event_id`,
        [workspaceId],
      );
      return json({ workspaceId, rows });
    }
    if (resource !== "status") return json({ error: "Not found" }, { status: 404 });
    const checkpoints = yield* Checkpoints;
    const loaded = yield* checkpoints.load(Projection.key(issueRows.member({ workspaceId })));
    return json({
      workspaceId,
      offsets: Option.match(loaded.record, {
        onNone: () => ({}),
        onSome: (record) => record.inputs,
      }),
    });
  }).pipe(
    Effect.tapError((error) => Effect.logError(error)),
    Effect.catch(() =>
      Effect.succeed(
        HttpServerResponse.jsonUnsafe({ error: "Internal server error" }, { status: 500 }),
      ),
    ),
  );
  return Layer.mergeAll(
    Http.routes({ prefix: "/streams" }),
    outputRoutes,
    HttpRouter.add("*", "/api/workspaces/:workspaceId/:resource", api),
    HttpRouter.add(
      "GET",
      "/",
      HttpServerResponse.jsonUnsafe({ service: "issue-tracker", workspaces }),
    ),
  );
};
