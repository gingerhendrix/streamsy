/* oxlint-disable effecttsgo/node-builtin-import -- The executable host owns its SQLite path. */
import { Checkpoints, Projection } from "@streamsy/projection";
import { Effect, ManagedRuntime, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { Schema } from "effect";
import { CommandRequest } from "../shared/api.ts";
import { applicationLayer, StreamHttp } from "./host.ts";
import { issueRows } from "./projection.ts";
import { transact } from "./state.ts";

const port = Number(process.env.PORT ?? 1340);
const databasePath =
  process.env.ISSUE_TRACKER_DB ?? `/tmp/streamsy-issue-tracker-${process.pid}.sqlite`;
const workspaces = (process.env.ISSUE_TRACKER_WORKSPACES ?? "acme,live").split(",").filter(Boolean);
if (databasePath !== ":memory:") await mkdir(dirname(databasePath), { recursive: true });
const runtime = ManagedRuntime.make(applicationLayer(databasePath, workspaces));
const streamHttp = await runtime.runPromise(StreamHttp);

// oxlint-disable-next-line anti-slop/no-object-parameters -- Every value is assembled from decoded commands or owner-typed SQL rows immediately before this response boundary.
const json = (value: object, init: ResponseInit = {}) => Response.json(value, init);
const pathMatch = (pathname: string) =>
  pathname.match(/^\/api\/workspaces\/([^/]+)\/(commands|issues|changes|drafts|status)$/);

const api = async (request: Request): Promise<Response> => {
  const match = pathMatch(new URL(request.url).pathname);
  if (match === null) return json({ error: "Not found" }, { status: 404 });
  const workspaceId = match[1] ?? "";
  const resource = match[2] ?? "";
  if (!workspaces.includes(workspaceId))
    return json({ error: "Unknown workspace" }, { status: 404 });
  try {
    if (request.method === "POST" && resource === "commands") {
      const command = Schema.decodeUnknownSync(CommandRequest)(await request.json());
      const accepted = await runtime.runPromise(transact(workspaceId, command));
      await runtime.runPromise(
        Projection.serialized(issueRows.member({ workspaceId }), { limit: 5 }),
      );
      const rows = await readIssues(workspaceId);
      return json({
        commandId: command.commandId,
        event: accepted.event,
        row: rows.find((row) => row.issueId === command.issueId),
      });
    }
    if (request.method !== "GET") return json({ error: "Method not allowed" }, { status: 405 });
    if (resource === "issues") return json({ workspaceId, rows: await readIssues(workspaceId) });
    if (resource === "changes")
      return json({
        workspaceId,
        rows: await query(`SELECT * FROM issue_changes WHERE workspace_id=? ORDER BY id`, [
          workspaceId,
        ]),
      });
    if (resource === "drafts")
      return json({
        workspaceId,
        rows: await query(
          `SELECT * FROM notification_drafts WHERE workspace_id=? ORDER BY event_id`,
          [workspaceId],
        ),
      });
    const loaded = await runtime.runPromise(
      Effect.gen(function* () {
        const checkpoints = yield* Checkpoints;
        return yield* checkpoints.load(Projection.key(issueRows.member({ workspaceId })));
      }),
    );
    return json({
      workspaceId,
      offsets: Option.match(loaded.record, {
        onNone: () => ({}),
        onSome: (record) => record.inputs,
      }),
    });
  } catch (error) {
    return json({ error: error instanceof Error ? error.message : String(error) }, { status: 400 });
  }
};

const query = <A extends object>(statement: string, params: ReadonlyArray<unknown>) =>
  runtime.runPromise(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return yield* sql.unsafe<A>(statement, params);
    }),
  );
const readIssues = async (workspaceId: string) => {
  interface IssueReadRow {
    readonly issueId: string;
    readonly projectId: string;
    readonly title: string;
    readonly status: string;
    readonly sequence: number;
    readonly updatedAt: string;
    readonly assigneeId: string | null;
    readonly labelIds: string;
  }
  const rows = await query<IssueReadRow>(
    `SELECT i.issue_id AS issueId,i.project_id AS projectId,i.title,i.status,i.sequence,i.updated_at AS updatedAt,i.assignee_id AS assigneeId,COALESCE(GROUP_CONCAT(CASE WHEN il.attached=1 THEN il.label_id END), '') AS labelIds FROM issues i LEFT JOIN issue_labels il ON il.workspace_id=i.workspace_id AND il.issue_id=i.issue_id WHERE i.workspace_id=? GROUP BY i.issue_id ORDER BY i.issue_id`,
    [workspaceId],
  );
  return rows.map((row) => ({
    ...row,
    assigneeId: row.assigneeId ?? undefined,
    labelIds: String(row.labelIds).split(",").filter(Boolean),
  }));
};

const server = Bun.serve({
  port,
  fetch: (request) => {
    const pathname = new URL(request.url).pathname;
    if (pathname.startsWith("/streams/")) return streamHttp.fetch(request);
    if (pathname.startsWith("/api/")) return api(request);
    return json({ service: "issue-tracker", workspaces });
  },
});
console.log(`Issue tracker listening on http://127.0.0.1:${server.port}`);

let closing: Promise<void> | undefined;
export const shutdown = () =>
  (closing ??= (async () => {
    await server.stop(true);
    await runtime.dispose();
  })());
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => void shutdown().finally(() => process.exit(0)));
export { server };
