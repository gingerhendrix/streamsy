/**
 * HTTP router. It translates typed application results into responses and
 * keeps every unexpected failure a structured 500 rather than a silent success.
 */
import { Cause, Effect } from "effect";
import type { ApiError } from "../shared/api.ts";
import {
  createIssue,
  createProject,
  getBoard,
  health,
  issueCommand,
  listProjects,
  loadDetail,
  projectsResponse,
  repairProject,
  type ApplicationOptions,
  type MeshServices,
} from "./application.ts";
import { LaneRegistry } from "./bindings.ts";
import { projectionContext, runIssueDetail, runProjectBoard } from "./projections.ts";
import { seedWorkspace } from "./seed.ts";

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });

const fail = (status: number, error: string, detail?: string): Response =>
  json({ error, ...(detail === undefined ? {} : { detail }) } satisfies ApiError, status);

const routerLanes = new LaneRegistry();

const BAD_REQUEST = Symbol.for("issue-tracker-projections/bad-request");

const readJson = (request: Request): Effect.Effect<unknown> =>
  Effect.tryPromise({
    try: () => request.json() as Promise<unknown>,
    catch: () => BAD_REQUEST,
  }).pipe(Effect.catchCause(() => Effect.succeed(BAD_REQUEST as unknown)));

function apiSegments(url: URL): readonly string[] | undefined {
  if (!url.pathname.startsWith("/api/")) return undefined;
  return url.pathname
    .slice("/api/".length)
    .split("/")
    .filter((segment) => segment.length > 0)
    .map(decodeURIComponent);
}

/** Route one API request. Static assets and stream routes remain host concerns. */
export const handleApi = (
  options: ApplicationOptions,
  request: Request,
): Effect.Effect<Response, never, MeshServices> =>
  Effect.gen(function* () {
    const url = new URL(request.url);
    if (url.pathname === "/health") return json(health(options));

    const segments = apiSegments(url);
    if (segments === undefined || segments[0] !== "workspaces" || segments[1] === undefined) {
      return fail(404, "not-found");
    }
    const workspaceId = segments[1];
    const rest = segments.slice(2);

    if (rest[0] === "seed" && request.method === "POST") {
      return json(yield* seedWorkspace(options, workspaceId));
    }

    if (rest[0] === "projects" && rest.length === 1) {
      if (request.method === "GET") {
        return json(projectsResponse(workspaceId, yield* listProjects(options, workspaceId)));
      }
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const body = yield* readJson(request);
      if (body === BAD_REQUEST) return fail(400, "invalid-json");
      return json(yield* createProject(options, workspaceId, body as never), 201);
    }

    if (rest[0] === "projects" && rest[1] !== undefined) {
      const projectId = rest[1];
      if (rest[2] === "board" && request.method === "GET") {
        const board = yield* getBoard(options, workspaceId, projectId);
        return board.status === "ok" ? json(board.response) : fail(404, board.status);
      }
      if (rest[2] === "repair" && request.method === "POST") {
        return json(yield* repairProject(options, workspaceId, projectId));
      }
      return fail(404, "not-found");
    }

    if (rest[0] === "issues" && rest.length === 1) {
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const body = yield* readJson(request);
      if (body === BAD_REQUEST) return fail(400, "invalid-json");
      const created = yield* createIssue(options, workspaceId, body as never);
      if (created.status === "unknown-project") return fail(404, "unknown-project");
      if (created.status === "rejected") {
        return fail(409, "append-rejected", created.appended.outcome.status);
      }
      return json(created.response, 201);
    }

    if (rest[0] === "issues" && rest[1] !== undefined) {
      const issueId = rest[1];
      const context = projectionContext(options.client, workspaceId, routerLanes);
      if (rest.length === 2 && request.method === "GET") {
        const detail = yield* loadDetail(context, issueId);
        return detail === undefined ? fail(404, "unknown-issue") : json(detail);
      }
      if (rest[2] === "commands" && request.method === "POST") {
        const body = yield* readJson(request);
        if (body === BAD_REQUEST) return fail(400, "invalid-json");
        const result = yield* issueCommand(options, workspaceId, issueId, body as never);
        if (result.status === "unknown-issue") return fail(404, "unknown-issue");
        if (result.status === "rejected") {
          return fail(409, "append-rejected", result.appended.outcome.status);
        }
        return json(result.response);
      }
      if (rest[2] === "sync" && request.method === "POST") {
        const detail = yield* loadDetail(context, issueId);
        if (detail === undefined) return fail(404, "unknown-issue");
        yield* runIssueDetail(context, issueId);
        const board = yield* runProjectBoard(context, detail.projectId);
        return json({ issueId, projectId: detail.projectId, board: board.status });
      }
      return fail(404, "not-found");
    }

    return fail(404, "not-found");
  }).pipe(Effect.catchCause((cause) => Effect.succeed(errorResponse(cause))));

/** Domain validation throws TypeError; everything else stays an opaque 500. */
function errorResponse(cause: Cause.Cause<unknown>): Response {
  if (Cause.hasInterrupts(cause)) return fail(499, "interrupted");
  const pretty = Cause.pretty(cause);
  const invalid = cause.reasons.some(
    (reason) => Cause.isDieReason(reason) && reason.defect instanceof TypeError,
  );
  return invalid
    ? fail(400, "invalid-request", firstLine(pretty))
    : fail(500, "internal-error", pretty.slice(0, 2_000));
}

function firstLine(value: string): string {
  return value.split("\n", 1)[0] ?? value;
}
