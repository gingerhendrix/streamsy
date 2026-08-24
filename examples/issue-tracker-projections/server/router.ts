/**
 * HTTP router.
 *
 * The router is the application's external trust boundary and nothing more. It
 * decodes the path and the body with Schema, calls application descriptions,
 * and translates the typed error channel into responses by `_tag`. Business
 * rules live in the application, not here.
 *
 * Everything below the router has an explicit error type, so the only thing
 * that can reach the defect handler is a genuine bug — not an expected
 * validation outcome.
 */
import type { JsonValue } from "@streamsy/core";
import { Cause, Effect, Schema } from "effect";
import type { ApiError } from "../shared/api.ts";
import {
  CreateIssueRequest,
  CreateProjectRequest,
  IssueCommandRequest,
} from "../shared/requests.ts";
import {
  createIssue,
  createProject,
  getBoard,
  health,
  issueCommand,
  listProjects,
  probeCoverage,
  projectsResponse,
  repairProject,
  requireDetail,
  syncIssue,
  type ApplicationServices,
  type CommandOptions,
} from "./application.ts";
import { InvalidRequest, MalformedBody } from "./errors.ts";
import { seedWorkspace } from "./seed.ts";

const json = (body: JsonValue, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });

const fail = (status: number, error: string, detail?: string): Response => {
  if (detail === undefined) return json({ error } satisfies ApiError, status);
  return json({ error, detail } satisfies ApiError, status);
};

/**
 * Read a JSON body and decode it into the declared request shape.
 *
 * The two failures are kept apart deliberately: a body that is not JSON is a
 * transport-level problem, while a body that is JSON but not the declared shape
 * is a request-validation problem — the same class of failure the application
 * reports as `InvalidRequest`.
 */
const body = <S extends Schema.Top>(schema: S, request: Request) =>
  Effect.tryPromise({
    try: () => request.json(),
    catch: () => new MalformedBody({ detail: "the request body is not valid JSON" }),
  }).pipe(
    Effect.flatMap((value) =>
      Schema.decodeUnknownEffect(schema)(value).pipe(
        Effect.mapError((issue) =>
          InvalidRequest.of("body", firstLine(String(issue)).slice(0, 200)),
        ),
      ),
    ),
  );

/**
 * `?projections=deferred` skips the immediate projection passes. Durability and
 * the acknowledgement are unchanged; only the latency optimisation is dropped,
 * so a caller can exercise queue or repair convergence deliberately.
 */
function commandOptions(url: URL): CommandOptions {
  return { deferProjections: url.searchParams.get("projections") === "deferred" };
}

function apiSegments(url: URL): readonly string[] | undefined {
  if (!url.pathname.startsWith("/api/")) return undefined;
  return url.pathname
    .slice("/api/".length)
    .split("/")
    .filter((segment) => segment.length > 0)
    .map(decodeURIComponent);
}

/** Route one API request. Static assets and stream routes remain host concerns. */
export const handleApi = (request: Request): Effect.Effect<Response, never, ApplicationServices> =>
  route(request).pipe(
    // Expected failures are typed, so each one has a named status here.
    Effect.catchTags({
      InvalidRequest: (error) => Effect.succeed(fail(400, "invalid-request", error.detail)),
      MalformedBody: (error) => Effect.succeed(fail(400, "invalid-json", error.detail)),
      UnknownProject: () => Effect.succeed(fail(404, "unknown-project")),
      UnknownIssue: () => Effect.succeed(fail(404, "unknown-issue")),
      AppendRejected: (error) => Effect.succeed(fail(409, "append-rejected", error.status)),
      StreamUnavailable: (error) =>
        Effect.succeed(fail(503, "stream-unavailable", `${error.streamId}: ${error.status}`)),
      StateRestorePoison: () => Effect.succeed(fail(500, "state-restore-poison")),
    }),
    // Anything still failing is an operational mesh error or a defect.
    Effect.catchCause((cause) => Effect.succeed(errorResponse(cause))),
  );

const route = (request: Request) =>
  Effect.gen(function* () {
    const url = new URL(request.url);
    if (url.pathname === "/health") return json(yield* health());

    const segments = apiSegments(url);
    if (segments === undefined || segments[0] !== "workspaces" || segments[1] === undefined) {
      return fail(404, "not-found");
    }
    const workspaceId = segments[1];
    const rest = segments.slice(2);

    if (rest[0] === "seed" && request.method === "POST") {
      return json(yield* seedWorkspace(workspaceId));
    }

    if (rest[0] === "projects" && rest.length === 1) {
      if (request.method === "GET") {
        return json(projectsResponse(workspaceId, yield* listProjects(workspaceId)));
      }
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const project = yield* createProject(workspaceId, yield* body(CreateProjectRequest, request));
      if (project.status === "created") return json(project.project, 201);
      if (project.status === "reconciled") return json(project.project, 200);
      if (project.status === "conflict") return fail(409, "project-conflict", project.detail);
      return fail(409, "append-rejected", project.detail);
    }

    if (rest[0] === "projects" && rest[1] !== undefined) {
      const projectId = rest[1];
      if (rest[2] === "board" && request.method === "GET") {
        const board = yield* getBoard(workspaceId, projectId);
        return board.status === "ok" ? json(board.response) : fail(404, board.status);
      }
      if (rest[2] === "repair" && request.method === "POST") {
        return json(yield* repairProject(workspaceId, projectId));
      }
      return fail(404, "not-found");
    }

    if (rest[0] === "issues" && rest.length === 1) {
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const created = yield* createIssue(
        workspaceId,
        yield* body(CreateIssueRequest, request),
        commandOptions(url),
      );
      return json(created, 201);
    }

    if (rest[0] === "issues" && rest[1] !== undefined) {
      const issueId = rest[1];
      if (rest.length === 2 && request.method === "GET") {
        return json(yield* requireDetail(workspaceId, issueId));
      }
      if (rest[2] === "commands" && request.method === "POST") {
        return json(
          yield* issueCommand(
            workspaceId,
            issueId,
            yield* body(IssueCommandRequest, request),
            commandOptions(url),
          ),
        );
      }
      if (rest[2] === "coverage" && request.method === "GET") {
        const position = url.searchParams.get("position") ?? "";
        return json(yield* probeCoverage(workspaceId, issueId, position));
      }
      if (rest[2] === "sync" && request.method === "POST") {
        return json(yield* syncIssue(workspaceId, issueId));
      }
      return fail(404, "not-found");
    }

    return fail(404, "not-found");
  });

/**
 * What is left after typed recovery: an operational mesh failure, or a defect.
 * A defect is a bug in this application and is reported as an opaque 500 —
 * validation never arrives here any more, because validation is typed.
 */
function errorResponse(cause: Cause.Cause<unknown>): Response {
  if (Cause.hasInterrupts(cause)) return fail(499, "interrupted");
  const pretty = Cause.pretty(cause);
  return fail(500, "internal-error", pretty.slice(0, 2_000));
}

function firstLine(value: string): string {
  return value.split("\n", 1)[0] ?? value;
}
