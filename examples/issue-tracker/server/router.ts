/**
 * HTTP router — the application's external trust boundary and nothing more.
 *
 * It decodes the path and body with Schema, calls application descriptions, and
 * translates the typed error channel into responses by `_tag`. Everything below
 * has an explicit error type, so the only thing that reaches the defect handler
 * is a genuine bug.
 */
import type { JsonValue } from "@streamsy/core";
import { Cause, Effect, Schema } from "effect";
import { issues } from "../domain/declaration.ts";
import type { ApiError } from "../shared/api.ts";
import { ChangeStatusRequest, CreateIssueRequest } from "../shared/api.ts";
import {
  changeStatus,
  createIssue,
  health,
  listIssues,
  seedWorkspace,
  sinkSession,
  type ApplicationServices,
  type CommandResult,
} from "./application.ts";
import { AppConfig } from "./config.ts";
import { InvalidRequest, MalformedBody } from "./errors.ts";
import type { StreamGateway } from "./gateway.ts";
import { handleSinkRequest, sinkWorkspaceId } from "./sink-http.ts";

const json = (body: JsonValue, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });

/**
 * A typed failure body.
 *
 * The three literals are written out rather than assembled, so each one is
 * checked against the `ApiError` wire contract as it is built.
 */
const fail = (status: number, error: string, detail?: string, fallback?: string): Response => {
  if (detail === undefined) return json({ error } satisfies ApiError, status);
  if (fallback === undefined) return json({ error, detail } satisfies ApiError, status);
  return json({ error, detail, fallback } satisfies ApiError, status);
};

/**
 * Two failures are kept apart deliberately: a body that is not JSON is a
 * transport problem, while a body that is JSON but not the declared shape is a
 * request-validation problem.
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

export type RouterServices = ApplicationServices | StreamGateway;

/** Route one request. Static assets remain a host concern. */
export const handle = (request: Request): Effect.Effect<Response, never, RouterServices> =>
  route(request).pipe(
    Effect.catchTags({
      InvalidRequest: (error) => Effect.succeed(fail(400, "invalid-request", error.detail)),
      MalformedBody: (error) => Effect.succeed(fail(400, "invalid-json", error.detail)),
      UnknownIssue: (error) => Effect.succeed(fail(404, "unknown-issue", error.issueId)),
      AppendRejected: (error) => Effect.succeed(fail(409, "append-rejected", error.status)),
      StreamUnavailable: (error) =>
        Effect.succeed(fail(503, "stream-unavailable", `${error.streamId}: ${error.status}`)),
      SourcePoison: (error) =>
        Effect.succeed(fail(500, "source-poison", `${error.position}: ${error.detail}`)),
      MaintenanceFault: (error) =>
        Effect.succeed(fail(500, "maintenance-fault", `${error.phase}: ${error.detail}`)),
      StoreRestorePoison: (error) =>
        Effect.succeed(fail(500, "state-restore-poison", `${error.table}/${error.key}`)),
      StoreUnavailable: (error) => Effect.succeed(fail(503, "store-unavailable", error.operation)),
      Unauthorized: (error) => Effect.succeed(fail(403, "unauthorized", error.required)),
      SessionResumeUnavailable: (error) =>
        Effect.succeed(fail(409, "resume-unavailable", error.reason, error.fallback)),
    }),
    Effect.catchCause((cause) => Effect.succeed(errorResponse(cause))),
  );

const route = (request: Request) =>
  Effect.gen(function* () {
    const url = new URL(request.url);

    if (url.pathname === "/health") return json(yield* health());

    const sinkWorkspace = sinkWorkspaceId(url.pathname);
    if (sinkWorkspace !== undefined) {
      if (request.method !== "GET" && request.method !== "HEAD") {
        return fail(405, "method-not-allowed");
      }
      return yield* handleSinkRequest(request, sinkWorkspace);
    }

    const segments = apiSegments(url);
    if (segments === undefined || segments[0] !== "workspaces" || segments[1] === undefined) {
      return fail(404, "not-found");
    }
    const workspaceId = segments[1];
    const rest = segments.slice(2);

    if (rest[0] === "seed" && rest.length === 1) {
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      return json(yield* seedWorkspace(workspaceId));
    }

    if (rest[0] === "sink-session" && rest.length === 1) {
      if (request.method !== "GET") return fail(405, "method-not-allowed");
      return json(yield* sinkSession(workspaceId));
    }

    if (rest[0] === "issues" && rest.length === 1) {
      if (request.method === "GET") {
        const config = yield* AppConfig;
        return json({
          workspaceId,
          view: issues.name,
          planHash: config.planHash,
          rows: yield* listIssues(workspaceId),
        });
      }
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const created = yield* createIssue(workspaceId, yield* body(CreateIssueRequest, request));
      return json(commandBody(created), created.reconciled ? 200 : 201);
    }

    if (
      rest[0] === "issues" &&
      rest[1] !== undefined &&
      rest[2] === "status" &&
      rest.length === 3
    ) {
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const moved = yield* changeStatus(
        workspaceId,
        rest[1],
        yield* body(ChangeStatusRequest, request),
      );
      return json(commandBody(moved));
    }

    return fail(404, "not-found");
  });

/** Project the workflow result onto the declared wire contract. */
function commandBody(result: CommandResult): JsonValue {
  return {
    commandId: result.commandId,
    workspaceId: result.workspaceId,
    issueId: result.issueId,
    eventId: result.eventId,
    sequence: result.sequence,
    ack: result.ack,
    reconciled: result.reconciled,
    maintenance: {
      checkpoint: result.maintenance.checkpoint ?? null,
      folded: result.maintenance.folded,
      changed: result.maintenance.changes.length,
      publication: result.maintenance.publication,
    },
    row: result.row ?? null,
  };
}

function apiSegments(url: URL): readonly string[] | undefined {
  if (!url.pathname.startsWith("/api/")) return undefined;
  return url.pathname
    .slice("/api/".length)
    .split("/")
    .filter((segment) => segment.length > 0)
    .map(decodeURIComponent);
}

/** What is left after typed recovery is an operational mesh failure, or a defect. */
function errorResponse(cause: Cause.Cause<unknown>): Response {
  if (Cause.hasInterrupts(cause)) return fail(499, "interrupted");
  return fail(500, "internal-error", Cause.pretty(cause).slice(0, 2_000));
}

function firstLine(value: string): string {
  return value.split("\n", 1)[0] ?? value;
}
