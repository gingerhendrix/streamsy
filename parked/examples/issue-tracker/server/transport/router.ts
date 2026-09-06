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
import { boardLabelCounts, issueLabelMemberships, issues } from "../../domain/declaration.ts";
import { labelCounts as labelCountsView } from "../../domain/views.ts";
import { CatalogCollection } from "../../domain/catalog.ts";
import type { ApiError } from "../../shared/api.ts";
import {
  AssignIssueRequest,
  CatalogUpsertRequest,
  ChangeStatusRequest,
  CreateIssueRequest,
  LabelMembershipRequest,
} from "../../shared/api.ts";
import { decodeAssignmentNotification } from "../../domain/notifications.ts";
import {
  assignIssue,
  attachLabel,
  changeStatus,
  createIssue,
  detachLabel,
  drainNotifications,
  listIssueLabels,
  listIssues,
  listCatalog,
  listLabelCounts,
  listNotifications,
  seedWorkspace,
  sinkSession,
  upsertCatalog,
  type ApplicationServices,
  type CommandResult,
  type LabelCommandResult,
} from "../application/application.ts";
import { AppConfig } from "../config.ts";
import { InvalidRequest, MalformedBody } from "../errors.ts";
import type { StreamGateway } from "./gateway.ts";
import {
  handleLabelCountSinkRequest,
  handleSinkRequest,
  handleTransitionFeedRequest,
  handleWorkspaceSummaryRequest,
  matchBoardSink,
  matchLabelCountSink,
  matchSummarySink,
  matchTransitionSink,
} from "../publication/sink-http.ts";

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
      UnknownLabel: (error) => Effect.succeed(fail(404, "unknown-label", error.labelId)),
      AppendRejected: (error) => Effect.succeed(fail(409, "append-rejected", error.status)),
      CommandIdConflict: (error) =>
        Effect.succeed(fail(409, "command-id-conflict", error.commandId)),
      CommandContention: (error) =>
        Effect.succeed(fail(409, "command-contention", `${error.attempts} attempts`)),
      CommandRecoveryExhausted: (error) =>
        Effect.succeed(
          fail(503, "command-recovery-exhausted", `${error.maxBatches}/${error.maxItems}`),
        ),
      StreamUnavailable: (error) =>
        Effect.succeed(fail(503, "stream-unavailable", `${error.streamId}: ${error.status}`)),
      SourcePoison: (error) =>
        Effect.succeed(fail(500, "source-poison", `${error.position}: ${error.detail}`)),
      UnsupportedStateOperation: (error) =>
        Effect.succeed(
          fail(500, "unsupported-state-operation", `${error.collection}/${error.key}: delete`),
        ),
      TransitionHistoryExpired: (error) =>
        Effect.succeed(fail(500, "transition-history-expired", error.detail)),
      GraphHistoryExpired: (error) =>
        Effect.succeed(fail(500, "graph-history-expired", `${error.product}: ${error.detail}`)),
      MaintenanceFault: (error) =>
        Effect.succeed(fail(500, "maintenance-fault", `${error.phase}: ${error.detail}`)),
      StoreRestorePoison: (error) =>
        Effect.succeed(fail(500, "state-restore-poison", `${error.table}/${error.key}`)),
      StoreUnavailable: (error) => Effect.succeed(fail(503, "store-unavailable", error.operation)),
      OutboxUnavailable: (error) =>
        Effect.succeed(fail(503, "outbox-unavailable", error.operation)),
    }),
    Effect.catchCause((cause) => Effect.succeed(errorResponse(cause))),
  );

const route = (request: Request) =>
  Effect.gen(function* () {
    const url = new URL(request.url);

    const sinkMatch = matchBoardSink(url.pathname);
    if (sinkMatch.kind !== "mismatch") {
      if (!readMethod(request)) return fail(405, "method-not-allowed");
      return yield* handleSinkRequest(request);
    }

    if (matchLabelCountSink(url.pathname).kind !== "mismatch") {
      if (!readMethod(request)) return fail(405, "method-not-allowed");
      return yield* handleLabelCountSinkRequest(request);
    }

    if (matchTransitionSink(url.pathname).kind !== "mismatch") {
      if (!readMethod(request)) return fail(405, "method-not-allowed");
      return yield* handleTransitionFeedRequest(request);
    }

    if (matchSummarySink(url.pathname).kind !== "mismatch") {
      if (!readMethod(request)) return fail(405, "method-not-allowed");
      return yield* handleWorkspaceSummaryRequest(request);
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

    if (rest[0] === "catalog" && rest[1] !== undefined && rest.length === 2) {
      const collection = yield* Schema.decodeUnknownEffect(CatalogCollection)(rest[1]).pipe(
        Effect.mapError(() => InvalidRequest.of("collection", rest[1] ?? "missing")),
      );
      if (request.method === "GET") {
        const listed = yield* listCatalog(workspaceId, collection);
        return json({
          workspaceId,
          collection,
          checkpoint: listed.report.checkpoint ?? null,
          folded: listed.report.folded,
          changed: listed.report.changes.length,
          rows: listed.rows,
        });
      }
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const listed = yield* upsertCatalog(
        workspaceId,
        collection,
        yield* body(CatalogUpsertRequest, request),
      );
      return json({
        workspaceId,
        collection,
        checkpoint: listed.report.checkpoint ?? null,
        folded: listed.report.folded,
        changed: listed.report.changes.length,
        rows: listed.rows,
      });
    }

    if (rest[0] === "issue-labels" && rest.length === 1) {
      if (request.method !== "GET") return fail(405, "method-not-allowed");
      return json({
        workspaceId,
        relation: issueLabelMemberships.name,
        rows: yield* listIssueLabels(workspaceId),
      });
    }

    if (rest[0] === "label-counts" && rest.length === 1) {
      if (request.method !== "GET") return fail(405, "method-not-allowed");
      return json({
        workspaceId,
        view: labelCountsView.name,
        sink: boardLabelCounts.name,
        contractFingerprint: boardLabelCounts.fingerprint,
        rows: yield* listLabelCounts(workspaceId),
      });
    }

    if (rest[0] === "notifications" && rest.length === 1) {
      if (request.method !== "GET") return fail(405, "method-not-allowed");
      const listed = yield* listNotifications(workspaceId);
      return json({
        workspaceId,
        sink: listed.sink,
        contractFingerprint: listed.contractFingerprint,
        pending: listed.entries.filter((entry) => entry.state === "pending").length,
        delivered: listed.entries.filter((entry) => entry.state === "delivered").length,
        dead: listed.entries.filter((entry) => entry.state === "dead").length,
        outbox: listed.entries.map((entry) => ({
          id: entry.id,
          idempotencyKey: entry.idempotencyKey,
          state: entry.state,
          attempts: entry.attempts,
          nextAttemptAtMs: entry.nextAttemptAtMs,
          lastError: entry.lastError ?? null,
          deadLetterReason: entry.deadLetterReason ?? null,
          payload: decodeAssignmentNotification(JSON.parse(entry.payload)),
        })),
        notified: [...listed.notified],
      });
    }

    if (rest[0] === "notifications" && rest[1] === "drain" && rest.length === 2) {
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const report = yield* drainNotifications(workspaceId);
      return json({
        workspaceId,
        sink: report.sink,
        claimed: report.claimed,
        delivered: report.delivered,
        retried: report.retried,
        deadLettered: report.deadLettered,
      });
    }

    if (
      rest[0] === "issues" &&
      rest[1] !== undefined &&
      rest[2] === "assignee" &&
      rest.length === 3
    ) {
      if (request.method !== "POST") return fail(405, "method-not-allowed");
      const assigned = yield* assignIssue(
        workspaceId,
        rest[1],
        yield* body(AssignIssueRequest, request),
      );
      return json(commandBody(assigned));
    }

    if (rest[0] === "issues" && rest[1] !== undefined && rest[2] === "labels") {
      if (rest.length === 3) {
        if (request.method !== "POST") return fail(405, "method-not-allowed");
        const attached = yield* attachLabel(
          workspaceId,
          rest[1],
          yield* body(LabelMembershipRequest, request),
        );
        return json(labelCommandBody(attached));
      }
      if (rest.length === 4 && rest[3] === "detach") {
        if (request.method !== "POST") return fail(405, "method-not-allowed");
        const detached = yield* detachLabel(
          workspaceId,
          rest[1],
          yield* body(LabelMembershipRequest, request),
        );
        return json(labelCommandBody(detached));
      }
      return fail(404, "not-found");
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

/** Project the membership workflow result onto the declared wire contract. */
function labelCommandBody(result: LabelCommandResult): JsonValue {
  return {
    commandId: result.commandId,
    workspaceId: result.workspaceId,
    issueId: result.issueId,
    labelId: result.labelId,
    membershipId: result.membershipId,
    attached: result.attached,
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

/** Every checked sink route is read-only; writing goes through the command routes. */
function readMethod(request: Request): boolean {
  return request.method === "GET" || request.method === "HEAD";
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
