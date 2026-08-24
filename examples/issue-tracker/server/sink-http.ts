/**
 * The `stateSink` route.
 *
 * `boardIssues` declares `route`, `auth`, `protocol.resume` and
 * `protocol.fallback`. This module is that declaration made servable:
 *
 * - the declared scope is required before anything is read;
 * - a resume token is verified and turned into a protocol offset;
 * - a token this sink will not honour is a typed `SessionResumeExpired`,
 *   carrying the fallback the consumer should take;
 * - every response carries a freshly minted token for its own tail, so a
 *   consumer's next session resumes exactly after what it received.
 *
 * The transport underneath is the ordinary Durable Streams read protocol, so a
 * consumer can use the standard client rather than a bespoke one.
 */
import { Effect } from "effect";
import { boardIssues, streamNames } from "../domain/declaration.ts";
import { advance } from "./maintenance.ts";
import { SessionResumeExpired, Unauthorized } from "./errors.ts";
import { StreamGateway } from "./gateway.ts";
import { IssueSink, resumeOutOfWindow } from "./sink.ts";
import { ensureWorkspace } from "./streams.ts";

/** Header the sink answers with, and the one a consumer echoes back as `?resume=`. */
export const RESUME_HEADER = "x-streamsy-resume";
export const SCOPE_HEADER = "x-streamsy-scope";

/** `/state/workspaces/{workspaceId}/issues` → the workspace id, or undefined. */
export function sinkWorkspaceId(pathname: string): string | undefined {
  const template = boardIssues.route.split("/").filter((segment) => segment.length > 0);
  const actual = pathname.split("/").filter((segment) => segment.length > 0);
  if (template.length !== actual.length) return undefined;
  let workspaceId: string | undefined;
  for (const [index, segment] of template.entries()) {
    const value = actual[index];
    if (value === undefined) return undefined;
    if (segment.startsWith(":")) {
      if (segment !== ":workspaceId") return undefined;
      workspaceId = decodeURIComponent(value);
      continue;
    }
    if (segment !== value) return undefined;
  }
  return workspaceId;
}

/** Serve one sink request for a resolved workspace. */
export const handleSinkRequest = Effect.fn("Sink.handleRequest")(function* (
  request: Request,
  workspaceId: string,
) {
  const gateway = yield* StreamGateway;
  const sink = yield* IssueSink;

  const url = new URL(request.url);
  const scope = url.searchParams.get("scope") ?? request.headers.get(SCOPE_HEADER);
  if (scope !== boardIssues.auth.value) {
    return yield* new Unauthorized({ required: boardIssues.auth.value });
  }

  const live = url.searchParams.get("live");
  if (live === null) {
    // A catch-up read is a snapshot request, so the product is brought up to
    // its source before it is served. A live read is already following the
    // tail and must not block on maintenance.
    yield* ensureWorkspace(workspaceId);
    yield* advance(workspaceId);
  }

  const resume = url.searchParams.get("resume");
  const target = new URL(request.url);
  target.pathname = `${gateway.prefix}/${streamNames.boardState(workspaceId)}`;
  target.searchParams.delete("resume");
  target.searchParams.delete("scope");

  if (resume !== null) {
    const position = yield* sink.verifyResume(workspaceId, resume);
    target.searchParams.set("offset", position.offset);
  }

  const proxied = yield* gateway.fetch(
    new Request(target, { method: request.method, headers: request.headers }),
  );

  // An offset the stream can no longer serve is the same operational outcome as
  // an aged-out token, and it gets the same declared fallback.
  if (resume !== null && (proxied.status === 404 || proxied.status === 410)) {
    return yield* resumeOutOfWindow();
  }

  const nextOffset = proxied.headers.get("stream-next-offset");
  if (nextOffset === null) return proxied;

  const headers = new Headers(proxied.headers);
  headers.set(RESUME_HEADER, yield* sink.mintResume(workspaceId, nextOffset));
  return new Response(proxied.body, { status: proxied.status, headers });
});

export { SessionResumeExpired };
