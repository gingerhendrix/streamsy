/**
 * The `stateSink` route.
 *
 * `boardIssues` declares `route`, `auth`, `protocol.resume` and
 * `protocol.fallback`. This module applies that declaration to the ordinary
 * Durable Streams read protocol. The client sends the native `offset` cursor.
 * An offset outside retained history gets the declared snapshot fallback.
 */
import { Effect } from "effect";
import { boardIssues, streamNames } from "../domain/declaration.ts";
import { SessionResumeUnavailable, Unauthorized } from "./errors.ts";
import { StreamGateway } from "./gateway.ts";
import { advance } from "./maintenance.ts";
import { ensureWorkspace } from "./streams.ts";

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
  const url = new URL(request.url);
  const scope = url.searchParams.get("scope") ?? request.headers.get(SCOPE_HEADER);
  if (scope !== boardIssues.auth.value) {
    return yield* new Unauthorized({ required: boardIssues.auth.value });
  }

  if (url.searchParams.get("live") === null) {
    yield* ensureWorkspace(workspaceId);
    yield* advance(workspaceId);
  }

  const offset = url.searchParams.get("offset");
  const target = new URL(request.url);
  target.pathname = `${gateway.prefix}/${streamNames.boardState(workspaceId)}`;
  target.searchParams.delete("scope");

  const proxied = yield* gateway.fetch(
    new Request(target, { method: request.method, headers: request.headers }),
  );

  if (offset !== null && offset !== "-1" && [400, 404, 410].includes(proxied.status)) {
    return yield* new SessionResumeUnavailable({
      sink: boardIssues.name,
      reason: "out-of-window",
      fallback: boardIssues.protocol.fallback,
    });
  }
  return proxied;
});
