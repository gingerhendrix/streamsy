/**
 * Which partition owns a request.
 *
 * Route resolution is pure and lives apart from the host that acts on it, so
 * the ownership rule — *this path belongs to exactly this workspace* — is a
 * value a test can assert directly, without a running server.
 *
 * The sink routes are resolved through the declaration's own compiled routes
 * rather than through a second set of patterns written here. A sink route and
 * the partition it is served from therefore cannot drift apart: if the
 * declaration moves a route, resolution moves with it.
 */
import { Effect } from "effect";
import { handleStateSink } from "@streamsy/state-sink/effect";
import { handleDocumentSink, handleStreamSink } from "@streamsy/sinks/effect";
import { boardIssues, issueTransitions, workspaceSummary } from "../domain/declaration.ts";
import { IDENTIFIER_PATTERN } from "../domain/issue.ts";
import { InvalidWorkspaceId, UnroutableRequest, type HostFailure } from "./host-errors.ts";

/** Host-level routes. They are answered without opening any partition. */
export const HOST_HEALTH_PATH = "/health";
export const HOST_METRICS_PATH = "/host/metrics";

export type RouteResolution =
  /** A host-level route: the host is what is being asked about, not a workspace. */
  | { readonly kind: "host"; readonly route: "health" | "metrics" }
  /** One workspace's partition owns this request. */
  | {
      readonly kind: "workspace";
      readonly workspaceId: string;
      /** `application` runs the router; `streams` goes straight to the partition gateway. */
      readonly target: "application" | "streams";
    }
  /**
   * A checked sink route matched, but its parameters do not decode.
   *
   * There is no partition to route this to — the id is not a key — and the
   * answer is the sink's own declared `InvalidSinkParams` error, so the host
   * asks the sink package for it rather than inventing a second body.
   */
  | { readonly kind: "sink-params" }
  /** Not an application path at all. The host serves it however it serves files. */
  | { readonly kind: "asset" }
  | { readonly kind: "failure"; readonly failure: HostFailure };

const SINK_PREFIXES = ["/state/", "/feed/", "/document/"] as const;

/**
 * Resolve one path to its owner.
 *
 * Order matters and is deliberate: host routes first, then the raw stream
 * transport, then the checked sinks, then the command API. Anything left that
 * looks like an application path is a 404 rather than a static file, so a
 * mistyped sink route never quietly returns the browser shell.
 */
export function resolveRoute(pathname: string, streamsPrefix = "/streams"): RouteResolution {
  if (pathname === HOST_HEALTH_PATH) return { kind: "host", route: "health" };
  if (pathname === HOST_METRICS_PATH) return { kind: "host", route: "metrics" };

  if (pathname === streamsPrefix || pathname.startsWith(`${streamsPrefix}/`)) {
    return resolveStreamPath(pathname.slice(streamsPrefix.length), pathname);
  }

  for (const matched of [
    boardIssues.compiledRoute.match(pathname),
    issueTransitions.compiledRoute.match(pathname),
    workspaceSummary.compiledRoute.match(pathname),
  ]) {
    if (matched.kind === "matched") return workspace(matched.params.workspaceId, "application");
    if (matched.kind === "invalid") return { kind: "sink-params" };
  }

  if (pathname.startsWith("/api/")) {
    const segments = pathSegments(pathname.slice("/api/".length));
    const workspaceId = segments[0] === "workspaces" ? segments[1] : undefined;
    if (workspaceId === undefined) return unroutable(pathname);
    return workspace(workspaceId, "application");
  }

  if (SINK_PREFIXES.some((prefix) => pathname.startsWith(prefix))) return unroutable(pathname);
  return { kind: "asset" };
}

/**
 * Every stream this application owns is named `.../workspaces/<id>/...`, so the
 * workspace is read out of the stream id itself. The `workspaces` segment is
 * only honoured in the first two positions, which is where the declaration puts
 * it — a stream that merely contains the word later is not a routing key.
 */
function resolveStreamPath(rest: string, pathname: string): RouteResolution {
  const segments = pathSegments(rest);
  const index = segments.indexOf("workspaces");
  const workspaceId = index === 0 || index === 1 ? segments[index + 1] : undefined;
  if (workspaceId === undefined) return unroutable(pathname);
  return workspace(workspaceId, "streams");
}

/**
 * A partition key is a workspace id, and the host accepts exactly the ids the
 * domain accepts. That is also what keeps the key usable as a directory name
 * for a durable partition: the pattern admits no separator and no traversal.
 */
function workspace(workspaceId: string, target: "application" | "streams"): RouteResolution {
  if (!IDENTIFIER_PATTERN.test(workspaceId)) {
    return {
      kind: "failure",
      failure: new InvalidWorkspaceId({
        workspaceId: workspaceId.slice(0, 80),
        detail: "not a workspace identifier",
      }),
    };
  }
  return { kind: "workspace", workspaceId, target };
}

function unroutable(pathname: string): RouteResolution {
  return { kind: "failure", failure: new UnroutableRequest({ pathname }) };
}

function pathSegments(rest: string): readonly string[] {
  return rest
    .split("/")
    .filter((segment) => segment.length > 0)
    .map(decodeSegment);
}

function decodeSegment(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    // A malformed escape is not a workspace id; leaving it verbatim lets the
    // identifier check reject it as the typed failure it is.
    return segment;
  }
}

/**
 * The sink's own answer to an undecodable route parameter.
 *
 * The capabilities are unreachable by construction: every checked sink handler
 * decides `InvalidSinkParams` from the route alone, before it asks its source
 * for anything. Calling the real handlers is what makes this response the
 * sink's contract rather than a copy of it.
 */
export function invalidSinkParamsResponse(request: Request): Response {
  const pathname = new URL(request.url).pathname;
  if (boardIssues.compiledRoute.match(pathname).kind === "invalid") {
    return Effect.runSync(
      handleStateSink(boardIssues, request, {
        snapshot: () => unreachableCapability("board-issues.snapshot"),
        suffix: () => unreachableCapability("board-issues.suffix"),
      }),
    );
  }
  if (issueTransitions.compiledRoute.match(pathname).kind === "invalid") {
    return Effect.runSync(
      handleStreamSink(issueTransitions, request, {
        read: () => unreachableCapability("issue-transitions.read"),
      }),
    );
  }
  return Effect.runSync(
    handleDocumentSink(workspaceSummary, request, {
      document: () => unreachableCapability("workspace-summary.document"),
    }),
  );
}

const unreachableCapability = (name: string): Effect.Effect<never> =>
  Effect.die(new Error(`${name} is unreachable for an undecodable sink route`));
