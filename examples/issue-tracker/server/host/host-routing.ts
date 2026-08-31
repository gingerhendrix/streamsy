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
import {
  boardIssues,
  boardLabelCounts,
  issueTransitions,
  workspaceSummary,
} from "../../domain/declaration.ts";
import {
  globalKey,
  isDomainId,
  userKey,
  workspaceKey,
  type DomainKind,
  type PartitionKey,
} from "../../domain/domains.ts";
import {
  InvalidDomainId,
  InvalidWorkspaceId,
  UnroutableRequest,
  type HostFailure,
} from "./host-errors.ts";

/** Host-level routes. They are answered without opening any partition. */
export const HOST_HEALTH_PATH = "/health";
export const HOST_METRICS_PATH = "/host/metrics";

export type RouteResolution =
  /** A host-level route: the host is what is being asked about, not a partition. */
  | { readonly kind: "host"; readonly route: "health" | "metrics" }
  /** One partition owns this request, and the key says which domain it is in. */
  | {
      readonly kind: "partition";
      readonly key: PartitionKey;
      /** `application` runs the domain's router; `streams` goes straight to the gateway. */
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

/** Every checked sink route this host serves, in the order it tries them. */
const CHECKED_SINKS = [boardIssues, boardLabelCounts, issueTransitions, workspaceSummary] as const;

/**
 * Whether a path names this sink's route, ignoring parameter values.
 *
 * The compiled matcher walks segments left to right, so a route whose parameter
 * comes *before* a distinguishing literal reports an undecodable parameter
 * rather than a mismatch. Two State sinks now share
 * `/state/workspaces/:workspaceId/…`, so without this an unusable workspace id
 * on the label-count route would be refused in the board sink's name — telling
 * the caller about a contract it was not using. Comparing literal segments
 * picks the sink the caller actually named.
 */
function namesRoute(template: string, pathname: string): boolean {
  const expected = template.startsWith("/") ? template.slice(1).split("/") : [];
  const actual = pathname.startsWith("/") ? pathname.slice(1).split("/") : [];
  if (expected.length !== actual.length) return false;
  return expected.every((segment, index) => segment.startsWith(":") || segment === actual[index]);
}

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

  for (const sink of CHECKED_SINKS) {
    const matched = sink.compiledRoute.match(pathname);
    if (matched.kind === "matched") return workspace(matched.params.workspaceId, "application");
    if (matched.kind === "invalid" && namesRoute(sink.route, pathname)) {
      return { kind: "sink-params" };
    }
  }

  if (pathname.startsWith("/api/")) return resolveApiPath(pathname);

  if (SINK_PREFIXES.some((prefix) => pathname.startsWith(prefix))) return unroutable(pathname);
  return { kind: "asset" };
}

/**
 * The API surface, one collection segment per domain.
 *
 * The collection name is what names the domain — `workspaces`, `users`,
 * `global` — so a path that names no collection this host serves is a 404
 * rather than an unkeyed request some partition might answer.
 */
function resolveApiPath(pathname: string): RouteResolution {
  const segments = pathSegments(pathname.slice("/api/".length));
  const [collection, id] = segments;
  if (collection === "workspaces") {
    return id === undefined ? unroutable(pathname) : workspace(id, "application");
  }
  if (collection === "users") {
    return id === undefined ? unroutable(pathname) : user(id);
  }
  // The global domain is a singleton, so its collection segment *is* its id.
  if (collection === "global") {
    return segments.length < 2 ? unroutable(pathname) : partition(globalKey(), "application");
  }
  return unroutable(pathname);
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
  if (!isDomainId("workspace", workspaceId)) {
    return {
      kind: "failure",
      failure: new InvalidWorkspaceId({
        workspaceId: workspaceId.slice(0, 80),
        detail: "not a workspace identifier",
      }),
    };
  }
  return partition(workspaceKey(workspaceId), target);
}

/**
 * A user id the host will accept as a partition key.
 *
 * It is refused with `InvalidDomainId` rather than `InvalidWorkspaceId`,
 * because a user is not a workspace and a caller told otherwise would look for
 * the wrong thing.
 */
function user(userId: string): RouteResolution {
  return isDomainId("user", userId)
    ? partition(userKey(userId), "application")
    : invalidDomainId("user", userId);
}

function invalidDomainId(kind: DomainKind, id: string): RouteResolution {
  return {
    kind: "failure",
    failure: new InvalidDomainId({
      domain: kind,
      id: id.slice(0, 80),
      detail: `not a ${kind} identifier`,
    }),
  };
}

function partition(key: PartitionKey, target: "application" | "streams"): RouteResolution {
  return { kind: "partition", key, target };
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
  if (namesRoute(boardIssues.route, pathname)) {
    return Effect.runSync(
      handleStateSink(boardIssues, request, {
        snapshot: () => unreachableCapability("board-issues.snapshot"),
        suffix: () => unreachableCapability("board-issues.suffix"),
      }),
    );
  }
  if (namesRoute(boardLabelCounts.route, pathname)) {
    return Effect.runSync(
      handleStateSink(boardLabelCounts, request, {
        snapshot: () => unreachableCapability("board-label-counts.snapshot"),
        suffix: () => unreachableCapability("board-label-counts.suffix"),
      }),
    );
  }
  if (namesRoute(issueTransitions.route, pathname)) {
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
