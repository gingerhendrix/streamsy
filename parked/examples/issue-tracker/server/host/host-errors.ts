/**
 * Typed operational failures of the host itself.
 *
 * `errors.ts` owns what the *application* can fail with; these are what the
 * *host* can fail with, and they are a different domain. An application error
 * is about one workspace's facts, rows or streams. A host error is about
 * routing a request to a partition at all: the id is not one this host will
 * accept, the path names nothing, the host is shutting down, or the partition
 * could not be opened.
 *
 * They never travel in an Effect error channel, because they are decided
 * before any partition runtime exists to run an Effect in. They are values the
 * host's `fetch` returns, not exceptions it throws, so every rejection is
 * counted and translated at exactly one place.
 */
import { Schema } from "effect";

/** A workspace id the host will not use as a partition key. */
export class InvalidWorkspaceId extends Schema.TaggedError<InvalidWorkspaceId>()(
  "InvalidWorkspaceId",
  { workspaceId: Schema.String, detail: Schema.String },
) {}

/**
 * An id the host will not use as a partition key in the domain that named it.
 *
 * Kept apart from {@link InvalidWorkspaceId} because a caller told "invalid
 * workspace id" when it asked about a user would look in the wrong place, and
 * the two failures reach different routes.
 */
export class InvalidDomainId extends Schema.TaggedError<InvalidDomainId>()("InvalidDomainId", {
  domain: Schema.String,
  id: Schema.String,
  detail: Schema.String,
}) {}

/** A path that names no host route, no sink route and no partition. */
export class UnroutableRequest extends Schema.TaggedError<UnroutableRequest>()(
  "UnroutableRequest",
  {
    pathname: Schema.String,
  },
) {}

/** The host is closing or closed. Requests are refused rather than half-served. */
export class HostClosed extends Schema.TaggedError<HostClosed>()("HostClosed", {
  pathname: Schema.String,
}) {}

/**
 * Every partition slot is taken and none is evictable.
 *
 * Reported rather than queued: a host that silently waits for a slot turns a
 * capacity problem into a latency problem, and a keyed host is exactly the
 * place where capacity should be visible.
 */
export class PartitionLimitReached extends Schema.TaggedError<PartitionLimitReached>()(
  "PartitionLimitReached",
  { partition: Schema.String, maxOpen: Schema.Finite },
) {}

/** A partition's storage, store or runtime could not be constructed. */
export class PartitionUnavailable extends Schema.TaggedError<PartitionUnavailable>()(
  "PartitionUnavailable",
  { partition: Schema.String, detail: Schema.String },
) {}

/** A valid domain route whose Cloudflare placement belongs to a later slice. */
export class DomainPlacementUnavailable extends Schema.TaggedError<DomainPlacementUnavailable>()(
  "DomainPlacementUnavailable",
  { domain: Schema.String, id: Schema.String },
) {}

export type HostFailure =
  | InvalidWorkspaceId
  | InvalidDomainId
  | UnroutableRequest
  | HostClosed
  | PartitionLimitReached
  | PartitionUnavailable
  | DomainPlacementUnavailable;

interface HostFailureReport {
  readonly status: number;
  readonly error: string;
  readonly detail: string;
}

const absurd = (value: never): never => {
  throw new TypeError(`unexpected host failure: ${String(value)}`);
};

/** How every host failure is reported publicly, chosen by `_tag` and nothing else. */
export function hostFailureReport(failure: HostFailure): HostFailureReport {
  const { _tag: tag } = failure;
  switch (tag) {
    case "InvalidWorkspaceId":
      return {
        status: 400,
        error: "invalid-workspace-id",
        detail: `${failure.workspaceId}: ${failure.detail}`,
      };
    case "InvalidDomainId":
      return {
        status: 400,
        error: "invalid-domain-id",
        detail: `${failure.domain}/${failure.id}: ${failure.detail}`,
      };
    case "UnroutableRequest":
      return { status: 404, error: "not-found", detail: failure.pathname };
    case "HostClosed":
      return { status: 503, error: "host-closed", detail: failure.pathname };
    case "PartitionLimitReached":
      return {
        status: 503,
        error: "partition-limit-reached",
        detail: `${failure.partition}: ${failure.maxOpen} partitions open`,
      };
    case "PartitionUnavailable":
      return {
        status: 503,
        error: "partition-unavailable",
        detail: `${failure.partition}: ${failure.detail}`,
      };
    case "DomainPlacementUnavailable":
      return {
        status: 503,
        error: "domain-placement-unavailable",
        detail: `${failure.domain}/${failure.id}: unavailable in cloudflare workspace placement`,
      };
  }
  return absurd(failure);
}

export function hostFailureResponse(failure: HostFailure): Response {
  const report = hostFailureReport(failure);
  return new Response(JSON.stringify({ error: report.error, detail: report.detail }), {
    status: report.status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}
