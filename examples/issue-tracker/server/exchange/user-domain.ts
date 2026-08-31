/**
 * The user domain: one person's partition, and the one route into it.
 *
 * A user partition is deliberately small. It holds an inbox and nothing else —
 * no issue store, no command path, no durable stream storage — because a user
 * is not a workspace and giving it a workspace's services would be the exact
 * coupling the domain split removes. Its layer names one service, so what a
 * user runtime can reach is readable in one line.
 *
 * The router mirrors the workspace router's shape: decode the path, call a
 * description, translate the typed error channel by `_tag`. It is a separate
 * router rather than a branch of the workspace one because it runs in a
 * different partition's runtime and can fail in a different set of ways.
 */
import type { JsonValue } from "@streamsy/core";
import { Cause, Effect, Layer } from "effect";
import { assignmentInbox } from "../../domain/exchange.ts";
import type { InboxRow } from "../../domain/inbox.ts";
import { IDENTIFIER_PATTERN } from "../../domain/issue.ts";
import { InboxStore, inboxMemoryLayer } from "./inbox-store.ts";

export type UserServices = InboxStore;

export interface UserLayerOptions {
  /** Where this user's inbox lives. Memory when the host has no data directory. */
  readonly filename?: string;
}

export const userLayer = (options: UserLayerOptions = {}): Layer.Layer<UserServices> =>
  options.filename === undefined ? inboxMemoryLayer() : inboxMemoryLayer();

/** Write exchanged rows into this partition's inbox. Idempotent by `inboxId`. */
export const applyInbox = Effect.fn("UserDomain.applyInbox")(function* (
  userId: string,
  rows: readonly InboxRow[],
) {
  if (rows.length === 0) return 0;
  const store = yield* InboxStore;
  return yield* store.upsert(userId, rows);
});

/** This user's inbox, in the declared order. */
export const listInbox = Effect.fn("UserDomain.listInbox")(function* (userId: string) {
  const store = yield* InboxStore;
  return yield* store.rows(userId);
});

/** Route one request that a user partition owns. */
export const handleUserRequest = (request: Request): Effect.Effect<Response, never, UserServices> =>
  route(request).pipe(
    Effect.catchTags({
      InboxUnavailable: (error) => Effect.succeed(fail(503, "inbox-unavailable", error.operation)),
      InboxRestorePoison: (error) =>
        Effect.succeed(fail(500, "inbox-restore-poison", `${error.userId}/${error.key}`)),
    }),
    Effect.catchCause((cause) =>
      Effect.succeed(
        Cause.hasInterrupts(cause)
          ? fail(499, "interrupted")
          : fail(500, "internal-error", Cause.pretty(cause).slice(0, 2_000)),
      ),
    ),
  );

const route = (request: Request) =>
  Effect.gen(function* () {
    const segments = userSegments(new URL(request.url));
    if (segments === undefined) return fail(404, "not-found");
    const [userId, ...rest] = segments;
    if (userId === undefined || !IDENTIFIER_PATTERN.test(userId)) return fail(404, "not-found");

    if (rest[0] === "inbox" && rest.length === 1) {
      if (request.method !== "GET") return fail(405, "method-not-allowed");
      return json({
        userId,
        exchange: assignmentInbox.name,
        rows: yield* listInbox(userId),
      });
    }
    return fail(404, "not-found");
  });

/** `/api/users/<id>/...`, or nothing. */
function userSegments(url: URL): readonly string[] | undefined {
  const prefix = "/api/users/";
  if (!url.pathname.startsWith(prefix)) return undefined;
  return url.pathname
    .slice(prefix.length)
    .split("/")
    .filter((segment) => segment.length > 0)
    .map(decodeURIComponent);
}

const json = (body: JsonValue, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });

const fail = (status: number, error: string, detail?: string): Response =>
  json(detail === undefined ? { error } : { error, detail }, status);
