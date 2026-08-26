/**
 * The global domain: the host's own partition.
 *
 * Exactly one exists, and it holds what belongs to no workspace and no user —
 * today, the exchange cursors. It is a partition rather than a field on the
 * host object for one reason: a cursor is durable state, and every other piece
 * of durable state in this host lives behind a partition with a lifecycle, a
 * lease and a metric. Giving the exchange's position the same treatment is
 * what keeps "the host owns no durable state directly" true.
 *
 * Its one route is read-only. Nothing here restarts, rewinds or clears a
 * cursor over HTTP: rewinding an exchange is a destructive operation, access
 * control does not arrive until C1, and an unauthenticated route that can
 * replay every workspace into every inbox is not something to ship first and
 * protect later.
 */
import type { JsonValue } from "@streamsy/core";
import { Cause, Effect, Layer } from "effect";
import { ExchangeCursorStore, exchangeMemoryLayer, exchangeSqliteLayer } from "./exchange-store.ts";

export type GlobalServices = ExchangeCursorStore;

export interface GlobalLayerOptions {
  /** Where the cursors live. Memory when the host has no data directory. */
  readonly filename?: string;
}

export const globalLayer = (options: GlobalLayerOptions = {}): Layer.Layer<GlobalServices> =>
  options.filename === undefined
    ? exchangeMemoryLayer()
    : exchangeSqliteLayer({ filename: options.filename });

/** Every exchange position this host holds. */
export const listExchangeCursors = Effect.fn("GlobalDomain.listExchangeCursors")(function* () {
  const cursors = yield* ExchangeCursorStore;
  return yield* cursors.list();
});

/** Route one request that the global partition owns. */
export const handleGlobalRequest = (
  request: Request,
): Effect.Effect<Response, never, GlobalServices> =>
  route(request).pipe(
    Effect.catchTags({
      ExchangeStoreUnavailable: (error) =>
        Effect.succeed(fail(503, "exchange-store-unavailable", error.operation)),
      ExchangeCursorPoison: (error) =>
        Effect.succeed(
          fail(500, "exchange-cursor-poison", `${error.exchange}@${error.source}: ${error.detail}`),
        ),
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
    const segments = globalSegments(new URL(request.url));
    if (segments === undefined) return fail(404, "not-found");
    if (segments[0] === "exchange" && segments.length === 1) {
      if (request.method !== "GET") return fail(405, "method-not-allowed");
      const cursors = yield* listExchangeCursors();
      return json({
        cursors: cursors.map((cursor) => ({
          domain: cursor.domain,
          exchange: cursor.exchange,
          version: cursor.version,
          source: { kind: cursor.source.kind, id: cursor.source.id },
          arrival: cursor.arrival,
          applied: cursor.applied,
        })),
      });
    }
    return fail(404, "not-found");
  });

/** `/api/global/...`, or nothing. */
function globalSegments(url: URL): readonly string[] | undefined {
  const prefix = "/api/global/";
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
