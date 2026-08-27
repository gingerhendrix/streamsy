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
import { Database } from "bun:sqlite";
import type { JsonValue } from "@streamsy/core";
import { Cause, Context, Effect, Layer } from "effect";
import { partitionKeyString } from "../domain/domains.ts";
import {
  EXCHANGE_SCHEMA,
  exchangeCursorService,
  ExchangeCursorStore,
  exchangeMemoryLayer,
} from "./exchange-store.ts";
import {
  ExchangeSourceRegistry,
  sourceRegistryMemoryLayer,
  sourceRegistryService,
  SOURCE_REGISTRY_SCHEMA,
} from "./source-registry.ts";

export type GlobalServices = ExchangeCursorStore | ExchangeSourceRegistry;

export interface GlobalLayerOptions {
  /** Where the cursors live. Memory when the host has no data directory. */
  readonly filename?: string;
}

/**
 * The global partition's one connection.
 *
 * Cursors and the source registry are two surfaces over one file for the same
 * reason the workspace's rows and outbox are: they are one partition's durable
 * state, and a partition owns one connection. Naming the connection is what
 * lets two layers share it without either one owning the other.
 */
class GlobalDatabase extends Context.Service<GlobalDatabase, Database>()(
  "issue-tracker/GlobalDatabase",
) {}

const databaseLayer = (filename: string): Layer.Layer<GlobalDatabase> =>
  Layer.effect(
    GlobalDatabase,
    Effect.acquireRelease(
      Effect.sync(() => {
        const database = new Database(filename, { create: true });
        database.exec("PRAGMA journal_mode = WAL");
        database.exec(EXCHANGE_SCHEMA);
        database.exec(SOURCE_REGISTRY_SCHEMA);
        return database;
      }),
      (database) => Effect.sync(() => database.close(false)),
    ),
  );

export const globalLayer = (options: GlobalLayerOptions = {}): Layer.Layer<GlobalServices> => {
  const filename = options.filename;
  if (filename === undefined) {
    return Layer.merge(exchangeMemoryLayer(), sourceRegistryMemoryLayer());
  }
  return Layer.merge(
    Layer.effect(ExchangeCursorStore, Effect.map(GlobalDatabase, exchangeCursorService)),
    Layer.effect(ExchangeSourceRegistry, Effect.map(GlobalDatabase, sourceRegistryService)),
  ).pipe(Layer.provide(databaseLayer(filename)));
};

/** Every exchange position this host holds. */
export const listExchangeCursors = Effect.fn("GlobalDomain.listExchangeCursors")(function* () {
  const cursors = yield* ExchangeCursorStore;
  return yield* cursors.list;
});

/** Every source this host's exchange is responsible for. */
export const listExchangeSources = Effect.fn("GlobalDomain.listExchangeSources")(function* () {
  const registry = yield* ExchangeSourceRegistry;
  return yield* registry.list;
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
    if (segments[0] === "sources" && segments.length === 1) {
      if (request.method !== "GET") return fail(405, "method-not-allowed");
      const sources = yield* listExchangeSources();
      return json({
        sources: sources.map((source) => ({
          partition: partitionKeyString(source.key),
          kind: source.key.kind,
          id: source.key.id,
          registeredAtMs: source.registeredAtMs,
          lastExchangedAtMs: source.lastExchangedAtMs,
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
