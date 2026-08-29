/** The global domain: exchange cursors and durable source registry. */
import type { JsonValue } from "@streamsy/core";
import { Cause, Context, Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { partitionKeyString } from "../domain/domains.ts";
import { EXCHANGE_SCHEMA, exchangeCursorService, ExchangeCursorStore, exchangeMemoryLayer } from "./exchange-store.ts";
import { ExchangeSourceRegistry, sourceRegistryMemoryLayer, sourceRegistryService, SOURCE_REGISTRY_SCHEMA } from "./source-registry.ts";

export type GlobalServices = ExchangeCursorStore | ExchangeSourceRegistry;
export interface GlobalLayerOptions { readonly filename?: string }

export const migrateGlobalStore = Effect.fn("GlobalStore.migrate")(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const statement of `${EXCHANGE_SCHEMA}\n${SOURCE_REGISTRY_SCHEMA}`.split(";").map((value) => value.trim()).filter(Boolean)) {
    yield* sql.unsafe<Record<string, never>>(statement).pipe(Effect.asVoid);
  }
});

export const migratedGlobalSqlLayer: Layer.Layer<GlobalServices, unknown, SqlClient.SqlClient> =
  Layer.effectContext(Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migrateGlobalStore();
    return Context.empty().pipe(
      Context.add(ExchangeCursorStore, exchangeCursorService(sql)),
      Context.add(ExchangeSourceRegistry, sourceRegistryService(sql)),
    );
  }));

export const globalLayer = (options: GlobalLayerOptions = {}): Layer.Layer<GlobalServices> =>
  options.filename === undefined
    ? Layer.merge(exchangeMemoryLayer(), sourceRegistryMemoryLayer())
    : Layer.merge(exchangeMemoryLayer(), sourceRegistryMemoryLayer());

export const listExchangeCursors = Effect.fn("GlobalDomain.listExchangeCursors")(function* () {
  return yield* (yield* ExchangeCursorStore).list;
});
export const listExchangeSources = Effect.fn("GlobalDomain.listExchangeSources")(function* () {
  return yield* (yield* ExchangeSourceRegistry).list;
});

export const handleGlobalRequest = (request: Request): Effect.Effect<Response, never, GlobalServices> =>
  route(request).pipe(
    Effect.catchTags({
      ExchangeStoreUnavailable: (error) => Effect.succeed(fail(503, "exchange-store-unavailable", error.operation)),
      ExchangeCursorPoison: (error) => Effect.succeed(fail(500, "exchange-cursor-poison", `${error.exchange}@${error.source}: ${error.detail}`)),
    }),
    Effect.catchCause((cause) => Effect.succeed(Cause.hasInterrupts(cause) ? fail(499, "interrupted") : fail(500, "internal-error", Cause.pretty(cause).slice(0, 2_000)))),
  );

const route = (request: Request) => Effect.gen(function* () {
  const segments = globalSegments(new URL(request.url));
  if (segments === undefined) return fail(404, "not-found");
  if (segments[0] === "exchange" && segments.length === 1) {
    if (request.method !== "GET") return fail(405, "method-not-allowed");
    const cursors = yield* listExchangeCursors();
    return json({ cursors: cursors.map((cursor) => ({ domain: cursor.domain, exchange: cursor.exchange, version: cursor.version, source: { kind: cursor.source.kind, id: cursor.source.id }, arrival: cursor.arrival, applied: cursor.applied })) });
  }
  if (segments[0] === "sources" && segments.length === 1) {
    if (request.method !== "GET") return fail(405, "method-not-allowed");
    const sources = yield* listExchangeSources();
    return json({ sources: sources.map((source) => ({ partition: partitionKeyString(source.key), kind: source.key.kind, id: source.key.id, registeredAtMs: source.registeredAtMs, lastExchangedAtMs: source.lastExchangedAtMs })) });
  }
  return fail(404, "not-found");
});

function globalSegments(url: URL): readonly string[] | undefined {
  const prefix = "/api/global/";
  if (!url.pathname.startsWith(prefix)) return undefined;
  return url.pathname.slice(prefix.length).split("/").filter((segment) => segment.length > 0).map(decodeURIComponent);
}
const json = (body: JsonValue, status = 200): Response => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", "cache-control": "no-store" } });
const fail = (status: number, error: string, detail?: string): Response => json(detail === undefined ? { error } : { error, detail }, status);
