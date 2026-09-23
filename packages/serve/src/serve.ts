import { Effect, Layer, Schema, Result } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Http, Offset, type StreamsReader, type StreamRef, type StreamRoute } from "@streamsy/core";
import * as Contract from "./contract.ts";
import {
  normalize,
  type StreamSource,
  type StateSource,
  type PathSource,
  type Source,
} from "./source.ts";
import { PublicErrorSchema } from "./errors.ts";
export type { StreamSource, StateSource, PathSource } from "./source.ts";

type RouteLayer<R, E = never> = Layer.Layer<
  never,
  never,
  | HttpRouter.HttpRouter
  | HttpRouter.Request.From<"Requires", Exclude<R, HttpRouter.Provided>>
  | HttpRouter.Request.From<"Error", E>
>;
type Names<S extends string> = S extends `${string}:${infer N}/${infer Tail}`
  ? N | Names<Tail>
  : S extends `${string}:${infer N}`
    ? N
    : never;
type ExactPath<Path extends string, P> = `/${string}` extends Path
  ? unknown
  : [Exclude<Names<Path>, keyof P>, Exclude<keyof P, Names<Path>>] extends [never, never]
    ? unknown
    : never;
export interface ReadOptions extends Http.ReadOptions {
  readonly contract?: Contract.CanonicalValue;
}
export interface ParamsOptions<P, E = never, R = never> extends ReadOptions {
  readonly params: Effect.Effect<P, E, R>;
}
export function stream<P, const Path extends `/${string}`>(
  source: StreamSource<P> & PathSource<P>,
  path: Path & ExactPath<Path, NoInfer<P>>,
  options?: ReadOptions,
): RouteLayer<StreamsReader>;
export function stream<P, E, R>(
  source: StreamSource<P>,
  path: `/${string}`,
  options: ParamsOptions<NoInfer<P>, E, R>,
): RouteLayer<StreamsReader | R, E>;
export function stream<P, E, R>(
  source: StreamSource<P>,
  path: `/${string}`,
  options: ReadOptions | ParamsOptions<P, E, R> = {},
): RouteLayer<StreamsReader | R, E> {
  return readRoute(normalize(source, path), path, options);
}
export function state<P, C extends StreamRef.Collections, const Path extends `/${string}`>(
  source: StateSource<P, C> & PathSource<P>,
  path: Path & ExactPath<Path, NoInfer<P>>,
  options?: ReadOptions,
): RouteLayer<StreamsReader>;
export function state<P, C extends StreamRef.Collections, E, R>(
  source: StateSource<P, C>,
  path: `/${string}`,
  options: ParamsOptions<NoInfer<P>, E, R>,
): RouteLayer<StreamsReader | R, E>;
export function state<P, C extends StreamRef.Collections, E, R>(
  source: StateSource<P, C>,
  path: `/${string}`,
  options: ReadOptions | ParamsOptions<P, E, R> = {},
): RouteLayer<StreamsReader | R, E> {
  return readRoute(normalize(source, path, "state"), path, options);
}

function params<P>(schema: StreamRoute.PathSchema<P> | undefined, path: string) {
  if (!schema) throw new TypeError("Path source requires paramSchema or an explicit params Effect");
  const names = pathNames(path);
  // Struct's encoded object properties are the original family's parameter names.
  const fields = Schema.toJsonSchemaDocument(schema).schema.properties ?? {};
  if (Object.keys(fields).toSorted().join("\0") !== names.toSorted().join("\0"))
    throw new TypeError("Route params must equal family params");
  return HttpRouter.schemaPathParams(schema).pipe(
    Effect.mapError(() => ({
      _tag: "InvalidParams" as const,
      route: path,
      parameter: "params",
      detail: "Invalid path parameters",
    })),
  );
}
function pathNames(path: string): string[] {
  const names: string[] = [];
  for (const segment of path.slice(1).split("/")) {
    if (!segment || /[?*#()]/.test(segment))
      throw new TypeError("Serve paths require literal segments or :name parameters");
    if (segment.startsWith(":")) {
      const name = segment.slice(1);
      if (!name || names.includes(name)) throw new TypeError("Invalid or duplicate path parameter");
      names.push(name);
    }
  }
  return names;
}
function errorResponse(
  error: Contract.PublicError,
  status: number,
  headers: Record<string, string>,
) {
  const body = Schema.encodeSync(PublicErrorSchema)(error);
  return HttpServerResponse.jsonUnsafe(body, {
    status,
    headers: { ...Http.securityHeaders, ...headers, "cache-control": "no-store" },
  });
}
function methodResponse() {
  return HttpServerResponse.empty({
    status: 405,
    headers: { ...Http.securityHeaders, allow: "GET, HEAD" },
  });
}
function readRoute<P, E, R>(
  source: Source<P>,
  path: `/${string}`,
  options: ReadOptions | ParamsOptions<P, E, R>,
): RouteLayer<StreamsReader | R, E> {
  pathNames(path);
  const selected = "params" in options ? undefined : params(source.paramSchema, path);
  const versionHeader =
    source.kind === "state" ? Contract.STATE_VERSION_HEADER : Contract.STREAM_VERSION_HEADER;
  const contractHeader =
    source.kind === "state" ? Contract.STATE_CONTRACT_HEADER : Contract.STREAM_CONTRACT_HEADER;
  const handler = Effect.gen(function* (): Effect.fn.Return<
    HttpServerResponse.HttpServerResponse,
    E,
    StreamsReader | HttpServerRequest.HttpServerRequest | HttpRouter.RouteContext | R
  > {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.method !== "GET" && request.method !== "HEAD") return methodResponse();
    const decoded =
      "params" in options
        ? Result.succeed(yield* options.params)
        : yield* Effect.result(selected ?? params(source.paramSchema, path));
    if (Result.isFailure(decoded)) return errorResponse(decoded.failure, 400, {});
    const resolution = yield* Effect.result(source.resolve(decoded.success));
    if (Result.isFailure(resolution)) return errorResponse(resolution.failure, 400, {});
    const resolved = resolution.success;
    const fingerprint = Contract.contractFingerprint({
      kind: source.kind,
      route: path,
      source: source.template,
      id: resolved.id,
      collections: resolved.descriptor,
      params: pathNames(path).toSorted(),
      version: 1,
      recovery: "replay-from-start",
      contract: options.contract ?? null,
    });
    const headers = { [versionHeader]: "1", [contractHeader]: fingerprint };
    const fail = (error: Contract.PublicError, status: number) =>
      errorResponse(error, status, headers);
    const received = request.headers[versionHeader] ?? "1";
    if (received !== "1")
      return fail(
        {
          _tag: "ProtocolVersionUnsupported",
          route: path,
          supported: 1,
          received,
          recovery: "replay-from-start",
        },
        409,
      );
    if (
      request.headers[contractHeader] !== undefined &&
      request.headers[contractHeader] !== fingerprint
    )
      return fail(
        {
          _tag: "ResumeRejected",
          route: path,
          reason: "contract-changed",
          recovery: "replay-from-start",
        },
        409,
      );
    if (source.kind === "state" && request.headers["x-streamsy-state-reset"] === "snapshot")
      return HttpServerResponse.text("Snapshot reset is unsupported; replay from -1", {
        status: 400,
        headers: { ...headers, ...Http.securityHeaders },
      });
    const offset = new URL(request.url, "http://streamsy.internal").searchParams.get("offset");
    const resume =
      offset !== null && offset !== "-1" && offset !== "now" && request.method !== "HEAD";
    if (resume && !Schema.is(Offset)(offset))
      return fail(
        {
          _tag: "ResumeRejected",
          route: path,
          reason: "invalid-offset",
          recovery: "replay-from-start",
        },
        409,
      );
    return yield* Http.read(resolved.id, options).pipe(
      Effect.map(HttpServerResponse.setHeaders(headers)),
      Effect.catchTags({
        StreamNotFound: () =>
          Effect.succeed(
            resume
              ? fail(
                  {
                    _tag: "ResumeRejected",
                    route: path,
                    reason: "history-unavailable",
                    recovery: "replay-from-start",
                  },
                  409,
                )
              : HttpServerResponse.empty({
                  status: 404,
                  headers: { ...headers, ...Http.securityHeaders },
                }),
          ),
        StreamGone: () =>
          Effect.succeed(
            resume
              ? fail(
                  {
                    _tag: "ResumeRejected",
                    route: path,
                    reason: "history-unavailable",
                    recovery: "replay-from-start",
                  },
                  409,
                )
              : HttpServerResponse.empty({
                  status: 410,
                  headers: { ...headers, ...Http.securityHeaders },
                }),
          ),
        InvalidReadRequest: () =>
          Effect.succeed(
            fail(
              {
                _tag: "ResumeRejected",
                route: path,
                reason: "invalid-offset",
                recovery: "replay-from-start",
              },
              409,
            ),
          ),
        NotSupported: () =>
          Effect.succeed(
            HttpServerResponse.empty({
              status: 400,
              headers: { ...headers, ...Http.securityHeaders },
            }),
          ),
        StorageFault: () =>
          Effect.succeed(
            fail({ _tag: "TransportUnavailable", route: path, detail: "Storage unavailable" }, 503),
          ),
        TransportFault: () =>
          Effect.succeed(
            fail(
              { _tag: "TransportUnavailable", route: path, detail: "Transport unavailable" },
              503,
            ),
          ),
      }),
    );
  });
  return HttpRouter.add<
    E,
    StreamsReader | HttpServerRequest.HttpServerRequest | HttpRouter.RouteContext | R
  >("*", path, handler);
}

export interface ValueUnavailable {
  readonly _tag: "ValueUnavailable";
  readonly detail: string;
}
export interface ValueSource<P, A, R = never, RE = never> {
  readonly id: string;
  readonly paramSchema: StreamRoute.PathSchema<P>;
  readonly schema: Schema.Codec<A, Contract.CanonicalValue, never, RE>;
  readonly resolve: (params: P) => Effect.Effect<A, ValueUnavailable, R>;
}
export interface DocumentOptions {
  readonly cache?: {
    readonly visibility: "private" | "public";
    readonly maxAgeSeconds: number;
    readonly mustRevalidate: boolean;
  };
  readonly contract?: Contract.CanonicalValue;
}
export function document<P, A, R, RE, const Path extends `/${string}`>(
  source: ValueSource<P, A, R, RE>,
  path: Path & ExactPath<Path, NoInfer<P>>,
  options: DocumentOptions = {},
): RouteLayer<R | RE> {
  const selected = params(source.paramSchema, path);
  const cache = options.cache ?? { visibility: "private", maxAgeSeconds: 0, mustRevalidate: true };
  if (!Number.isFinite(cache.maxAgeSeconds) || cache.maxAgeSeconds < 0)
    throw new RangeError("Invalid cache age");
  const cacheControl = `${cache.visibility}, max-age=${cache.maxAgeSeconds}${cache.mustRevalidate ? ", must-revalidate" : ""}`;
  const fingerprint = Contract.contractFingerprint({
    kind: "document",
    source: source.id,
    route: path,
    cache: cacheControl,
    contract: options.contract ?? null,
  });
  const headers = { ...Http.securityHeaders, [Contract.DOCUMENT_CONTRACT_HEADER]: fingerprint };
  return HttpRouter.add<
    never,
    HttpServerRequest.HttpServerRequest | HttpRouter.RouteContext | R | RE
  >(
    "*",
    path,
    Effect.gen(function* (): Effect.fn.Return<
      HttpServerResponse.HttpServerResponse,
      | Extract<Contract.PublicError, { _tag: "InvalidParams" }>
      | ValueUnavailable
      | Schema.SchemaError
      | { readonly _tag: "WireEncodeFailed"; readonly route: string; readonly detail: string },
      HttpServerRequest.HttpServerRequest | HttpRouter.RouteContext | R | RE
    > {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.method !== "GET" && request.method !== "HEAD") return methodResponse();
      const decoded = yield* selected;
      if (
        request.headers[Contract.DOCUMENT_CONTRACT_HEADER] !== undefined &&
        request.headers[Contract.DOCUMENT_CONTRACT_HEADER] !== fingerprint
      )
        return errorResponse(
          { _tag: "ContractChanged", route: path, recovery: "refetch" },
          409,
          headers,
        );
      const value = yield* source.resolve(decoded);
      const encoded = yield* Schema.encodeEffect(source.schema)(value);
      const canonical = yield* Effect.try({
        try: () => Contract.canonicalJson(encoded),
        catch: () => ({
          _tag: "WireEncodeFailed" as const,
          route: path,
          detail: "Document is not finite JSON",
        }),
      });
      const etag = Contract.documentEtag(canonical);
      const responseHeaders = { ...headers, "cache-control": cacheControl, etag };
      const conditional = request.headers["if-none-match"]
        ?.split(",")
        .some((tag) => tag.trim() === "*" || tag.trim().replace(/^W\//, "") === etag);
      if (conditional) return HttpServerResponse.empty({ status: 304, headers: responseHeaders });
      return HttpServerResponse.text(request.method === "HEAD" ? "" : canonical, {
        headers: { ...responseHeaders, "content-type": "application/json" },
      });
    }).pipe(
      Effect.catchTags({
        InvalidParams: (error) => Effect.succeed(errorResponse(error, 400, headers)),
        ValueUnavailable: () =>
          Effect.succeed(
            errorResponse(
              { _tag: "DocumentUnavailable", route: path, detail: "Document unavailable" },
              503,
              headers,
            ),
          ),
        WireEncodeFailed: (error) => Effect.succeed(errorResponse(error, 500, headers)),
        SchemaError: () =>
          Effect.succeed(
            errorResponse(
              { _tag: "WireEncodeFailed", route: path, detail: "Document cannot be encoded" },
              500,
              headers,
            ),
          ),
      }),
    ),
  );
}
