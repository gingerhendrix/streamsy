import { Effect, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { Http, Offset, type StreamAddress, type Streamsy } from "@streamsy/core";
import { StreamsReader, type StreamRoute } from "@streamsy/core/protocol";
import * as Contract from "./contract.ts";
import {
  InvalidParams,
  ProtocolVersionUnsupported,
  ResumeRejected,
  ContractChanged,
  TransportUnavailable,
  WireEncodeFailed,
  withHeaders,
} from "./errors.ts";

export {
  InvalidParams,
  ProtocolVersionUnsupported,
  ResumeRejected,
  ContractChanged,
  TransportUnavailable,
  WireEncodeFailed,
  type ServeError,
} from "./errors.ts";

export { layer, teardown, type LayerOptions } from "./host.ts";

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

/** Decode the matched route's parameters with the stream's codecs. */
export const params = Effect.fn("Serve.params")(function* <P>(stream: {
  readonly paramSchema: StreamRoute.PathSchema<P>;
}) {
  const { route } = yield* HttpRouter.RouteContext;

  return yield* HttpRouter.schemaPathParams(stream.paramSchema).pipe(
    Effect.mapError(
      () =>
        new InvalidParams({
          route: route.path,
          parameter: "params",
          detail: "Invalid path parameters",
        }),
    ),
  );
});

/** Own every method at this path, including beneath protocol wildcard routes. */
export function route<E, R>(
  path: HttpRouter.PathInput,
  handler: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
) {
  return HttpRouter.add(
    "*",
    path,
    Effect.flatMap(HttpServerRequest.HttpServerRequest, (request) =>
      request.method === "GET" || request.method === "HEAD"
        ? handler
        : Effect.succeed(
            HttpServerResponse.empty({
              status: 405,
              headers: { ...Http.securityHeaders, allow: "GET, HEAD" },
            }),
          ),
    ),
  );
}

/** Mount one declared stream; state addresses select the Durable State contract. */
export function mount<P, A, K, RD, RE, const Path extends `/${string}`>(
  declaration: {
    readonly paramSchema: StreamRoute.PathSchema<P>;
    readonly at: (params: P) => Streamsy.Address<A, K, RD, RE>;
  },
  path: Path & ExactPath<Path, NoInfer<P>>,
  options?: ReadOptions,
) {
  return route(
    path,
    Effect.gen(function* () {
      const p = yield* params(declaration);

      const address = yield* Effect.try({
        try: () => declaration.at(p),
        catch: () =>
          new InvalidParams({
            route: path,
            parameter: "params",
            detail: "Parameters cannot form a canonical stream id",
          }),
      });

      return yield* read(
        address,
        options ?? {},
        "collections" in address ? "state" : "stream",
        "collections" in address ? collectionDescriptor(address.collections) : Effect.succeed(null),
      );
    }),
  );
}

/** Durable Streams response for one resolved address. */
export function stream<A, K, RD, RE>(
  address: Streamsy.Address<A, K, RD, RE>,
  options: ReadOptions = {},
) {
  return read(address, options, "stream", Effect.succeed(null));
}

/** Durable State change response with replay-from-start recovery. */
export function state<C extends StreamAddress.Collections, K, RD, RE>(
  address: StreamAddress.StateAddress<C, RD, RE> &
    Streamsy.Address<StreamAddress.CollectionsChange<C>, K, RD, RE>,
  options: ReadOptions = {},
) {
  return read(address, options, "state", collectionDescriptor(address.collections));
}

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- Collection keys are decoded at this metadata boundary.
function collectionDescriptor(value: unknown) {
  return Effect.gen(function* () {
    const { route } = yield* HttpRouter.RouteContext;

    const collections = yield* Schema.decodeUnknownEffect(
      Schema.Record(
        Schema.String,
        Schema.Struct({ key: Schema.Union([Schema.String, Schema.Finite]) }),
      ),
    )(value).pipe(
      Effect.mapError(
        () => new WireEncodeFailed({ route: route.path, detail: "Invalid collection metadata" }),
      ),
    );

    return Object.fromEntries(
      Object.entries(collections).map(([type, collection]) => [type, collection.key]),
    );
  });
}

const read = Effect.fn("Serve.read")(function* <A, K, RD, RE>(
  address: Streamsy.Address<A, K, RD, RE>,
  options: ReadOptions,
  kind: "stream" | "state",
  descriptor: Effect.Effect<Contract.CanonicalValue, WireEncodeFailed, HttpRouter.RouteContext>,
) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const { route } = yield* HttpRouter.RouteContext;
  const path = route.path;

  const versionHeader =
    kind === "state" ? Contract.STATE_VERSION_HEADER : Contract.STREAM_VERSION_HEADER;

  const contractHeader =
    kind === "state" ? Contract.STATE_CONTRACT_HEADER : Contract.STREAM_CONTRACT_HEADER;

  const source =
    "template" in address.stream && Schema.is(Schema.String)(address.stream.template)
      ? address.stream.template
      : "";

  const collections = yield* descriptor;

  const fingerprint = Contract.contractFingerprint({
    kind,
    route: path,
    source,
    id: address.id,
    collections,
    params: path
      .split("/")
      .filter((segment) => segment.startsWith(":"))
      .map((segment) => segment.slice(1))
      .toSorted(),
    version: 1,
    recovery: "replay-from-start",
    contract: options.contract ?? null,
  });

  const headers = { [versionHeader]: "1", [contractHeader]: fingerprint };

  const reject = (reason: Contract.ResumeRejectedReason) =>
    Effect.fail(
      withHeaders(
        new ResumeRejected({ route: path, reason, recovery: "replay-from-start" }),
        headers,
      ),
    );

  const received = request.headers[versionHeader] ?? "1";

  if (received !== "1")
    return yield* withHeaders(
      new ProtocolVersionUnsupported({
        route: path,
        supported: 1,
        received,
        recovery: "replay-from-start",
      }),
      headers,
    );

  if (
    request.headers[contractHeader] !== undefined &&
    request.headers[contractHeader] !== fingerprint
  )
    return yield* reject("contract-changed");

  if (kind === "state" && request.headers["x-streamsy-state-reset"] === "snapshot")
    return HttpServerResponse.text("Snapshot reset is unsupported; replay from -1", {
      status: 400,
      headers: { ...headers, ...Http.securityHeaders },
    });
  const offset = new URL(request.url, "http://streamsy.internal").searchParams.get("offset");

  const resume =
    offset !== null && offset !== "-1" && offset !== "now" && request.method !== "HEAD";

  if (resume && !Schema.is(Offset)(offset)) return yield* reject("invalid-offset");
  const service = yield* address.stream;

  return yield* Http.read(address.id, options).pipe(
    Effect.provideService(StreamsReader, service.wire),
    Effect.map(HttpServerResponse.setHeaders(headers)),
    Effect.catchTags({
      StreamNotFound: () =>
        resume
          ? reject("history-unavailable")
          : Effect.succeed(
              HttpServerResponse.empty({
                status: 404,
                headers: { ...headers, ...Http.securityHeaders },
              }),
            ),
      StreamGone: () =>
        resume
          ? reject("history-unavailable")
          : Effect.succeed(
              HttpServerResponse.empty({
                status: 410,
                headers: { ...headers, ...Http.securityHeaders },
              }),
            ),
      InvalidReadRequest: () => reject("invalid-offset"),
      NotSupported: () =>
        Effect.succeed(
          HttpServerResponse.empty({
            status: 400,
            headers: { ...headers, ...Http.securityHeaders },
          }),
        ),
      StorageFault: () =>
        Effect.fail(
          withHeaders(
            new TransportUnavailable({ route: path, detail: "Storage unavailable" }),
            headers,
          ),
        ),
      TransportFault: () =>
        Effect.fail(
          withHeaders(
            new TransportUnavailable({ route: path, detail: "Transport unavailable" }),
            headers,
          ),
        ),
    }),
  );
});

export interface ValueOptions {
  readonly cache?: {
    readonly visibility: "private" | "public";
    readonly maxAgeSeconds: number;
    readonly mustRevalidate: boolean;
  };
  readonly contract?: Contract.CanonicalValue;
}

/** Canonical JSON response for any computation, including mapped projection state. */
export const value = Effect.fn("Serve.value")(function* <
  A,
  I extends Contract.CanonicalValue,
  E,
  R,
  RE,
>(
  schema: Schema.Codec<A, I, never, RE>,
  compute: Effect.Effect<A, E, R>,
  options: ValueOptions = {},
) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const { route } = yield* HttpRouter.RouteContext;
  const path = route.path;
  const cache = options.cache ?? { visibility: "private", maxAgeSeconds: 0, mustRevalidate: true };
  const cacheControl = `${cache.visibility}, max-age=${cache.maxAgeSeconds}${cache.mustRevalidate ? ", must-revalidate" : ""}`;

  const fingerprint = Contract.contractFingerprint({
    kind: "value",
    route: path,
    cache: cacheControl,
    contract: options.contract ?? null,
  });

  const headers = { ...Http.securityHeaders, [Contract.VALUE_CONTRACT_HEADER]: fingerprint };

  if (
    request.headers[Contract.VALUE_CONTRACT_HEADER] !== undefined &&
    request.headers[Contract.VALUE_CONTRACT_HEADER] !== fingerprint
  )
    return yield* withHeaders(new ContractChanged({ route: path, recovery: "refetch" }), headers);
  const computed = yield* compute;

  const encoded = yield* Schema.encodeEffect(schema)(computed).pipe(
    Effect.mapError(() =>
      withHeaders(
        new WireEncodeFailed({ route: path, detail: "Value cannot be encoded" }),
        headers,
      ),
    ),
  );

  const canonical = yield* Effect.try({
    try: () => Contract.canonicalJson(encoded),
    catch: () =>
      withHeaders(
        new WireEncodeFailed({ route: path, detail: "Value is not finite JSON" }),
        headers,
      ),
  });

  const etag = Contract.valueEtag(canonical);
  const responseHeaders = { ...headers, "cache-control": cacheControl, etag };

  const conditional = request.headers["if-none-match"]
    ?.split(",")
    .some((tag) => tag.trim() === "*" || tag.trim().replace(/^W\//, "") === etag);

  if (conditional) return HttpServerResponse.empty({ status: 304, headers: responseHeaders });

  return HttpServerResponse.text(request.method === "HEAD" ? "" : canonical, {
    headers: { ...responseHeaders, "content-type": "application/json" },
  });
});
