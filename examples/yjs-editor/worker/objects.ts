import type { DurableObjectState } from "@cloudflare/workers-types";
import { CreateResult, Http, StreamId, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, ProjectionWakeup, Transaction } from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { Digest, SecureRandom } from "effect/crypto";
import { Array, Clock, ConfigProvider, Context, Effect, Layer, Schema, Semaphore } from "effect";
import {
  Headers as HttpHeaders,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";
import {
  compaction,
  CompactionThreshold,
  readIndex,
  snapshotStream,
  Updates,
} from "./compaction.ts";
import type { Env } from "./env.ts";
import { protocolErrorResponse, unavailable, YjsProtocolError } from "./protocol-error.ts";

const streams = Streamsy.group(Updates, Streamsy.Wildcard);

const binary = "application/octet-stream";

/** One update message must fit in a Durable Object SQLite value, which is limited to 2 MB. */
const maxMessageSize = 1.5 * 1024 * 1024;

class Mutations extends Context.Service<Mutations, Semaphore.Semaphore>()("yjs-editor/Mutations") {
  static readonly layer = Layer.effect(Mutations, Semaphore.make(1));
}

const storage = (state: DurableObjectState) =>
  Layer.mergeAll(
    CheckpointStore.layer,
    Transaction.layer,
    Layer.effect(
      StreamsReader,
      Effect.map(Streamsy.Wildcard, (service) => service.wire),
    ),
    Layer.effect(
      StreamsWriter,
      Effect.map(Streamsy.Wildcard, (service) => service.wire),
    ),
  ).pipe(
    Layer.provideMerge(DurableObjectStorage.layer(streams, { client: { storage: state.storage } })),
    Layer.provideMerge(Layer.merge(Digest.layerWebCrypto, SecureRandom.layerWebCrypto)),
  );

// One permit covers Yjs and raw backing-stream mutations. Reads never take it.
const serializeMutations = <A, E, R>(handler: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;

    if (["GET", "HEAD"].includes(request.method)) return yield* handler;
    const mutations = yield* Mutations;

    return yield* mutations.withPermit(handler);
  });

const missingDocument = () =>
  new YjsProtocolError({
    code: "DOCUMENT_NOT_FOUND",
    message: "Document does not exist",
    status: 404,
  });

const snapshotNotFound = () =>
  new YjsProtocolError({
    code: "SNAPSHOT_NOT_FOUND",
    message: "Snapshot not found",
    status: 404,
  });

const requireDocument = Effect.fn("Yjs.requireDocument")(function* (id: string) {
  const reader = yield* StreamsReader;

  // The original existence probe distinguished only a 404, not other base failures.
  const exists = yield* reader.head(StreamId.make(`${id}/.updates`)).pipe(
    Effect.as(true),
    Effect.catchTag("StreamNotFound", () => Effect.succeed(false)),
    Effect.orElseSucceed(() => true),
  );

  return yield* exists ? Effect.void : missingDocument();
});

const AwarenessEntry = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  createdAt: Schema.optionalKey(Schema.Finite),
});

const AwarenessEntryJson = Schema.fromJsonString(AwarenessEntry);

const readAwarenessRegistry = Effect.fn("Yjs.readAwarenessRegistry")(function* (id: string) {
  const reader = yield* StreamsReader;

  const result = yield* reader
    .read(StreamId.make(id), { offset: "-1" })
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

  if (result === undefined) return [];

  return yield* Effect.forEach(result.messages, (message) =>
    Schema.decodeEffect(AwarenessEntryJson)(new TextDecoder().decode(message.data)),
  );
});

const createAwareness = Effect.fn("Yjs.createAwareness")(function* (id: string, name: string) {
  const writer = yield* StreamsWriter;

  const result = yield* writer.create(StreamId.make(`${id}/.awareness/${name}`), {
    contentType: binary,
    ttlSeconds: 3600,
  });

  const created = CreateResult.$is("Created")(result);

  if (created && name !== "default") {
    const registry = StreamId.make(`${id}/.awareness-index`);
    yield* writer.create(registry, { contentType: "application/json" });
    const createdAt = yield* Clock.currentTimeMillis;

    const json = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(AwarenessEntry)))([
      { name, createdAt },
    ]);

    yield* writer.append(registry, {
      contentType: "application/json",
      data: new TextEncoder().encode(json),
    });
  }

  return HttpServerResponse.empty({ status: created ? 201 : 200 });
});

const backingStreamProgram = Http.app({ pathPrefix: "/v1/stream", maxMessageSize });

/** Reuse the base HTTP write program, including producer acknowledgements. No self-HTTP. */
const writeStream = Effect.fn("Yjs.writeStream")(function* (
  request: HttpServerRequest.HttpServerRequest,
  id: string,
  body: Uint8Array,
) {
  // The stock provider closes its producer on disconnect. Stream-Closed must not
  // close the shared document; match the reference server's POST header policy.
  const headers = request.method === "POST" ? new Headers() : new Headers(request.headers);

  if (request.method === "POST") {
    for (const name of [
      "producer-id",
      "producer-epoch",
      "producer-seq",
      "stream-producer-id",
      "stream-producer-epoch",
      "stream-producer-seq",
    ]) {
      const value = request.headers[name];

      if (value !== undefined) headers.set(name, value);
    }
  }

  headers.delete("host");

  if (request.method === "POST" || !headers.has("content-type"))
    headers.set("content-type", binary);
  const original = yield* HttpServerRequest.toWeb(request);

  const forwarded = HttpServerRequest.fromWeb(
    new Request(`https://internal/v1/stream/${id}`, {
      method: request.method,
      headers,
      body,
      signal: original.signal,
    }),
  );

  return yield* backingStreamProgram.pipe(
    Effect.provideService(HttpServerRequest.HttpServerRequest, forwarded),
  );
});

/** Native Response text errors use this charset on the existing stream wire. */
const streamFailure = (message: string, options: HttpServerResponse.Options.WithContentType) =>
  HttpServerResponse.text(message, { ...options, contentType: "text/plain;charset=UTF-8" });

/** Http.read supplies framing/live modes; keep the base protocol's read failures. */
const readStream = Effect.fn("Yjs.readStream")(function* (
  request: HttpServerRequest.HttpServerRequest,
  id: string,
  query: URLSearchParams,
) {
  const headers = request.method === "HEAD" ? { "cache-control": "no-store" } : {};

  return yield* Http.read(StreamId.make(id)).pipe(
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      request.modify({
        url: `/v1/stream/${id}?${query.toString()}`,
        headers: HttpHeaders.remove(request.headers, "host"),
      }),
    ),
    Effect.catchTags({
      StreamNotFound: () =>
        Effect.succeed(streamFailure("Stream not found", { status: 404, headers })),
      StreamGone: () =>
        Effect.succeed(streamFailure("Stream is soft-deleted", { status: 410, headers })),
      InvalidReadRequest: ({ message }) => Effect.succeed(streamFailure(message, { status: 400 })),
      NotSupported: ({ feature }) =>
        Effect.succeed(
          streamFailure(`Feature not supported: ${feature}`, {
            status: 400,
            headers: { "stream-not-supported": feature },
          }),
        ),
      StorageFault: () => Effect.succeed(streamFailure("Internal server error", { status: 500 })),
      TransportFault: () => Effect.succeed(streamFailure("Internal server error", { status: 500 })),
    }),
    Effect.map(HttpServerResponse.setHeaders(Http.securityHeaders)),
  );
});

/** Return the same base-protocol status/body as DELETE on a backing stream. */
const removeStream = Effect.fn("Yjs.removeStream")(function* (id: string) {
  const writer = yield* StreamsWriter;

  return yield* writer.remove(StreamId.make(id)).pipe(
    Effect.as(HttpServerResponse.empty({ status: 204 })),
    Effect.catchTags({
      StreamNotFound: () => Effect.succeed(streamFailure("Stream not found", { status: 404 })),
      StreamGone: () => Effect.succeed(streamFailure("Stream is soft-deleted", { status: 410 })),
      StreamBusy: () => Effect.succeed(streamFailure("Stream busy, retry later", { status: 503 })),
      StorageFault: () => Effect.succeed(streamFailure("Internal server error", { status: 500 })),
      TransportFault: () => Effect.succeed(streamFailure("Internal server error", { status: 500 })),
    }),
    Effect.map(HttpServerResponse.setHeaders(Http.securityHeaders)),
  );
});

/** One transaction, so the compaction alarm never sees a half-deleted document. */
const deleteDocument = Effect.fn("Yjs.deleteDocument")(function* (id: string) {
  const tx = yield* Transaction;

  return yield* tx.withTransaction(removeDocument(id));
});

const removeDocument = Effect.fn("Yjs.removeDocument")(function* (id: string) {
  const writer = yield* StreamsWriter;
  const deleted = yield* removeStream(`${id}/.updates`);

  if (deleted.status === 404) return yield* missingDocument();

  if (deleted.status < 200 || deleted.status >= 300) return deleted;

  const paths = new Set([
    `${id}/.index`,
    `${id}/.awareness/default`,
    `${id}/.awareness-index`,
    `${id}/.awareness/.index`,
  ]);

  for (const entry of yield* readIndex(id)) paths.add(snapshotStream(id, entry.snapshotOffset));

  for (const registry of [".awareness-index", ".awareness/.index"])
    for (const entry of yield* readAwarenessRegistry(`${id}/${registry}`))
      if (entry.name) paths.add(`${id}/.awareness/${entry.name}`);

  for (const path of paths)
    yield* writer
      .remove(StreamId.make(path))
      .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));
  yield* Projection.forget(compaction.at({ document: id }));

  return HttpServerResponse.empty({ status: 204 });
});

const documentRequest = Effect.fn("Yjs.documentRequest")(function* (
  request: HttpServerRequest.HttpServerRequest,
  url: URL,
  route: DocumentRoute,
) {
  const id = documentId(route);
  const updates = `${id}/.updates`;
  const method = request.method;

  if (!["GET", "HEAD", "PUT", "POST", "DELETE"].includes(method))
    return yield* new YjsProtocolError({
      code: "INVALID_REQUEST",
      message: "Method not allowed",
      status: 405,
    });

  // Buffer once so an expired awareness stream can be recreated and retried.
  const body = ["PUT", "POST"].includes(method)
    ? new Uint8Array(yield* request.arrayBuffer)
    : new Uint8Array();

  const awareness = url.searchParams.get("awareness");

  if (awareness !== null) {
    if (!/^[a-zA-Z0-9_.-]{1,256}$/.test(awareness) || [".", "..", ".index"].includes(awareness))
      return yield* new YjsProtocolError({
        code: "INVALID_REQUEST",
        message: "Invalid awareness name",
        status: 400,
      });
    const target = `${id}/.awareness/${awareness}`;

    if (method === "PUT" || method === "POST") {
      yield* requireDocument(id);

      if (method === "PUT") return yield* createAwareness(id, awareness);
      let result = yield* writeStream(request, target, body);

      if (result.status === 404) {
        yield* createAwareness(id, awareness);
        result = yield* writeStream(request, target, body);
      }

      return result;
    }

    if (method === "DELETE") {
      const removed = yield* removeStream(target);

      if (removed.status === 404)
        return yield* new YjsProtocolError({
          code: "STREAM_NOT_FOUND",
          message: "Awareness stream not found",
          status: 404,
        });

      return removed;
    }

    const query = new URLSearchParams(url.searchParams);
    query.delete("awareness");

    if (query.get("live") === "true") query.set("live", "sse");

    return yield* readStream(request, target, query);
  }

  const offset = url.searchParams.get("offset");

  if (method === "GET" && offset === "snapshot") {
    yield* requireDocument(id);

    // Discovery only reads the index. A missing or unreadable index means "replay from -1".
    const latest = yield* readIndex(id).pipe(
      Effect.map((entries) => entries.at(-1)?.snapshotOffset),
      Effect.catch((error) =>
        Effect.logWarning("Yjs snapshot index unreadable", error).pipe(Effect.as(undefined)),
      ),
    );

    url.searchParams.set("offset", latest ? `${latest}_snapshot` : "-1");

    return HttpServerResponse.empty({
      status: 307,
      headers: {
        location: `${url.pathname}${url.search}`,
        "cache-control": "private, max-age=5",
      },
    });
  }

  if (method === "GET" && offset?.endsWith("_snapshot")) {
    if (!/^\d{16}_\d{16}_snapshot$/.test(offset)) return yield* snapshotNotFound();

    // Snapshot reads ignore client conditionals and live/query options, as before.
    const result = yield* readStream(
      HttpServerRequest.fromWeb(new Request("https://internal")),
      `${id}/.snapshots/${offset}`,
      new URLSearchParams({ offset: "-1" }),
    );

    if (result.status === 404) return yield* snapshotNotFound();

    if (result.status < 200 || result.status >= 300) return result;

    const base = offset.slice(0, -"_snapshot".length);
    // Upstream uses the next sequence position; this still precedes the next Streamsy update.
    const next = `${base.slice(0, 17)}${String(BigInt(base.slice(17)) + 1n).padStart(16, "0")}`;

    return HttpServerResponse.raw(HttpServerResponse.toWeb(result).body, {
      status: 200,
      headers: { "content-type": binary, "stream-next-offset": next },
    });
  }

  if (method === "DELETE") return yield* deleteDocument(id);

  if (method === "GET" || method === "HEAD") {
    // HEAD ignored the query in the original adapter.
    const query = method === "GET" ? new URLSearchParams(url.searchParams) : new URLSearchParams();

    if (query.get("live") === "true") query.set("live", "long-poll");

    return yield* readStream(request, updates, query);
  }

  // The append's commit marks the compaction fold; the object alarm runs it.
  const result = yield* writeStream(request, updates, body);

  if (method === "PUT" && result.status >= 200 && result.status < 300)
    yield* createAwareness(id, "default");

  return result;
});

const yjsRoutes = HttpRouter.add(
  "*",
  "/v1/yjs/*",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = new URL(request.url, "https://internal");
    const route = parseDocumentRoute(url.pathname);

    if (route === null)
      return yield* new YjsProtocolError({
        code: "INVALID_REQUEST",
        message: "Invalid document path",
        status: 400,
      });

    return yield* documentRequest(request, url, route);
  }).pipe(
    serializeMutations,
    Effect.catchTag("YjsProtocolError", (error) => Effect.succeed(protocolErrorResponse(error))),
    Effect.catch((error) =>
      Effect.logError("Yjs request failed", error).pipe(
        Effect.as(protocolErrorResponse(unavailable())),
      ),
    ),
    Effect.catchDefect((defect) =>
      Effect.logError("Yjs request failed", defect).pipe(
        Effect.as(protocolErrorResponse(unavailable())),
      ),
    ),
  ),
);

/**
 * One object owns every stream and checkpoint belonging to a document. Each
 * commit to `.updates` writes a pending mark for the compaction fold in the same
 * transaction; the object's alarm then runs the fold, so requests never compact.
 */
export class DocumentObject extends StreamsyObject.make({
  app: Layer.merge(
    HttpRouter.add("*", "/v1/stream/*", backingStreamProgram.pipe(serializeMutations)),
    yjsRoutes,
  ),
  wakeups: [ProjectionWakeup.attach(compaction)],
  layer: (state: DurableObjectState, env: Env) =>
    Layer.mergeAll(storage(state), Mutations.layer, CompactionThreshold.layer).pipe(
      Layer.provideMerge(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            COMPACTION_THRESHOLD: env.COMPACTION_THRESHOLD || undefined,
          }),
        ),
      ),
    ),
}) {}

export interface DocumentRoute {
  readonly service: string;
  readonly docPath: string;
}

export const documentId = (route: DocumentRoute) => `yjs/${route.service}/docs/${route.docPath}`;

/** Match the pinned upstream path grammar, including encoded traversal rejection. */
export const parseDocumentRoute = (pathname: string): DocumentRoute | null => {
  const match = /^\/v1\/yjs\/([^/]+)\/docs\/(.+)$/.exec(pathname);

  if (!match?.[1] || !match[2]) return null;
  let path: string;

  try {
    path = decodeURIComponent(match[2]).replace(/\/+/g, "/");
  } catch {
    return null;
  }

  if (
    !/^[a-zA-Z0-9_\-/]+$/.test(path) ||
    path.length > 256 ||
    Array.some(path.split("/"), (segment) => segment === "." || segment === "..")
  )
    return null;

  return { service: match[1], docPath: path };
};
