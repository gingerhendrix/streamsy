import type { DurableObjectState } from "@cloudflare/workers-types";
import { CreateResult, Http, StreamId, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, ProjectionWakeup, Transaction } from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { Digest, SecureRandom } from "effect/crypto";
import { Array, Clock, ConfigProvider, Context, Effect, Layer, Schema, Semaphore } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import {
  compaction,
  CompactionThreshold,
  concat,
  readIndex,
  readToTail,
  snapshotStream,
  Updates,
} from "./compaction.ts";
import type { Env } from "./env.ts";
import { protocolErrorResponse, unavailable, YjsProtocolError } from "./protocol-error.ts";

const streams = Streamsy.group(Updates, Streamsy.Wildcard);

const binary = "application/octet-stream";

/** One update message must fit in a Durable Object SQLite value, which is limited to 2 MB. */
const maxMessageSize = 1.5 * 1024 * 1024;

/**
 * Total update bytes one document accepts. Compaction holds the whole document
 * in the object's memory; the perf run measured safe snapshots up to 11 MB.
 */
const maxDocumentBytes = 8 * 1024 * 1024;

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

// One permit covers Yjs mutations. Reads never take it.
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

const documentFull = () =>
  new YjsProtocolError({
    code: "DOCUMENT_TOO_LARGE",
    message: "Document reached its size limit",
    status: 413,
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

const backingStream = Http.app({ pathPrefix: "/v1/stream", maxMessageSize });

/** Producer headers the stock provider sends with each append. */
const producerHeaders = [
  "producer-id",
  "producer-epoch",
  "producer-seq",
  "stream-producer-id",
  "stream-producer-epoch",
  "stream-producer-seq",
];

/**
 * Run the base protocol program on one backing stream, passing the request's
 * unread body through. `Http.app` reads the stream id from the Web request's
 * URL, so the request is rebuilt rather than modified.
 */
const forward = Effect.fn("Yjs.forward")(function* (
  request: HttpServerRequest.HttpServerRequest,
  id: string,
  query: URLSearchParams,
) {
  const original = yield* HttpServerRequest.toWeb(request);
  const headers = new Headers(request.method === "POST" ? {} : original.headers);

  // The stock provider closes its producer on disconnect. An append keeps only
  // producer headers, so `Stream-Closed` never closes the shared document.
  if (request.method === "POST") {
    for (const name of producerHeaders) {
      const value = original.headers.get(name);

      if (value !== null) headers.set(name, value);
    }

    headers.set("content-type", binary);
  }

  const forwarded = new Request(`https://internal/v1/stream/${id}?${query.toString()}`, {
    method: request.method,
    headers,
    body: original.body,
    signal: original.signal,
  });

  return yield* backingStream.pipe(
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      HttpServerRequest.fromWeb(forwarded),
    ),
  );
});

/** One transaction, so the compaction alarm never sees a half-deleted document. */
const deleteDocument = Effect.fn("Yjs.deleteDocument")(function* (id: string) {
  const tx = yield* Transaction;

  return yield* tx.withTransaction(removeDocument(id));
});

const removeDocument = Effect.fn("Yjs.removeDocument")(function* (id: string) {
  const writer = yield* StreamsWriter;

  yield* writer
    .remove(StreamId.make(`${id}/.updates`))
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.fail(missingDocument())));

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

      // Create is idempotent, and it recreates a stream whose TTL expired.
      const created = yield* createAwareness(id, awareness);

      return method === "PUT" ? created : yield* forward(request, target, new URLSearchParams());
    }

    if (method === "DELETE") {
      const writer = yield* StreamsWriter;

      return yield* writer.remove(StreamId.make(target)).pipe(
        Effect.as(HttpServerResponse.empty({ status: 204 })),
        Effect.catchTag("StreamNotFound", () =>
          Effect.fail(
            new YjsProtocolError({
              code: "STREAM_NOT_FOUND",
              message: "Awareness stream not found",
              status: 404,
            }),
          ),
        ),
      );
    }

    const query = new URLSearchParams(url.searchParams);
    query.delete("awareness");

    if (query.get("live") === "true") query.set("live", "sse");

    return yield* forward(request, target, query);
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

    const parts: Array<Uint8Array> = [];

    yield* readToTail(StreamId.make(`${id}/.snapshots/${offset}`), "-1", (data) =>
      parts.push(data),
    ).pipe(Effect.catchTag("StreamNotFound", () => Effect.fail(snapshotNotFound())));

    const base = offset.slice(0, -"_snapshot".length);
    // Upstream uses the next sequence position; this still precedes the next Streamsy update.
    const next = `${base.slice(0, 17)}${String(BigInt(base.slice(17)) + 1n).padStart(16, "0")}`;

    return HttpServerResponse.uint8Array(concat(parts), {
      contentType: binary,
      headers: { "stream-next-offset": next },
    });
  }

  if (method === "DELETE") return yield* deleteDocument(id);

  if (method === "GET" || method === "HEAD") {
    // HEAD ignored the query in the original adapter.
    const query = method === "GET" ? new URLSearchParams(url.searchParams) : new URLSearchParams();

    if (query.get("live") === "true") query.set("live", "long-poll");

    return yield* forward(request, updates, query);
  }

  if (method === "PUT") {
    const writer = yield* StreamsWriter;
    const result = yield* writer.create(StreamId.make(updates), { contentType: binary });
    yield* createAwareness(id, "default");

    return HttpServerResponse.empty({ status: CreateResult.$is("Created")(result) ? 201 : 200 });
  }

  // The fold's byte count lags the log by the updates the alarm has not folded yet.
  const { state } = yield* Projection.read(compaction.at({ document: id }));

  if (state.bytes > maxDocumentBytes) return yield* documentFull();

  // The append's commit marks the compaction fold; the object alarm runs it.
  return yield* forward(request, updates, new URLSearchParams());
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
  app: Layer.merge(Http.routes(streams, { prefix: "/v1/stream", access: "read" }), yjsRoutes),
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
