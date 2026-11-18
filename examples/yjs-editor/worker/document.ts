import { CreateResult, StreamId } from "@streamsy/core";
import { StreamsWriter } from "@streamsy/core/protocol";
import { Projection, Transaction } from "@streamsy/projection";
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { awarenessRequest, createAwareness, readAwarenessRegistry } from "./awareness.ts";
import { compaction, snapshotStream } from "./compaction.ts";
import { YjsProtocolError } from "./protocol-error.ts";
import { documentId, type DocumentRoute } from "./route.ts";
import {
  binary,
  concat,
  forward,
  missingDocument,
  readToTail,
  requireDocument,
} from "./streams.ts";

/**
 * Total update bytes one document accepts. Compaction holds the whole document
 * in the object's memory; the perf run measured safe snapshots up to 11 MB.
 */
const maxDocumentBytes = 8 * 1024 * 1024;

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

/** `GET ?offset=snapshot` redirects to the latest snapshot, or to `-1` before the first. */
const snapshotDiscovery = Effect.fn("Yjs.snapshotDiscovery")(function* (url: URL, id: string) {
  yield* requireDocument(id);

  // A pending snapshot is not in the fold state yet, so discovery returns the previous one.
  const { state } = yield* Projection.read(compaction.at({ document: id }));

  url.searchParams.set("offset", state.snapshot ? `${state.snapshot}_snapshot` : "-1");

  return HttpServerResponse.empty({
    status: 307,
    headers: {
      location: `${url.pathname}${url.search}`,
      "cache-control": "private, max-age=5",
    },
  });
});

/** `GET ?offset=<offset>_snapshot` returns the snapshot as one Yjs update. */
const snapshotRead = Effect.fn("Yjs.snapshotRead")(function* (id: string, offset: string) {
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
});

const readUpdates = Effect.fn("Yjs.readUpdates")(function* (
  request: HttpServerRequest.HttpServerRequest,
  url: URL,
  id: string,
) {
  // HEAD reports the tail, so it takes no read query.
  const query =
    request.method === "GET" ? new URLSearchParams(url.searchParams) : new URLSearchParams();

  if (query.get("live") === "true") query.set("live", "long-poll");

  return yield* forward(request, `${id}/.updates`, query);
});

const createDocument = Effect.fn("Yjs.createDocument")(function* (id: string) {
  const writer = yield* StreamsWriter;
  const result = yield* writer.create(StreamId.make(`${id}/.updates`), { contentType: binary });
  yield* createAwareness(id, "default");

  return HttpServerResponse.empty({ status: CreateResult.$is("Created")(result) ? 201 : 200 });
});

const appendUpdate = Effect.fn("Yjs.appendUpdate")(function* (
  request: HttpServerRequest.HttpServerRequest,
  id: string,
) {
  // The fold's byte count lags the log by the updates the alarm has not folded yet.
  const { state } = yield* Projection.read(compaction.at({ document: id }));

  if (state.bytes > maxDocumentBytes) return yield* documentFull();

  // The append's commit marks the compaction fold; the object alarm runs it.
  return yield* forward(request, `${id}/.updates`, new URLSearchParams());
});

const removeDocument = Effect.fn("Yjs.removeDocument")(function* (id: string) {
  const writer = yield* StreamsWriter;

  yield* writer
    .remove(StreamId.make(`${id}/.updates`))
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.fail(missingDocument())));

  const { state } = yield* Projection.read(compaction.at({ document: id }));
  const paths = new Set([`${id}/.awareness/default`, `${id}/.awareness-index`]);

  if (state.snapshot !== undefined) paths.add(snapshotStream(id, state.snapshot));

  for (const entry of yield* readAwarenessRegistry(`${id}/.awareness-index`))
    if (entry.name) paths.add(`${id}/.awareness/${entry.name}`);

  for (const path of paths)
    yield* writer
      .remove(StreamId.make(path))
      .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));
  yield* Projection.forget(compaction.at({ document: id }));

  return HttpServerResponse.empty({ status: 204 });
});

/** One transaction, so the compaction alarm never sees a half-deleted document. */
const deleteDocument = Effect.fn("Yjs.deleteDocument")(function* (id: string) {
  const tx = yield* Transaction;

  return yield* tx.withTransaction(removeDocument(id));
});

/** The router cannot dispatch on a query string, so one dispatcher does. */
export const documentRequest = Effect.fn("Yjs.documentRequest")(function* (
  request: HttpServerRequest.HttpServerRequest,
  url: URL,
  route: DocumentRoute,
) {
  const id = documentId(route);
  const awareness = url.searchParams.get("awareness");
  const offset = url.searchParams.get("offset");

  if (!["GET", "HEAD", "PUT", "POST", "DELETE"].includes(request.method))
    return yield* new YjsProtocolError({
      code: "INVALID_REQUEST",
      message: "Method not allowed",
      status: 405,
    });

  if (awareness !== null) return yield* awarenessRequest(request, url, id, awareness);

  switch (request.method) {
    case "PUT":
      return yield* createDocument(id);
    case "POST":
      return yield* appendUpdate(request, id);
    case "DELETE":
      return yield* deleteDocument(id);
    default:
      if (request.method === "GET" && offset === "snapshot")
        return yield* snapshotDiscovery(url, id);

      if (request.method === "GET" && offset?.endsWith("_snapshot"))
        return yield* snapshotRead(id, offset);

      return yield* readUpdates(request, url, id);
  }
});
