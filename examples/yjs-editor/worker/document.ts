import { CreateResult } from "@streamsy/core";
import { Projection, Transaction } from "@streamsy/projection";
import { Array, Effect, Option } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { awarenessRequest, createAwareness } from "./awareness.ts";
import { compaction } from "./compaction.ts";
import { YjsProtocolError } from "./protocol-error.ts";
import { documentId, type DocumentRoute } from "./route.ts";
import {
  Awareness,
  AwarenessIndex,
  concat,
  forward,
  missingDocument,
  requireDocument,
  Snapshots,
  Updates,
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
const snapshotRead = Effect.fn("Yjs.snapshotRead")(function* (document: string, offset: string) {
  const params = Snapshots.parse(`${document}/.snapshots/${offset}`);

  if (Option.isNone(params)) return yield* snapshotNotFound();
  const snapshots = yield* Snapshots;

  const { items } = yield* snapshots
    .collect(params.value)
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.fail(snapshotNotFound())));

  // Upstream uses the next sequence position; this still precedes the next Streamsy update.
  const base = params.value.offset;
  const next = `${base.slice(0, 17)}${String(BigInt(base.slice(17)) + 1n).padStart(16, "0")}`;

  return HttpServerResponse.uint8Array(concat(items), {
    contentType: "application/octet-stream",
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

  return yield* forward(request, Updates.at({ document: id }), query);
});

const createDocument = Effect.fn("Yjs.createDocument")(function* (document: string) {
  const updates = yield* Updates;
  const result = yield* updates.create({ document });
  yield* createAwareness(document, "default");

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
  return yield* forward(request, Updates.at({ document: id }), new URLSearchParams());
});

const removeDocument = Effect.fn("Yjs.removeDocument")(function* (document: string) {
  const updates = yield* Updates;
  const snapshots = yield* Snapshots;
  const awareness = yield* Awareness;
  const index = yield* AwarenessIndex;

  yield* updates
    .remove({ document })
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.fail(missingDocument())));
  const { state } = yield* Projection.read(compaction.at({ document }));

  if (state.snapshot !== undefined)
    yield* snapshots
      .remove({ document, offset: state.snapshot })
      .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

  const named = yield* index
    .collect({ document })
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.succeed({ items: [] })));

  for (const name of ["default", ...Array.map(named.items, (entry) => entry.name)])
    yield* awareness
      .remove({ document, name })
      .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));
  yield* index.remove({ document }).pipe(Effect.catchTag("StreamNotFound", () => Effect.void));
  yield* Projection.forget(compaction.at({ document }));

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
