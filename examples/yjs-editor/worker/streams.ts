import { Http, StreamId } from "@streamsy/core";
import { StreamsReader } from "@streamsy/core/protocol";
import { Effect } from "effect";
import { HttpServerRequest } from "effect/http";
import { YjsProtocolError } from "./protocol-error.ts";

export const binary = "application/octet-stream";

/** One update message must fit in a Durable Object SQLite value, which is limited to 2 MB. */
export const maxMessageSize = 1.5 * 1024 * 1024;

/** Producer headers the stock provider sends with each append. */
const producerHeaders = [
  "producer-id",
  "producer-epoch",
  "producer-seq",
  "stream-producer-id",
  "stream-producer-epoch",
  "stream-producer-seq",
];

const backingStream = Http.app({ pathPrefix: "/v1/stream", maxMessageSize });

/**
 * Run the base protocol program on one backing stream, passing the request's
 * unread body through. `Http.app` reads the stream id from the Web request's
 * URL, so the request is rebuilt rather than modified.
 */
export const forward = Effect.fn("Yjs.forward")(function* (
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

/** Read every message of a stream from `offset` to its tail. Returns the tail cursor. */
export const readToTail = Effect.fn("Yjs.readToTail")(function* (
  id: StreamId,
  offset: string,
  each: (data: Uint8Array) => void,
) {
  const reader = yield* StreamsReader;
  let cursor = offset;

  while (true) {
    const page = yield* reader.read(id, { offset: cursor });

    for (const message of page.messages) each(message.data);
    const done = page.upToDate || page.nextOffset === cursor;
    cursor = page.nextOffset;

    if (done) return cursor;
  }
});

export const concat = (parts: ReadonlyArray<Uint8Array>) => {
  let length = 0;

  for (const part of parts) length += part.length;
  const out = new Uint8Array(length);
  let at = 0;

  for (const part of parts) {
    out.set(part, at);
    at += part.length;
  }

  return out;
};

export const missingDocument = () =>
  new YjsProtocolError({
    code: "DOCUMENT_NOT_FOUND",
    message: "Document does not exist",
    status: 404,
  });

export const requireDocument = Effect.fn("Yjs.requireDocument")(function* (id: string) {
  const reader = yield* StreamsReader;

  yield* reader.head(StreamId.make(`${id}/.updates`)).pipe(
    Effect.catchTags({
      StreamNotFound: () => Effect.fail(missingDocument()),
      StreamGone: () => Effect.fail(missingDocument()),
    }),
  );
});
