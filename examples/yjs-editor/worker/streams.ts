import { Http, StreamAddress, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { Effect, Option, Schema } from "effect";
import { HttpServerRequest } from "effect/http";
import { YjsProtocolError } from "./protocol-error.ts";

/*
 * Every stream of a document is `yjs/<service>/docs/<path>/<suffix>`. A document
 * path holds no `.`, so the suffix after it is unambiguous.
 */

/** Each document's update log: lib0 frames of Yjs updates. */
export class Updates extends Streamsy.custom<Updates>()("YjsEditor/Updates", {
  parse: (id: string) => {
    const match = /^(yjs\/[^/]+\/docs\/[\w/-]+)\/\.updates$/.exec(id);

    return match?.[1] === undefined ? Option.none() : Option.some({ document: match[1] });
  },
  address: ({ document }: { readonly document: string }) =>
    StreamAddress.bytes(`${document}/.updates`),
}) {}

/** The merged document at an `.updates` offset, as 1 MiB parts of one Yjs update. */
export class Snapshots extends Streamsy.custom<Snapshots>()("YjsEditor/Snapshots", {
  parse: (id: string) => {
    const match = /^(yjs\/[^/]+\/docs\/[\w/-]+)\/\.snapshots\/(\d{16}_\d{16})_snapshot$/.exec(id);

    return match?.[1] === undefined || match[2] === undefined
      ? Option.none()
      : Option.some({ document: match[1], offset: match[2] });
  },
  address: ({ document, offset }: { readonly document: string; readonly offset: string }) =>
    StreamAddress.bytes(`${document}/.snapshots/${offset}_snapshot`),
}) {}

/** One TTL stream of lib0-framed awareness updates per name. */
export class Awareness extends Streamsy.custom<Awareness>()("YjsEditor/Awareness", {
  parse: (id: string) => {
    const match = /^(yjs\/[^/]+\/docs\/[\w/-]+)\/\.awareness\/((?!\.\.?$)[\w.-]{1,256})$/.exec(id);

    return match?.[1] === undefined || match[2] === undefined
      ? Option.none()
      : Option.some({ document: match[1], name: match[2] });
  },
  address: ({ document, name }: { readonly document: string; readonly name: string }) =>
    StreamAddress.bytes(`${document}/.awareness/${name}`),
}) {}

const AwarenessEntry = Schema.Struct({ name: Schema.String, createdAt: Schema.Finite });

/** Named awareness streams, so deleting a document can find them. */
export class AwarenessIndex extends Streamsy.custom<AwarenessIndex>()("YjsEditor/AwarenessIndex", {
  parse: (id: string) => {
    const match = /^(yjs\/[^/]+\/docs\/[\w/-]+)\/\.awareness-index$/.exec(id);

    return match?.[1] === undefined ? Option.none() : Option.some({ document: match[1] });
  },
  address: ({ document }: { readonly document: string }) =>
    StreamAddress.json(`${document}/.awareness-index`, { schema: AwarenessEntry }),
}) {}

export const streams = Streamsy.group(Updates, Snapshots, Awareness, AwarenessIndex);

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
 * Run the base protocol program on one stream, passing the request's unread
 * body through. `Http.app` reads the stream id from the Web request's URL, so
 * the request is rebuilt rather than modified.
 */
export const forward = Effect.fn("Yjs.forward")(function* <A, R>(
  request: HttpServerRequest.HttpServerRequest,
  address: Streamsy.Address<A, R>,
  query: URLSearchParams,
) {
  const { wire } = yield* address.stream;
  const original = yield* HttpServerRequest.toWeb(request);
  const headers = new Headers(request.method === "POST" ? {} : original.headers);

  // The stock provider closes its producer on disconnect. An append keeps only
  // producer headers, so `Stream-Closed` never closes the shared document.
  if (request.method === "POST") {
    for (const name of producerHeaders) {
      const value = original.headers.get(name);

      if (value !== null) headers.set(name, value);
    }

    headers.set("content-type", address.contentType);
  }

  const forwarded = new Request(`https://internal/v1/stream/${address.id}?${query.toString()}`, {
    method: request.method,
    headers,
    body: original.body,
    signal: original.signal,
  });

  return yield* backingStream.pipe(
    Effect.provideService(StreamsReader, wire),
    Effect.provideService(StreamsWriter, wire),
    Effect.provideService(
      HttpServerRequest.HttpServerRequest,
      HttpServerRequest.fromWeb(forwarded),
    ),
  );
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

export const requireDocument = Effect.fn("Yjs.requireDocument")(function* (document: string) {
  const updates = yield* Updates;

  yield* updates.head({ document }).pipe(
    Effect.catchTags({
      StreamNotFound: () => Effect.fail(missingDocument()),
      StreamGone: () => Effect.fail(missingDocument()),
    }),
  );
});
