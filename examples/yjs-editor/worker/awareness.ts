import { CreateResult, StreamId } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { Clock, Effect, Schema } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { YjsProtocolError } from "./protocol-error.ts";
import { binary, forward, requireDocument } from "./streams.ts";

const AwarenessEntry = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  createdAt: Schema.optionalKey(Schema.Finite),
});

const AwarenessEntryJson = Schema.fromJsonString(AwarenessEntry);

export const readAwarenessRegistry = Effect.fn("Yjs.readAwarenessRegistry")(function* (id: string) {
  const reader = yield* StreamsReader;

  const result = yield* reader
    .read(StreamId.make(id), { offset: "-1" })
    .pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

  if (result === undefined) return [];

  return yield* Effect.forEach(result.messages, (message) =>
    Schema.decodeEffect(AwarenessEntryJson)(new TextDecoder().decode(message.data)),
  );
});

export const createAwareness = Effect.fn("Yjs.createAwareness")(function* (
  id: string,
  name: string,
) {
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

const removeAwareness = Effect.fn("Yjs.removeAwareness")(function* (target: string) {
  const writer = yield* StreamsWriter;

  yield* writer.remove(StreamId.make(target)).pipe(
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

  return HttpServerResponse.empty({ status: 204 });
});

/** `?awareness=<name>` on a document: one TTL stream per name. */
export const awarenessRequest = Effect.fn("Yjs.awarenessRequest")(function* (
  request: HttpServerRequest.HttpServerRequest,
  url: URL,
  id: string,
  name: string,
) {
  if (!/^[a-zA-Z0-9_.-]{1,256}$/.test(name) || [".", "..", ".index"].includes(name))
    return yield* new YjsProtocolError({
      code: "INVALID_REQUEST",
      message: "Invalid awareness name",
      status: 400,
    });
  const target = `${id}/.awareness/${name}`;

  switch (request.method) {
    case "PUT":
      yield* requireDocument(id);

      return yield* createAwareness(id, name);
    case "POST":
      yield* requireDocument(id);
      // Create is idempotent, and it recreates a stream whose TTL expired.
      yield* createAwareness(id, name);

      return yield* forward(request, target, new URLSearchParams());
    case "DELETE":
      return yield* removeAwareness(target);
    default: {
      const query = new URLSearchParams(url.searchParams);
      query.delete("awareness");

      if (query.get("live") === "true") query.set("live", "sse");

      return yield* forward(request, target, query);
    }
  }
});
