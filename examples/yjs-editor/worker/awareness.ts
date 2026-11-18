import { CreateResult } from "@streamsy/core";
import { Clock, Effect, Option } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { YjsProtocolError } from "./protocol-error.ts";
import { Awareness, AwarenessIndex, forward, requireDocument } from "./streams.ts";

/** Presence is ephemeral: an awareness stream nobody writes to expires after an hour. */
const awarenessTtlSeconds = 3600;

export const createAwareness = Effect.fn("Yjs.createAwareness")(function* (
  document: string,
  name: string,
) {
  const awareness = yield* Awareness;
  const result = yield* awareness.create({ document, name }, { ttlSeconds: awarenessTtlSeconds });
  const created = CreateResult.$is("Created")(result);

  if (created && name !== "default") {
    const index = yield* AwarenessIndex;
    yield* index.create({ document });
    yield* index.append({ document }, [{ name, createdAt: yield* Clock.currentTimeMillis }]);
  }

  return HttpServerResponse.empty({ status: created ? 201 : 200 });
});

const removeAwareness = Effect.fn("Yjs.removeAwareness")(function* (
  document: string,
  name: string,
) {
  const awareness = yield* Awareness;

  yield* awareness.remove({ document, name }).pipe(
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
  document: string,
  name: string,
) {
  if (Option.isNone(Awareness.parse(`${document}/.awareness/${name}`)))
    return yield* new YjsProtocolError({
      code: "INVALID_REQUEST",
      message: "Invalid awareness name",
      status: 400,
    });

  switch (request.method) {
    case "PUT":
      yield* requireDocument(document);

      return yield* createAwareness(document, name);
    case "POST":
      yield* requireDocument(document);
      // Create is idempotent, and it recreates a stream whose TTL expired.
      yield* createAwareness(document, name);

      return yield* forward(request, Awareness.at({ document, name }), new URLSearchParams());
    case "DELETE":
      return yield* removeAwareness(document, name);
    default: {
      const query = new URLSearchParams(url.searchParams);
      query.delete("awareness");

      if (query.get("live") === "true") query.set("live", "sse");

      return yield* forward(request, Awareness.at({ document, name }), query);
    }
  }
});
