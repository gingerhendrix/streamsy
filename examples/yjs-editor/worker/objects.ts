import type { DurableObjectState } from "@cloudflare/workers-types";
import { Http, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, ProjectionWakeup, Transaction } from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { Digest, SecureRandom } from "effect/crypto";
import { ConfigProvider, Effect, Layer } from "effect";
import { HttpRouter, HttpServerRequest } from "effect/http";
import { compaction, CompactionThreshold, Updates } from "./compaction.ts";
import { documentRequest } from "./document.ts";
import type { Env } from "./env.ts";
import { protocolErrorResponse, unexpected, YjsProtocolError } from "./protocol-error.ts";
import { parseDocumentRoute } from "./route.ts";

const streams = Streamsy.group(Updates, Streamsy.Wildcard);

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
    Effect.catchTag("YjsProtocolError", (error) => Effect.succeed(protocolErrorResponse(error))),
    Effect.catch((error) =>
      Effect.logError("Yjs request failed", error).pipe(
        Effect.as(protocolErrorResponse(unexpected(error))),
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
    Layer.merge(storage(state), CompactionThreshold.layer).pipe(
      Layer.provideMerge(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            COMPACTION_THRESHOLD: env.COMPACTION_THRESHOLD || undefined,
          }),
        ),
      ),
    ),
}) {}
