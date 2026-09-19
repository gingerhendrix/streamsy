import * as Cloudflare from "alchemy/Cloudflare";
import type { HttpEffect } from "alchemy/Http";
import { Effect, Layer } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { layerProtocol } from "@streamsy/storage/durable-object";
import * as Host from "@streamsy/serve/alchemy";
import { objectHandlers } from "./runtime.ts";

/** Only the runtime phase reads raw storage. No stack is executed by this fixture. */
export class StreamsObject extends Cloudflare.DurableObject<StreamsObject>()(
  "Streams",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    return Effect.suspend(() =>
      objectHandlers(
        Layer.mergeAll(
          layerProtocol({ client: { storage: state.raw.storage } }),
          Layer.succeed(Host.ObjectOptions, { pathPrefix: "/streams" }),
          Host.alarmLayer(state.raw.storage),
        ),
      ),
    );
  }),
) {}

export const worker = Effect.gen(function* () {
  const objects = yield* StreamsObject;
  const routed: HttpEffect = Host.router({
    objects,
    pathPrefix: "/streams",
    placement: Host.Placement.byStream(),
  });
  return {
    fetch: Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      if (request.headers.authorization !== "Bearer fixture") {
        return HttpServerResponse.text("Unauthorized", { status: 401 });
      }
      return yield* routed;
    }),
  };
});
