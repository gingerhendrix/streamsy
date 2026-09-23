import * as Cloudflare from "alchemy/Cloudflare";
import { Effect, Layer } from "effect";
import { Http } from "@streamsy/core";
import * as Host from "@streamsy/serve/alchemy";
import { layerProtocol } from "@streamsy/storage/durable-object";
import {
  COMPATIBILITY_DATE,
  COMPATIBILITY_FLAGS,
  CONFORMANCE_LONG_POLL_TIMEOUT_MS,
} from "./contract.ts";

/** The runtime phase alone reads raw Durable Object storage. */
export class StreamsObject extends Cloudflare.DurableObject<StreamsObject>()(
  "Streams",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    return Effect.suspend(() =>
      Host.objectHandlers({
        app: Http.routes({ prefix: "/streams" }),
        layer: Layer.mergeAll(
          layerProtocol({
            client: { storage: state.raw.storage },
            longPollTimeoutMs: CONFORMANCE_LONG_POLL_TIMEOUT_MS,
          }),
          Host.alarmLayer(state.raw.storage),
        ),
      }),
    );
  }),
) {}

const worker = Effect.gen(function* () {
  const objects = yield* StreamsObject;
  return {
    fetch: Host.router({
      objects,
      prefix: "/streams",
      placement: Host.Placement.byKey(() => "conformance"),
    }),
  };
});

/** Effect-native source form selected by alchemy.source.run.ts. */
export const sourceWorker = (name: string) =>
  Cloudflare.Worker(
    "Server",
    {
      name,
      main: import.meta.url,
      compatibility: { date: COMPATIBILITY_DATE, flags: [...COMPATIBILITY_FLAGS] },
      workersDev: true,
    },
    worker,
  );
