/**
 * Node entry: `node src/hosts/node.ts`. `NODE_REQUEST_TIMEOUT_MS` sets the
 * `node:http` `requestTimeout` and a short connection check, so a test can
 * show that the bound does not cut a parked response.
 */
import { NodeRuntime } from "@effect/platform-node";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/node";
import { Config, Effect, Layer, Option } from "effect";
import { hostLayer } from "./sqlite-host.ts";

const RequestTimeout = Config.Int("NODE_REQUEST_TIMEOUT_MS").pipe(Config.option);

const Host = Layer.unwrap(
  Effect.map(RequestTimeout, (timeout) =>
    hostLayer({
      listener: ({ port }) =>
        listener({
          port,
          ...Option.match(timeout, {
            onNone: () => ({}),
            onSome: (requestTimeout) => ({
              server: { requestTimeout, connectionsCheckingInterval: 200 },
            }),
          }),
        }),
    }),
  ),
);

NodeRuntime.runMain(Layer.launch(Host), { teardown: Serve.teardown });
