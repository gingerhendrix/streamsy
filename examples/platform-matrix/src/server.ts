/**
 * The server Layer shared by the runtime entries: the app routes under
 * `Serve.layer`, which drains live reads before the listener closes.
 */
import { Serve } from "@streamsy/serve";
import { Config, Effect, Layer } from "effect";
import type { HttpServer } from "effect/http";
import * as App from "./app.ts";

export const DEFAULT_PORT = 4100;

export const PortConfig = Config.Port("PORT").pipe(Config.withDefault(DEFAULT_PORT));

export const layer = <E>(
  options: App.Options,
  listener: (port: number) => Layer.Layer<HttpServer.HttpServer, E>,
) =>
  Layer.unwrap(
    Effect.map(PortConfig, (port) =>
      Serve.layer(App.routes(options), { disableLogger: true }).pipe(Layer.provide(listener(port))),
    ),
  );
