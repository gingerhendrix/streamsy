import { Http, MemoryStorage, Streamsy } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { Layer } from "effect";

const streams = Streamsy.group(Streamsy.Wildcard);

export const Server = Serve.layer(Http.routes(streams, { access: "full" }), {
  disableLogger: true,
}).pipe(Layer.provide(MemoryStorage.layer(streams)));
