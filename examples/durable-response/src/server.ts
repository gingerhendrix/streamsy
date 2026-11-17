import { BunCrypto } from "@effect/platform-bun";
import { Streamsy } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { BunStorage } from "@streamsy/storage/bun";
import { Layer } from "effect";
import { generationsLayer } from "./generations.ts";
import { scriptedModel } from "./model.ts";
import { routes } from "./routes.ts";
import { Responses } from "./streams.ts";

/** The same SQLite/Serve.layer Bun host is used by the executable and HTTP proofs. */
export const server = (options: { database: string; port: number; paceMs?: number }) => {
  const storage = BunStorage.layer(Streamsy.group(Responses), {
    client: { filename: options.database },
  });

  return Serve.layer(routes).pipe(
    Layer.provide(listener({ port: options.port, hostname: "127.0.0.1", idleTimeout: 60 })),
    Layer.provide(generationsLayer),
    Layer.provide(scriptedModel(options.paceMs)),
    Layer.provide([storage, BunCrypto.layerSecureRandom]),
  );
};
