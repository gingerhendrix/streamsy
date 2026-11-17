import { BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { Config, Effect, FileSystem, Layer } from "effect";
import { BunFileSystem } from "@effect/platform-bun";
import { server } from "./server.ts";

const application = Layer.unwrap(
  Effect.gen(function* () {
    const port = yield* Config.Port("PORT").pipe(Config.withDefault(1340));

    const database = yield* Config.String("DURABLE_RESPONSE_DB").pipe(
      Config.withDefault(`${import.meta.dirname}/../.data/responses.sqlite`),
    );

    const paceMs = yield* Config.Int("DURABLE_RESPONSE_PACE_MS").pipe(Config.withDefault(250));

    if (paceMs < 0) return yield* Effect.die("DURABLE_RESPONSE_PACE_MS must be nonnegative");
    const fs = yield* FileSystem.FileSystem;
    const lastSlash = database.lastIndexOf("/");

    if (lastSlash >= 0)
      yield* fs.makeDirectory(database.slice(0, lastSlash) || "/", { recursive: true });

    return server({ database, port, paceMs });
  }),
).pipe(Layer.provide(BunFileSystem.layer));

BunRuntime.runMain(Layer.launch(application), { teardown: Serve.teardown });
