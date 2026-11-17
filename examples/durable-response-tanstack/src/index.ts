import { BunFileSystem, BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { Config, Effect, FileSystem, Layer } from "effect";
import { server } from "./server.ts";

const application = Layer.unwrap(
  Effect.gen(function* () {
    const port = yield* Config.Port("PORT").pipe(Config.withDefault(1345));

    const database = yield* Config.String("DURABLE_RESPONSE_DB").pipe(
      Config.withDefault(`${import.meta.dirname}/../.data/responses.sqlite`),
    );

    const paceMs = yield* Config.Int("DURABLE_RESPONSE_PACE_MS").pipe(Config.withDefault(150));

    if (paceMs < 0) return yield* Effect.die("DURABLE_RESPONSE_PACE_MS must be nonnegative");
    const fs = yield* FileSystem.FileSystem;
    const slash = database.lastIndexOf("/");

    if (slash >= 0) yield* fs.makeDirectory(database.slice(0, slash) || "/", { recursive: true });

    return server({ port, database, paceMs });
  }),
).pipe(Layer.provide(BunFileSystem.layer));

BunRuntime.runMain(Layer.launch(application), { teardown: Serve.teardown });
