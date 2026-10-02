import { BunRuntime } from "@effect/platform-bun";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { Config, Console, Effect, Layer } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";

// A minimal host for the spawnServer tests. FIXTURE_MODE picks how it stops:
// stderr (exit 0 with stderr output) or interrupted (default teardown, exit 130).
const Server = Layer.unwrap(
  Effect.gen(function* () {
    const port = yield* Config.Port("PORT");
    const mode = yield* Config.String("FIXTURE_MODE");
    const routes = HttpRouter.add("GET", "/", Effect.succeed(HttpServerResponse.text("ok")));
    const onStop =
      mode === "stderr"
        ? Layer.effectDiscard(Effect.addFinalizer(() => Console.error("fixture wrote to stderr")))
        : Layer.empty;
    return Serve.layer(routes, { disableLogger: true }).pipe(
      Layer.provide(onStop),
      Layer.provide(listener({ port })),
    );
  }),
);

// runMain options are fixed before the program runs, so this one read skips Config.
// oxlint-disable-next-line effecttsgo/process-env
const mode = process.env["FIXTURE_MODE"];
BunRuntime.runMain(
  Layer.launch(Server),
  mode === "interrupted" ? {} : { teardown: Serve.teardown },
);
