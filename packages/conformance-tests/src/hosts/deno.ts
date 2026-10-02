/** Deno entry: `deno run -A src/hosts/deno.ts`. */
import { DenoRuntime } from "@effect/platform-deno";
import { Serve } from "@streamsy/serve";
import { listener, nodeSqliteCompat } from "@streamsy/serve/deno";
import { Layer } from "effect";
import { hostLayer } from "./sqlite-host.ts";

const Host = hostLayer({
  listener: ({ port }) => listener({ port }),
  beforeOpen: nodeSqliteCompat,
});

DenoRuntime.runMain(Layer.launch(Host), { teardown: Serve.teardown });
