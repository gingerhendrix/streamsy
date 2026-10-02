/**
 * Deno entry: `deno run --allow-all src/deno.ts`. SQLite runs on Deno's
 * `node:sqlite`, with the `nodeSqliteCompat` shim applied before each open.
 */
import { DenoRuntime } from "@effect/platform-deno";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Serve } from "@streamsy/serve";
import { listener, nodeSqliteCompat } from "@streamsy/serve/deno";
import { Effect, Layer } from "effect";
import * as Server from "./server.ts";

const Main = Server.layer(
  {
    runtime: "deno",
    drivers: {
      sqlite: (filename) => nodeSqliteCompat.pipe(Effect.andThen(SqliteClient.make({ filename }))),
    },
  },
  (port) => listener({ port }),
);

DenoRuntime.runMain(Layer.launch(Main), { teardown: Serve.teardown });
