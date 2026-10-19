/** Bun entry: `bun src/bun.ts`. SQLite runs on `bun:sqlite`. */
import { BunRuntime } from "@effect/platform-bun";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { Layer } from "effect";
import * as Server from "./server.ts";

const Main = Server.layer(
  { runtime: "bun", drivers: { sqlite: (filename) => SqliteClient.make({ filename }) } },
  (port) => listener({ port }),
);

BunRuntime.runMain(Layer.launch(Main), { teardown: Serve.teardown });
