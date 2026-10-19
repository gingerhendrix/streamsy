/** Node entry: `node src/node.ts`. SQLite runs on `node:sqlite`. */
import { NodeRuntime } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/node";
import { Layer } from "effect";
import * as Server from "./server.ts";

const Main = Server.layer(
  { runtime: "node", drivers: { sqlite: (filename) => SqliteClient.make({ filename }) } },
  (port) => listener({ port }),
);

NodeRuntime.runMain(Layer.launch(Main), { teardown: Serve.teardown });
