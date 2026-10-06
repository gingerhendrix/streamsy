import { BunRuntime, BunServices } from "@effect/platform-bun";
import { MysqlClient } from "@effect/sql-mysql2";
import { PgClient } from "@effect/sql-pg";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { Effect, FileSystem, Layer, Path } from "effect";
import { app } from "./app.ts";
import { ServerConfig, storageChoice, type StorageChoice } from "./config.ts";
import { applicationLayer, sqlStorage, sqliteStorage } from "./host.ts";

/** The storage Layer for the configured database. */
const storage = (choice: StorageChoice) =>
  Effect.gen(function* () {
    if (choice.dialect === "pg") return sqlStorage(PgClient.make({ url: choice.url }));
    if (choice.dialect === "mysql") return sqlStorage(MysqlClient.make({ url: choice.url }));
    if (choice.filename !== ":memory:") {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      yield* fs.makeDirectory(path.dirname(choice.filename), { recursive: true });
    }
    return sqliteStorage(choice.filename);
  });

const Server = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig;
    const host = applicationLayer(yield* storage(yield* storageChoice(config)), config.workspaces);
    return Serve.layer(app(config.workspaces), { disableLogger: true }).pipe(
      Layer.provide(host),
      Layer.provideMerge(listener({ port: config.port })),
    );
  }),
).pipe(Layer.provide(BunServices.layer));

BunRuntime.runMain(Layer.launch(Server), { teardown: Serve.teardown });
