import { Config, Effect, Option, type Redacted, Schema } from "effect";

/** Runtime settings, read once when the layer graph is built. */
export const ServerConfig = Config.all({
  port: Config.Port("PORT").pipe(Config.withDefault(1340)),
  /** SQLite file for streams, checkpoints and application tables. */
  databasePath: Config.String("BUGSY_DB").pipe(
    Config.withDefault(`/tmp/streamsy-bugsy-${process.pid}.sqlite`),
  ),
  /** Postgres database for streams, checkpoints and application tables, in place of SQLite. */
  pgUrl: Config.Redacted("STREAMSY_PG_URL").pipe(Config.option),
  /** MySQL database for streams, checkpoints and application tables, in place of SQLite. */
  mysqlUrl: Config.Redacted("STREAMSY_MYSQL_URL").pipe(Config.option),
  /** Fixed list of workspaces that the server creates and runs at startup. */
  workspaces: Config.String("BUGSY_WORKSPACES").pipe(
    Config.withDefault("acme,live"),
    Config.map((list) => list.split(",").filter(Boolean)),
  ),
});

/** Both database URLs are set, so the server cannot pick one. */
export class StorageConfigConflict extends Schema.TaggedError<
  StorageConfigConflict,
  { readonly brand: unique symbol }
>()("StorageConfigConflict", { message: Schema.String }) {}

/** Where the server keeps its data. */
export type StorageChoice =
  | { readonly dialect: "sqlite"; readonly filename: string }
  | { readonly dialect: "pg"; readonly url: Redacted.Redacted }
  | { readonly dialect: "mysql"; readonly url: Redacted.Redacted };

/**
 * `STREAMSY_PG_URL` picks Postgres, `STREAMSY_MYSQL_URL` picks MySQL, and
 * neither picks SQLite at `BUGSY_DB`. Both is an error.
 */
export const storageChoice = (config: Config.Success<typeof ServerConfig>) =>
  Option.match(config.pgUrl, {
    onSome: (url) =>
      Option.isSome(config.mysqlUrl)
        ? Effect.fail(
            new StorageConfigConflict({
              message:
                "Both STREAMSY_PG_URL and STREAMSY_MYSQL_URL are set. Set one of them, or neither for SQLite.",
            }),
          )
        : Effect.succeed<StorageChoice>({ dialect: "pg", url }),
    onNone: () =>
      Effect.succeed<StorageChoice>(
        Option.match(config.mysqlUrl, {
          onSome: (url) => ({ dialect: "mysql", url }),
          onNone: () => ({ dialect: "sqlite", filename: config.databasePath }),
        }),
      ),
  });
