import { Config } from "effect";

/** Runtime settings, read once when the layer graph is built. */
export const ServerConfig = Config.all({
  port: Config.Port("PORT").pipe(Config.withDefault(1340)),
  /** SQLite file for streams, checkpoints and application tables. */
  databasePath: Config.String("ISSUE_TRACKER_DB").pipe(
    Config.withDefault(`/tmp/streamsy-issue-tracker-${process.pid}.sqlite`),
  ),
  /** Fixed list of workspaces that the server creates and runs at startup. */
  workspaces: Config.String("ISSUE_TRACKER_WORKSPACES").pipe(
    Config.withDefault("acme,live"),
    Config.map((list) => list.split(",").filter(Boolean)),
  ),
});
