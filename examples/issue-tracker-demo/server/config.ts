import { Config } from "effect";

/** Runtime settings, read once when the layer graph is built. */
export const ServerConfig = Config.all({
  port: Config.Port("PORT").pipe(Config.withDefault(1338)),
  /** SQLite file. Unset keeps a process-local memory store. */
  databasePath: Config.option(Config.String("ISSUE_TRACKER_DB")),
  isDevelopment: Config.String("NODE_ENV").pipe(
    Config.withDefault("development"),
    Config.map((env) => env !== "production"),
  ),
});

export const streamPrefix = "/streams";

/** The always-present, seeded demo workspace (default when no `?w=` is set). */
export const mainWorkspaceId = "main";

/** Stream id for a workspace, served at `/streams/workspace/<id>`. */
export function workspaceStreamId(workspaceId: string): string {
  return `workspace/${workspaceId}`;
}

// Streamsy's in-memory storage long-polls for up to 30 seconds. Bun's default
// HTTP idle timeout is 10 seconds, which can terminate live reads before
// Streamsy returns its normal 204 timeout response.
export const serverIdleTimeoutSeconds = 60;
