import { Config, Effect } from "effect";

const configuredNewestLimit = Config.Int("HN_NEWEST_LIMIT").pipe(Config.withDefault(50));

export const demoConfig = Config.all({
  port: Config.Port("PORT").pipe(Config.withDefault(1339)),
  pollIntervalMs: Config.Int("HN_POLL_INTERVAL_MS").pipe(Config.withDefault(60_000)),
  newestLimit: configuredNewestLimit,
  databasePath: Config.String("HN_DB").pipe(Config.withDefault(".data/hackernews.sqlite")),
  hnApiBase: Config.String("HN_API_BASE").pipe(
    Config.withDefault("https://hacker-news.firebaseio.com/v0"),
  ),
}).pipe(
  Effect.map((config) => {
    return {
      port: config.port,
      pollIntervalMs: config.pollIntervalMs,
      newestLimit: config.newestLimit,
      databasePath: config.databasePath,
      hnApiBase: config.hnApiBase.replace(/\/$/, ""),
    };
  }),
);

const loaded = Effect.runSync(demoConfig);

export const port = loaded.port;
export const streamPath = "/state/newest";
export const pollIntervalMs = loaded.pollIntervalMs;
export const newestLimit = loaded.newestLimit;
export const databasePath = loaded.databasePath;
export const hnApiBase = loaded.hnApiBase;

// Streamsy's in-memory storage long-polls for up to 30 seconds. Bun's default
// HTTP idle timeout is 10 seconds, which can terminate live reads before
// Streamsy returns its normal 204 timeout response.
export const serverIdleTimeoutSeconds = 60;
