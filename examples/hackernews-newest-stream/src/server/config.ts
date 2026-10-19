import { Config } from "effect";

export const streamPath = "/state/newest";

/** Host and poll settings. The server layers read them at startup. */
export const DemoConfig = Config.all({
  port: Config.Port("PORT").pipe(Config.withDefault(1339)),
  pollIntervalMs: Config.Int("HN_POLL_INTERVAL_MS").pipe(Config.withDefault(60_000)),
  newestLimit: Config.Int("HN_NEWEST_LIMIT").pipe(Config.withDefault(50)),
  databasePath: Config.String("HN_DB").pipe(Config.withDefault(".data/hackernews.sqlite")),
  hnApiBase: Config.String("HN_API_BASE").pipe(
    Config.withDefault("https://hacker-news.firebaseio.com/v0"),
    Config.map((base) => base.replace(/\/$/, "")),
  ),
});
