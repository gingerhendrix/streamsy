import { Config, Effect, Option } from "effect";

const configuredNewestLimit = Config.int("HN_NEWEST_LIMIT").pipe(Config.withDefault(50));

export const demoConfig = Config.all({
  port: Config.port("PORT").pipe(Config.withDefault(1339)),
  streamPrefix: Config.string("STREAM_PREFIX").pipe(Config.withDefault("/streams")),
  targetStreamId: Config.string("HN_TARGET_STREAM_ID").pipe(Config.withDefault("session/main")),
  sourceStreamId: Config.string("HN_SOURCE_STREAM_ID").pipe(
    Config.withDefault("session/main/source"),
  ),
  streamContentType: Config.string("STREAM_CONTENT_TYPE").pipe(
    Config.withDefault("application/json"),
  ),
  pollIntervalMs: Config.int("HN_POLL_INTERVAL_MS").pipe(Config.withDefault(60_000)),
  newestLimit: configuredNewestLimit,
  projectionMaxPages: Config.int("HN_PROJECTION_MAX_PAGES").pipe(Config.withDefault(10)),
  projectionMaxBatches: Config.int("HN_PROJECTION_MAX_BATCHES").pipe(Config.withDefault(10)),
  projectionMaxItems: Config.option(Config.int("HN_PROJECTION_MAX_ITEMS")),
  projectionMaxBytes: Config.int("HN_PROJECTION_MAX_BYTES").pipe(Config.withDefault(1_000_000)),
  hnApiBase: Config.string("HN_API_BASE").pipe(
    Config.withDefault("https://hacker-news.firebaseio.com/v0"),
  ),
}).pipe(
  Effect.map((config) => {
    const streamPrefix = normalizePrefix(config.streamPrefix);
    const targetStreamId = normalizeStreamId(config.targetStreamId);
    const sourceStreamId = normalizeStreamId(config.sourceStreamId);
    return {
      port: config.port,
      streamPrefix,
      targetStreamId,
      sourceStreamId,
      streamPath: `${streamPrefix}/${targetStreamId}`,
      sourceStreamPath: `${streamPrefix}/${sourceStreamId}`,
      streamContentType: config.streamContentType,
      pollIntervalMs: config.pollIntervalMs,
      newestLimit: config.newestLimit,
      projectionLimits: {
        pages: config.projectionMaxPages,
        batches: config.projectionMaxBatches,
        items: Option.getOrElse(config.projectionMaxItems, () => config.newestLimit * 2),
        bytes: config.projectionMaxBytes,
      },
      hnApiBase: config.hnApiBase.replace(/\/$/, ""),
    };
  }),
);

const loaded = Effect.runSync(demoConfig);

export const port = loaded.port;
export const streamPrefix = loaded.streamPrefix;
export const targetStreamId = loaded.targetStreamId;
export const sourceStreamId = loaded.sourceStreamId;
export const streamPath = loaded.streamPath;
export const sourceStreamPath = loaded.sourceStreamPath;
export const streamContentType = loaded.streamContentType;
export const pollIntervalMs = loaded.pollIntervalMs;
export const newestLimit = loaded.newestLimit;
export const projectionLimits = loaded.projectionLimits;
export const hnApiBase = loaded.hnApiBase;

// Streamsy's in-memory storage long-polls for up to 30 seconds. Bun's default
// HTTP idle timeout is 10 seconds, which can terminate live reads before
// Streamsy returns its normal 204 timeout response.
export const serverIdleTimeoutSeconds = 60;

function normalizePrefix(value: string): string {
  const trimmed = value.replace(/\/+$/, "");
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

function normalizeStreamId(value: string): string {
  return value.replace(/^\/+/, "");
}
