export const port = Number.parseInt(process.env.PORT ?? "1339", 10);
export const streamPath = "/streams/session/main";
export const contentType = "application/json";
export const pollIntervalMs = Number.parseInt(process.env.HN_POLL_INTERVAL_MS ?? "60000", 10);
export const newestLimit = Number.parseInt(process.env.HN_NEWEST_LIMIT ?? "50", 10);

// Streamsy's in-memory storage long-polls for up to 30 seconds. Bun's default
// HTTP idle timeout is 10 seconds, which can terminate live reads before
// Streamsy returns its normal 204 timeout response.
export const serverIdleTimeoutSeconds = 60;
