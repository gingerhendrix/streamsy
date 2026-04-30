export const port = Number.parseInt(process.env.PORT ?? "1338", 10);
export const streamPath = "/streams/session/main";
export const contentType = "application/json";

// Streamsy's in-memory storage long-polls for up to 30 seconds. Bun's default
// HTTP idle timeout is 10 seconds, which can terminate live reads before
// Streamsy returns its normal 204 timeout response.
export const serverIdleTimeoutSeconds = 60;
