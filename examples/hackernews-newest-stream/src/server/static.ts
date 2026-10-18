import { Effect, FileSystem, Layer, Path } from "effect";
import { HttpRouter, HttpServerResponse, HttpStaticServer } from "effect/http";

const missingBuild =
  "Hacker News newest stream API is running. Run `bun run build` in examples/hackernews-newest-stream to serve the React app from this Bun server.";

/**
 * The built React client from `dist/`, with the SPA fallback to `index.html`.
 * The server checks for the build once at startup. Before the first build,
 * `GET /` answers with a plain-text hint.
 */
export const clientRoute = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = path.join(import.meta.dirname, "../../dist");

    return (yield* fs.exists(path.join(root, "index.html")))
      ? HttpStaticServer.layer({ root, spa: true })
      : HttpRouter.add("GET", "/", HttpServerResponse.text(missingBuild));
  }),
);
