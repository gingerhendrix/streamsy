import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import { Effect } from "effect";

// One SQLite Durable Object per game. The class name must match the Worker bundle's export.
const games = Cloudflare.DurableObject("Games", { className: "GameDurableObject" });

export default Alchemy.Stack(
  "hex-domination",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const endpoint: Pick<Cloudflare.WorkerProps, "domain" | "workersDev"> = {};

    if (stage === "prod") {
      endpoint.domain = "hex-domination.streamsy.dev";
      endpoint.workersDev = false;
    }

    const worker = yield* Cloudflare.Worker("Server", {
      name: `hex-domination-${stage}`,
      ...endpoint,
      main: "dist/worker/worker.js",
      bundle: false,
      compatibility: { date: "2026-08-06", flags: ["nodejs_compat"] },
      env: { GAME: games },
      assets: {
        directory: "dist/web",
        binding: "ASSETS",
        runWorkerFirst: true,
        notFoundHandling: "single-page-application",
      },
    });

    return { url: worker.url };
  }),
);
