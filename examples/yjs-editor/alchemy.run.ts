import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import { Effect } from "effect";

const documents = Cloudflare.DurableObject("Documents", { className: "DocumentObject" });

export default Alchemy.Stack(
  "yjs-editor",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const endpoint: Pick<Cloudflare.WorkerProps, "domain" | "workersDev"> = {};

    if (stage === "prod") {
      endpoint.domain = "yjs-editor.streamsy.dev";
      endpoint.workersDev = false;
    }

    const worker = yield* Cloudflare.Worker("Server", {
      name: `yjs-editor-${stage}`,
      ...endpoint,
      main: "public/worker/index.js",
      bundle: false,
      compatibility: { date: "2026-08-06", flags: ["nodejs_compat"] },
      env: { DOCUMENTS: documents, COMPACTION_THRESHOLD: "1048576" },
      assets: {
        directory: "public/web",
        binding: "ASSETS",
        runWorkerFirst: true,
        notFoundHandling: "single-page-application",
      },
    });

    return { url: worker.url };
  }),
);
