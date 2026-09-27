import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { STACK_NAME } from "./src/contract.ts";

const Streams = Cloudflare.DurableObject("Streams", { className: "StreamsObject" });

export default Alchemy.Stack(
  STACK_NAME,
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const server = yield* Cloudflare.Worker("Server", {
      name: `${STACK_NAME}-${stage}`,
      main: "../packages/conformance-tests/dist/worker/worker.js",
      bundle: false,
      compatibility: { date: "2026-07-30", flags: ["nodejs_compat"] },
      workersDev: true,
      env: { STREAMS: Streams },
    });
    return { url: server.url, workerName: server.workerName };
  }),
);
