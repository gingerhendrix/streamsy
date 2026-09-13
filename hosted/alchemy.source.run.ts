import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { sourceWorker } from "./src/source-worker.ts";
import { STACK_NAME } from "./src/contract.ts";

/** Source-form alternative to alchemy.run.ts. This stack is typechecked, never executed here. */
export default Alchemy.Stack(
  STACK_NAME,
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const server = yield* sourceWorker(`${STACK_NAME}-${stage}`);
    return { url: server.url, workerName: server.workerName };
  }),
);
