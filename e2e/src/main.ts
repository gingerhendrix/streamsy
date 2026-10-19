import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Array, Console, Effect, Layer } from "effect";
import { liveHttp, liveIdentity, liveOperations, livePaths, liveReports } from "./live.ts";
import { runEvidence } from "./run.ts";

const usage = Array.join(
  [
    "streamsy deployed e2e",
    "",
    "Deploys the conformance Worker to a new workers.dev stage with Alchemy,",
    "runs the official Durable Streams suite against it, then destroys it.",
    "Build the worker first: bun run --cwd ../packages/conformance-tests build:worker",
    "Needs a Cloudflare login in the default Alchemy profile.",
    "The report is written to reports/<stage>.json.",
  ],
  "\n",
);

const paths = livePaths(new URL("..", import.meta.url).pathname.replace(/\/$/, ""));

const live = Layer.mergeAll(liveOperations(paths), liveReports(paths), liveHttp).pipe(
  Layer.provideMerge(BunServices.layer),
);

const program = process.argv.slice(2).includes("--help")
  ? Console.log(usage)
  : Effect.gen(function* () {
      const identity = yield* liveIdentity(paths);
      yield* Effect.logInfo(`Deployed e2e run: stage ${identity.stage}, ${identity.workerName}`);
      yield* runEvidence(identity);
      yield* Effect.logInfo(`Deployed e2e passed and ${identity.workerName} was destroyed`);
    });

// oxlint-disable-next-line effecttsgo/strict-effect-provide -- This is the CLI entry point.
BunRuntime.runMain(program.pipe(Effect.provide(live)));
