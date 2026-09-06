/**
 * Finite Cloudflare topology for the projection issue tracker, as an Alchemy v2
 * stack.
 *
 * Alchemy v2 is itself an Effect program: `Alchemy.Stack` takes a providers
 * layer, a state layer, and an `Effect` describing the topology. Nothing is
 * applied by importing this module — the default export is a description, and
 * only the `alchemy` CLI (`plan` / `deploy` / `destroy`) runs it. That is why
 * `test/alchemy-stack.test.ts` can import this file and assert the shape.
 *
 * Alchemy owns one Worker, one SQLite-backed Durable Object namespace, one wake
 * queue with its consumer, and the static assets. It owns no workspace, project,
 * issue, cursor, membership, or lineage value: all of those are Streamsy runtime
 * state inside the Durable Objects.
 *
 * Every spelling here is checked against the installed alchemy@2.0.0-beta.70
 * declarations by `bun run typecheck`.
 */
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";
import type { DurableObjectStreamStorage } from "@streamsy/storage/durable-object/storage";

export const STACK_NAME = "streamsy-issue-tracker";

/**
 * The SQLite-backed Durable Object namespace that stores every durable stream.
 *
 * `server/worker.ts` re-exports the class as `StreamStorage`; the binding names
 * that export and nothing else. SQLite storage is the v2 default for a class a
 * Worker hosts itself, so there is no `sqlite: true` flag to set any more.
 */
export const StreamDO = Cloudflare.DurableObject<DurableObjectStreamStorage>("StreamDO", {
  className: "StreamStorage",
});

/** Best-effort projection wakes. Delivery affects latency, never durability. */
export const ProjectionWakes = Cloudflare.Queues.Queue("ProjectionWakes");

/**
 * The single public Worker: API, static assets, the Durable Streams protocol
 * routes, and the wake consumer's `queue` handler.
 *
 * This is Alchemy v2's *async Worker* form — `main` points at an ordinary
 * module with `fetch` and `queue` handlers. The Worker edge owns one Effect
 * `ManagedRuntime` and runs the application's Effect descriptions through it;
 * see `server/worker.ts`.
 */
export const Api = Cloudflare.Worker("Api", {
  main: "./server/worker.ts",
  compatibility: { flags: ["nodejs_compat"] },
  // The API, health, and Durable Streams routes must reach the Worker; every
  // other path is a built asset, with the SPA shell as the fallback. That is
  // the same split `server/local.ts` implements for the local host.
  assets: {
    directory: "./dist/assets",
    notFoundHandling: "single-page-application",
    runWorkerFirst: ["/api/*", "/health", "/streams/*"],
  },
  env: {
    STREAM_DO: StreamDO,
    PROJECTION_WAKES: ProjectionWakes,
    ISSUE_TRACKER_HOST: "cloudflare",
    // The deployed stage, resolved from the stack rather than restated. It is
    // the only value the Worker reads from `env` that is not a resource.
    ISSUE_TRACKER_DEPLOYMENT: Alchemy.Stage,
  },
});

/** The Worker's runtime `env`, derived from the topology rather than restated. */
export type IssueTrackerEnv = Cloudflare.InferEnv<typeof Api>;

export default Alchemy.Stack(
  STACK_NAME,
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const wakes = yield* ProjectionWakes;
    const api = yield* Api;

    // Registering the consumer is what makes Cloudflare dispatch wake messages
    // to the Worker's `queue` handler.
    yield* Cloudflare.Queues.Consumer("WakeConsumer", {
      queueId: wakes.queueId,
      scriptName: api.workerName,
    });

    return { url: api.url, worker: api.workerName, queue: wakes.queueName };
  }),
);
