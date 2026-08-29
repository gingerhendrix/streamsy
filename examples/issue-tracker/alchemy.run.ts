/**
 * Example-owned Cloudflare placement for the Integration 3A issue tracker.
 *
 * Importing this module only constructs an Alchemy v2 Effect description. The
 * CLI is the only edge that plans, deploys, or destroys its resources.
 */
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";

export const STACK_NAME = "streamsy-issue-tracker-3b";
export const WORKSPACE_OBJECT_CLASS = "WorkspacePartitionObject";
export const WORKSPACE_OBJECT_MIGRATION = `new_sqlite_classes:${WORKSPACE_OBJECT_CLASS}`;
export const USER_OBJECT_CLASS = "UserPartitionObject";
export const GLOBAL_OBJECT_CLASS = "GlobalExchangeObject";

/** One SQLite Durable Object namespace, partitioned by canonical workspace key. */
export const WorkspacePartitions = Cloudflare.DurableObject("WorkspacePartitions", {
  className: WORKSPACE_OBJECT_CLASS,
});
export const UserPartitions = Cloudflare.DurableObject("UserPartitions", {
  className: USER_OBJECT_CLASS,
});
export const GlobalExchange = Cloudflare.DurableObject("GlobalExchange", {
  className: GLOBAL_OBJECT_CLASS,
});

/** Stateless gateway Worker, application API, checked sinks, streams and assets. */
export const Gateway = Cloudflare.Worker("Gateway", {
  main: "./server/cloudflare.ts",
  compatibility: { flags: ["nodejs_compat"] },
  assets: {
    directory: "./dist/assets",
    notFoundHandling: "single-page-application",
    runWorkerFirst: [
      "/health",
      "/host/*",
      "/api/*",
      "/streams/*",
      "/state/*",
      "/feed/*",
      "/document/*",
    ],
  },
  env: {
    WORKSPACES: WorkspacePartitions,
    USERS: UserPartitions,
    GLOBALS: GlobalExchange,
    DEPLOYMENT: Alchemy.Stage,
  },
});

/** Runtime bindings derived from the topology rather than restated by hand. */
export type IssueTrackerCloudflareEnv = Cloudflare.InferEnv<typeof Gateway>;

export default Alchemy.Stack(
  STACK_NAME,
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const gateway = yield* Gateway;
    return {
      stage,
      url: gateway.url,
      workerId: gateway.workerId,
      workerName: gateway.workerName,
      workspaceNamespaceId: gateway.durableObjectNamespaces[WORKSPACE_OBJECT_CLASS],
      workspaceClass: WORKSPACE_OBJECT_CLASS,
      workspaceMigration: WORKSPACE_OBJECT_MIGRATION,
      userNamespaceId: gateway.durableObjectNamespaces[USER_OBJECT_CLASS],
      globalNamespaceId: gateway.durableObjectNamespaces[GLOBAL_OBJECT_CLASS],
      userClass: USER_OBJECT_CLASS,
      globalClass: GLOBAL_OBJECT_CLASS,
    };
  }),
);
