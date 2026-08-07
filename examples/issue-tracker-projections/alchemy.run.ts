/**
 * Finite Cloudflare topology for the projection issue tracker.
 *
 * Alchemy owns one Worker, one SQLite-backed Durable Object namespace, one
 * wake queue with its consumer, and the static assets. It owns no workspace,
 * project, issue, cursor, membership, or lineage value: all of those are
 * Streamsy runtime state inside the Durable Objects.
 *
 * Spelling is pinned to the repository's installed alchemy@0.82.1 declarations.
 */
import alchemy from "alchemy";
import { Assets, DurableObjectNamespace, Queue, Worker } from "alchemy/cloudflare";

const app = await alchemy("streamsy-issue-tracker");

const streamDO = DurableObjectNamespace("stream-do", {
  className: "StreamStorage",
  sqlite: true,
});

const wakes = await Queue<{
  workspaceId: string;
  projectId: string;
  issueId?: string;
}>("projection-wakes");

const assets = await Assets({ path: "./public" });

const worker = await Worker("api", {
  entrypoint: "./server/worker.ts",
  compatibility: "node",
  url: true,
  bindings: {
    STREAM_DO: streamDO,
    PROJECTION_WAKES: wakes,
    ASSETS: assets,
    DEPLOYMENT: app.stage,
  },
  eventSources: [wakes],
});

export type IssueTrackerEnv = typeof worker.Env;

console.log(JSON.stringify({ stage: app.stage, url: worker.url }, null, 2));

await app.finalize();
