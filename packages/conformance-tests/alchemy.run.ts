/**
 * Durable Streams Server Resource
 *
 * Alchemy resource definition for deploying the Durable Object
 * implementation of the Durable Streams protocol.
 */
import alchemy from "alchemy";
import { Worker, DurableObjectNamespace } from "alchemy/cloudflare";

const app = await alchemy("streamsy-conf");

const streamDO = DurableObjectNamespace("stream-do", {
  className: "StreamStorage",
  sqlite: true,
});

// Create the worker that handles HTTP requests
const worker = await Worker("server", {
  entrypoint: "./src/do-worker.ts",
  compatibility: "node",
  bindings: {
    STREAM_DO: streamDO,
  },
});

export type DurableStreamsServerEnv = typeof worker.Env;

await app.finalize();
