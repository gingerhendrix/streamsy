/* oxlint-disable effecttsgo/async-function -- Cloudflare's ExportedHandler fetch and queue entry points are Promise-native platform contracts; both run application descriptions through the isolate's single ManagedRuntime. */
/**
 * Cloudflare Worker host.
 *
 * A thin executable edge, exactly like `server/local.ts`. Its `env` type is
 * `Cloudflare.InferEnv<typeof Api>` from `alchemy.run.ts`, so the bindings this
 * file reads and the bindings the Alchemy v2 stack declares cannot drift: a
 * renamed or removed binding is a typecheck failure.
 *
 * One `ManagedRuntime` lives for the isolate lifetime and is reused by `fetch`
 * and the queue consumer. Concrete streams route to SQLite-backed Durable
 * Object instances through the storage adapter; no runtime key ever reaches
 * deployment state.
 */
import { HttpHandler, StreamProtocol, directProtocolClient } from "@streamsy/core";
import { createDurableObjectStorageAdapter } from "@streamsy/storage/durable-object";
import { DurableObjectStreamStorage as StreamStorage } from "@streamsy/storage/durable-object/storage";
import { ConfigProvider, Layer, ManagedRuntime } from "effect";
import type { IssueTrackerEnv } from "../alchemy.run.ts";
import { repairProject, type ApplicationServices } from "./application.ts";
import * as AppConfigModule from "./config.ts";
import { handleApi } from "./router.ts";
import { applicationLayer } from "./runtime.ts";
import * as WakeModule from "./wake.ts";
import type { WakeMessage } from "./wake.ts";

/** The Durable Object class `alchemy.run.ts` binds by name. */
export { StreamStorage };

export type Env = IssueTrackerEnv;

interface IsolateState {
  readonly runtime: ManagedRuntime.ManagedRuntime<ApplicationServices, never>;
  readonly protocol: StreamProtocol;
}

let isolate: IsolateState | undefined;

/**
 * Build the isolate's runtime once and reuse it. Workers have no shutdown hook,
 * so the runtime deliberately lives as long as the isolate does; every scoped
 * resource inside a request is still released by `Effect.scoped` at its own
 * boundary.
 */
function state(env: Env): IsolateState {
  if (isolate !== undefined) return isolate;
  const adapter = createDurableObjectStorageAdapter({ namespace: env.STREAM_DO });
  const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 1_500 });

  // Worker bindings are the isolate's configuration source, so they are exposed
  // to the application as an Effect `ConfigProvider` rather than read directly.
  const configProvider = ConfigProvider.fromEnv({
    env: {
      ISSUE_TRACKER_HOST: env.ISSUE_TRACKER_HOST,
      ISSUE_TRACKER_DEPLOYMENT: env.ISSUE_TRACKER_DEPLOYMENT,
    },
  });

  isolate = {
    protocol,
    runtime: ManagedRuntime.make(
      applicationLayer({
        client: directProtocolClient(protocol),
        config: AppConfigModule.layerFromEnv.pipe(
          Layer.provide(ConfigProvider.layer(configProvider)),
        ),
        wake: WakeModule.layerQueue(async (message) => {
          await env.PROJECTION_WAKES.send(message);
        }),
      }),
    ),
  };
  return isolate;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { runtime, protocol } = state(env);

    if (url.pathname.startsWith("/streams/")) {
      return new HttpHandler({ protocol, pathPrefix: "/streams" }).fetch(request);
    }
    if (url.pathname === "/health" || url.pathname.startsWith("/api/")) {
      return runtime.runPromise(handleApi(request));
    }
    return new Response("Not found", { status: 404 });
  },

  /**
   * Wake delivery only affects latency. Every message runs the same bounded
   * repair the explicit endpoint runs, so a lost or duplicated wake converges.
   */
  async queue(batch: MessageBatch<WakeMessage>, env: Env): Promise<void> {
    const { runtime } = state(env);
    const seen = new Set<string>();
    for (const message of batch.messages) {
      const key = `${message.body.workspaceId}/${message.body.projectId}`;
      if (seen.has(key)) {
        message.ack();
        continue;
      }
      seen.add(key);
      try {
        await runtime.runPromise(repairProject(message.body.workspaceId, message.body.projectId));
        message.ack();
      } catch {
        message.retry();
      }
    }
  },
} satisfies ExportedHandler<Env, WakeMessage>;
