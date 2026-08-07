/**
 * Cloudflare Worker host.
 *
 * One `ManagedRuntime` lives for the isolate lifetime and is reused by `fetch`
 * and the queue consumer. Concrete streams route to SQLite-backed Durable
 * Object instances through the storage adapter; no runtime key ever reaches
 * deployment state.
 */
import { HttpHandler, StreamProtocol, directProtocolClient } from "@streamsy/core";
import {
  createDurableObjectStorageAdapter,
  DurableObjectStreamStorage as StreamStorage,
} from "@streamsy/storage-durable-object";
import { ManagedRuntime } from "effect";
import {
  MeshLayer,
  repairProject,
  type ApplicationOptions,
  type MeshServices,
  type WakeMessage,
} from "./application.ts";
import { handleApi } from "./router.ts";

export { StreamStorage };

export interface Env {
  readonly STREAM_DO: DurableObjectNamespace<StreamStorage>;
  readonly ASSETS?: { readonly fetch: (request: Request) => Promise<Response> };
  readonly PROJECTION_WAKES?: { readonly send: (message: WakeMessage) => Promise<void> };
  readonly DEPLOYMENT?: string;
}

interface IsolateState {
  readonly runtime: ManagedRuntime.ManagedRuntime<MeshServices, never>;
  readonly application: ApplicationOptions;
}

let isolate: IsolateState | undefined;

function state(env: Env): IsolateState {
  if (isolate !== undefined) return isolate;
  const adapter = createDurableObjectStorageAdapter({ namespace: env.STREAM_DO });
  const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 1_500 });
  isolate = {
    runtime: ManagedRuntime.make(MeshLayer),
    application: {
      client: directProtocolClient(protocol),
      host: "cloudflare",
      deployment: env.DEPLOYMENT ?? "cloudflare",
      ...(env.PROJECTION_WAKES === undefined
        ? {}
        : { wake: (message: WakeMessage) => env.PROJECTION_WAKES!.send(message) }),
    },
  };
  return isolate;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const { runtime, application } = state(env);

    if (url.pathname.startsWith("/streams/")) {
      const adapter = createDurableObjectStorageAdapter({ namespace: env.STREAM_DO });
      const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 1_500 });
      return new HttpHandler({ protocol, pathPrefix: "/streams" }).fetch(request);
    }
    if (url.pathname === "/health" || url.pathname.startsWith("/api/")) {
      return runtime.runPromise(handleApi(application, request));
    }
    if (env.ASSETS !== undefined) return env.ASSETS.fetch(request);
    return new Response("Not found", { status: 404 });
  },

  /**
   * Wake delivery only affects latency. Every message runs the same bounded
   * repair the explicit endpoint runs, so a lost or duplicated wake converges.
   */
  async queue(batch: MessageBatch<WakeMessage>, env: Env): Promise<void> {
    const { runtime, application } = state(env);
    const seen = new Set<string>();
    for (const message of batch.messages) {
      const key = `${message.body.workspaceId}/${message.body.projectId}`;
      if (seen.has(key)) {
        message.ack();
        continue;
      }
      seen.add(key);
      try {
        await runtime.runPromise(
          repairProject(application, message.body.workspaceId, message.body.projectId),
        );
        message.ack();
      } catch {
        message.retry();
      }
    }
  },
} satisfies ExportedHandler<Env, WakeMessage>;
