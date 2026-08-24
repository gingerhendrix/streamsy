/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/node-builtin-import, effecttsgo/process-env -- This is the Bun executable edge: Web fetch handlers, Node-compatible static-asset reads, the listen port read from the process environment, and one startup line on the terminal. Application work runs through the single ManagedRuntime this file owns. */
/**
 * Local Bun host.
 *
 * A thin executable edge: resolve storage into a protocol client, own exactly
 * one `ManagedRuntime` for the host's lifetime, dispose it on close, and serve
 * static files. It contains no application logic and no `Effect.runPromise`
 * anywhere but the request boundary.
 */
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import {
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
  type StorageAdapter,
} from "@streamsy/core";
import { ManagedRuntime } from "effect";
import type { ApplicationServices } from "./application.ts";
import * as AppConfigModule from "./config.ts";
import { handleApi } from "./router.ts";
import { applicationLayer } from "./runtime.ts";
import * as WakeModule from "./wake.ts";

const here = dirname(fileURLToPath(import.meta.url));
/** `bun run build` emits the browser bundle here. */
const assetDir = join(here, "..", "dist", "assets");

export interface LocalHostOptions {
  readonly port?: number;
  readonly adapter?: StorageAdapter;
  readonly deployment?: string;
}

export function createLocalHost(options: LocalHostOptions = {}) {
  const adapter = options.adapter ?? createMemoryStorageAdapter();
  const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 5_000 });
  const client = directProtocolClient(protocol);
  const streams = createHttpHandler({ protocol, pathPrefix: "/streams" });

  const runtime: ManagedRuntime.ManagedRuntime<ApplicationServices, never> = ManagedRuntime.make(
    applicationLayer({
      client,
      config: AppConfigModule.layer({ host: "local", deployment: options.deployment ?? "local" }),
      // The local host has no queue, so repair and the next request are the
      // only convergence. That is a host capability, not an application flag.
      wake: WakeModule.layerDisabled,
    }),
  );

  async function fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/streams/")) return streams.fetch(request);
    if (url.pathname === "/health" || url.pathname.startsWith("/api/")) {
      return runtime.runPromise(handleApi(request));
    }
    return serveAsset(url.pathname);
  }

  return {
    adapter,
    client,
    runtime,
    fetch,
    async close() {
      await runtime.dispose();
      await client.close();
    },
  };
}

const CONTENT_TYPES = new Map([
  [".html", "text/html; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".map", "application/json"],
  [".json", "application/json"],
  [".svg", "image/svg+xml"],
]);

const MISSING_BUILD = `<!doctype html><meta charset="utf-8"><title>Build required</title>
<body style="font:14px system-ui;padding:24px;background:#0f1115;color:#e6e9ef">
<h1>Browser bundle missing</h1><p>Run <code>bun run --cwd examples/issue-tracker-projections build</code>.</p>`;

async function serveAsset(pathname: string): Promise<Response> {
  const relative = normalize(pathname === "/" ? "/index.html" : pathname).replace(
    /^(\.\.[/\\])+/,
    "",
  );
  const file = join(assetDir, relative);
  if (!file.startsWith(assetDir) || !existsSync(file)) {
    const shell = join(assetDir, "index.html");
    if (!existsSync(shell)) {
      return new Response(MISSING_BUILD, {
        status: 200,
        headers: { "content-type": CONTENT_TYPES.get(".html") ?? "text/html; charset=utf-8" },
      });
    }
    return new Response(await readFile(shell), {
      headers: { "content-type": CONTENT_TYPES.get(".html") ?? "text/html; charset=utf-8" },
    });
  }
  const extension = file.slice(file.lastIndexOf("."));
  return new Response(await readFile(file), {
    headers: { "content-type": CONTENT_TYPES.get(extension) ?? "application/octet-stream" },
  });
}

if (import.meta.main) {
  const host = createLocalHost({ port: Number(process.env.PORT ?? 8787) });
  const server = Bun.serve({
    port: Number(process.env.PORT ?? 8787),
    fetch: host.fetch,
    idleTimeout: 30,
  });
  console.log(`issue-tracker-projections listening on http://localhost:${server.port}`);
}
