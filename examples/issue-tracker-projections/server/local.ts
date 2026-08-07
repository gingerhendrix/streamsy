/**
 * Local Bun host.
 *
 * The host owns exactly one `ManagedRuntime`, resolves storage into a fixed
 * protocol client, and translates Effects into HTTP responses. No library code
 * calls `Effect.runPromise`.
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
import { MeshLayer, type ApplicationOptions } from "./application.ts";
import { handleApi } from "./router.ts";

const here = dirname(fileURLToPath(import.meta.url));
const publicDir = join(here, "..", "public");

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
  const runtime = ManagedRuntime.make(MeshLayer);
  const application: ApplicationOptions = {
    client,
    host: "local",
    deployment: options.deployment ?? "local",
  };

  async function fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/streams/")) return streams.fetch(request);
    if (url.pathname === "/health" || url.pathname.startsWith("/api/")) {
      return runtime.runPromise(handleApi(application, request));
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

const CONTENT_TYPES: Readonly<Record<string, string>> = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json",
  ".svg": "image/svg+xml",
};

async function serveAsset(pathname: string): Promise<Response> {
  const relative = normalize(pathname === "/" ? "/index.html" : pathname).replace(
    /^(\.\.[/\\])+/,
    "",
  );
  const file = join(publicDir, relative);
  if (!file.startsWith(publicDir) || !existsSync(file)) {
    // Single-page shell fallback keeps deep links usable.
    const shell = join(publicDir, "index.html");
    if (!existsSync(shell)) return new Response("Not found", { status: 404 });
    return new Response(await readFile(shell), {
      headers: { "content-type": CONTENT_TYPES[".html"]! },
    });
  }
  const extension = file.slice(file.lastIndexOf("."));
  return new Response(await readFile(file), {
    headers: { "content-type": CONTENT_TYPES[extension] ?? "application/octet-stream" },
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
