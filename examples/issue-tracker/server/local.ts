/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/node-builtin-import, effecttsgo/process-env -- This is the Bun executable edge: Web fetch handlers, Node-compatible static-asset reads, the listen port read from the process environment, and one startup line on the terminal. Application work runs through the single ManagedRuntime this file owns. */
/**
 * Local Bun host.
 *
 * A thin executable edge: resolve storage into a protocol client, own exactly
 * one `ManagedRuntime` for the host's lifetime, dispose it on close, and serve
 * static files. It contains no application logic and runs Effects only at the
 * request boundary.
 *
 * `storage` and `store` are the two host choices. Passing a SQLite adapter and
 * a SQLite store makes the same application durable across a restart; passing
 * neither keeps it entirely in memory.
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
  type StreamProtocolClient,
} from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import { Layer, ManagedRuntime } from "effect";
import * as AppConfigModule from "./config.ts";
import type { AppConfigOverrides } from "./config.ts";
import type { StreamGateway } from "./gateway.ts";
import { handle } from "./router.ts";
import { applicationLayer } from "./runtime.ts";
import type { ApplicationServices } from "./application.ts";
import { memoryLayer } from "./store.ts";
import { sqliteLayer } from "./store-sqlite.ts";
import type { IssueStore } from "./store.ts";

const here = dirname(fileURLToPath(import.meta.url));
/** `bun run build` emits the browser bundle here. */
const assetDir = join(here, "..", "dist", "assets");

export interface LocalHostOptions {
  /** Durable-stream storage. Defaults to memory, or SQLite under `databaseDirectory`. */
  readonly adapter?: StorageAdapter;
  /** Maintained-state store. Defaults to memory, or SQLite under `databaseDirectory`. */
  readonly store?: Layer.Layer<IssueStore>;
  /** Put both the durable log and the maintained state on disk in this directory. */
  readonly databaseDirectory?: string;
  readonly deployment?: string;
  /** Test/host adapter seam for transport fault injection around application calls. */
  readonly applicationClient?: (client: StreamProtocolClient) => StreamProtocolClient;
}

export function createLocalHost(options: LocalHostOptions = {}) {
  const adapter =
    options.adapter ??
    (options.databaseDirectory === undefined
      ? createMemoryStorageAdapter()
      : createSqliteStorageAdapter({
          filename: join(options.databaseDirectory, "streams.sqlite"),
        }));
  const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 5_000 });
  const client = directProtocolClient(protocol);
  const applicationClient = options.applicationClient?.(client) ?? client;
  const gateway = createHttpHandler({ protocol, pathPrefix: "/streams" });
  const store =
    options.store ??
    (options.databaseDirectory === undefined
      ? memoryLayer()
      : sqliteLayer({ filename: join(options.databaseDirectory, "view.sqlite") }));

  const configValues: AppConfigOverrides = { deployment: options.deployment ?? "local" };

  const runtime: ManagedRuntime.ManagedRuntime<ApplicationServices | StreamGateway, never> =
    ManagedRuntime.make(
      applicationLayer({
        client: applicationClient,
        protocol,
        gateway,
        store,
        config: AppConfigModule.layer(configValues),
      }),
    );

  async function fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/streams/")) return gateway.fetch(request);
    if (
      url.pathname === "/health" ||
      url.pathname.startsWith("/api/") ||
      url.pathname.startsWith("/state/")
    ) {
      return runtime.runPromise(handle(request));
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
<h1>Browser bundle missing</h1><p>Run <code>bun run --cwd examples/issue-tracker build</code>.</p>`;

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
        headers: { "content-type": "text/html; charset=utf-8" },
      });
    }
    return new Response(await readFile(shell), {
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  }
  const extension = file.slice(file.lastIndexOf("."));
  return new Response(await readFile(file), {
    headers: { "content-type": CONTENT_TYPES.get(extension) ?? "application/octet-stream" },
  });
}

if (import.meta.main) {
  const dataDirectory = process.env.ISSUE_TRACKER_DATA;
  const host = createLocalHost(
    dataDirectory === undefined ? {} : { databaseDirectory: dataDirectory },
  );
  const server = Bun.serve({
    port: Number(process.env.PORT ?? 8788),
    fetch: host.fetch,
    idleTimeout: 30,
  });
  console.log(`issue-tracker listening on http://localhost:${server.port}`);
}
