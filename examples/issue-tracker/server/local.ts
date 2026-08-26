/* oxlint-disable effecttsgo/async-function, effecttsgo/global-console, effecttsgo/node-builtin-import -- This is the executable edge: it serves files from disk to `Bun.serve`'s Promise-native `fetch` and reports to the invoking terminal. */
/**
 * Local Bun host.
 *
 * A thin executable edge over the keyed multi-workspace host in `host.ts`: it
 * decides where the data lives, serves the browser bundle, and — when run as a
 * program — drives effect delivery on a timer and shuts every partition down
 * cleanly.
 *
 * `createLocalHost` is the single-workspace *view* of that host. It keeps the
 * shape the tests and the smoke scripts drive (`adapter`, `client`, `runtime`),
 * resolving each against one default workspace, while requests for any other
 * workspace still open their own isolated partition. Passing `adapter` or
 * `store` explicitly pins the host to that one workspace, because a single
 * adapter or store value cannot be two partitions' durable state.
 */
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import type { StorageAdapter, StreamProtocolClient } from "@streamsy/core";
import type { OutboxStore } from "@streamsy/effect-sink";
import type { Layer, ManagedRuntime } from "effect";
import { workspaceKey } from "../domain/domains.ts";
import type { ApplicationServices } from "./application.ts";
import type { StreamGateway } from "./gateway.ts";
import type { GlobalServices } from "./global-domain.ts";
import {
  createWorkspaceHost,
  type DeliveryPolicy,
  type ExchangePolicy,
  type PartitionPolicy,
  type WorkspaceHost,
} from "./host.ts";
import type { InboxStore } from "./inbox-store.ts";
import type { NotificationTargetOptions } from "./notifications.ts";
import type { IssueStore } from "./store.ts";

const here = dirname(fileURLToPath(import.meta.url));
/** `bun run build` emits the browser bundle here. */
const assetDir = join(here, "..", "dist", "assets");

/** The workspace `adapter`, `client` and `runtime` resolve to. */
export const DEFAULT_WORKSPACE_ID = "main";

export interface LocalHostOptions {
  /** Durable-stream storage for the default workspace. Pins the host to it. */
  readonly adapter?: StorageAdapter;
  /** Maintained state and the effect-sink outbox for the default workspace. Pins the host to it. */
  readonly store?: Layer.Layer<IssueStore | OutboxStore>;
  /** Where assignment notifications land. Defaults to the in-process log. */
  readonly notifications?: NotificationTargetOptions;
  /** Put every workspace's durable log and maintained state under this directory. */
  readonly databaseDirectory?: string;
  readonly deployment?: string;
  /** Test/host adapter seam for transport fault injection around application calls. */
  readonly applicationClient?: (client: StreamProtocolClient) => StreamProtocolClient;
  /** Per-user-partition inbox storage. A factory: two users never share one. */
  readonly inbox?: (userId: string) => Layer.Layer<InboxStore>;
  /** The global partition's exchange cursors and source registry. */
  readonly exchangeStore?: () => Layer.Layer<GlobalServices>;
  readonly partitions?: PartitionPolicy;
  readonly delivery?: DeliveryPolicy;
  readonly exchange?: ExchangePolicy;
  readonly now?: () => number;
  /** Which workspace the single-workspace accessors resolve to. */
  readonly defaultWorkspaceId?: string;
}

export interface LocalHost {
  /** The keyed host underneath. Multi-workspace lifecycle and metrics live here. */
  readonly host: WorkspaceHost;
  readonly workspaceId: string;
  /** The default workspace's durable-stream storage. Opens the partition on first read. */
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly runtime: ManagedRuntime.ManagedRuntime<ApplicationServices | StreamGateway, never>;
  readonly fetch: (request: Request) => Promise<Response>;
  readonly close: () => Promise<void>;
}

export function createLocalHost(options: LocalHostOptions = {}): LocalHost {
  const workspaceId = options.defaultWorkspaceId ?? DEFAULT_WORKSPACE_ID;
  // Bound once, so the per-partition factories the keyed host wants are built
  // from values rather than from repeated optional reads.
  const pinnedAdapter = options.adapter;
  const pinnedStore = options.store;
  const wrapClient = options.applicationClient;
  const host = createWorkspaceHost({
    databaseDirectory: options.databaseDirectory,
    deployment: options.deployment,
    adapter: pinnedAdapter === undefined ? undefined : () => pinnedAdapter,
    store: pinnedStore === undefined ? undefined : () => pinnedStore,
    inbox: options.inbox,
    exchangeStore: options.exchangeStore,
    notifications: options.notifications,
    applicationClient: wrapClient === undefined ? undefined : (client) => wrapClient(client),
    partitions: options.partitions,
    delivery: options.delivery,
    exchange: options.exchange,
    now: options.now,
    fallback: (request) => serveAsset(new URL(request.url).pathname),
  });

  /** Open the default partition, or report why the host cannot. */
  const partition = () => {
    const opened = host.partition(workspaceKey(workspaceId));
    if ("_tag" in opened) throw new Error(`${opened._tag}: ${workspaceId}`);
    return opened;
  };

  return {
    host,
    workspaceId,
    get adapter() {
      return partition().adapter;
    },
    get client() {
      return partition().client;
    },
    get runtime() {
      return partition().runtime;
    },
    fetch: host.fetch,
    close: host.close,
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
  // A running host drives its own effect delivery and its own exchange on the
  // one managed tick; nothing else would.
  const started: LocalHostOptions = {
    delivery: { mode: "interval" },
    exchange: { mode: "interval" },
  };
  const host = createLocalHost(
    dataDirectory === undefined ? started : { ...started, databaseDirectory: dataDirectory },
  );
  const server = Bun.serve({
    port: Number(process.env.PORT ?? 8788),
    fetch: host.fetch,
    idleTimeout: 30,
  });
  const shutdown = async (): Promise<void> => {
    await server.stop(true);
    await host.close();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown());
  process.on("SIGTERM", () => void shutdown());
  console.log(`issue-tracker listening on http://localhost:${server.port}`);
}
