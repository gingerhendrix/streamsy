/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- The host is the executable edge: it is entered from `Bun.serve`'s Promise-native `fetch`, and a durable partition needs a real directory on disk before its databases exist. Everything below the request boundary is an Effect. */
/**
 * The keyed multi-workspace host.
 *
 * One process, many workspaces, one partition each. A partition owns
 * everything a single workspace needs and shares none of it: its durable
 * stream storage, its protocol client and gateway, its maintained-state store,
 * its outbox, and its `ManagedRuntime`. Routing decides *which* partition
 * before any application code runs, so a request for workspace A cannot read
 * or write B's rows, streams, receipts or deliveries — not because the
 * application filters by id, but because B's services are not reachable from
 * A's runtime at all.
 *
 * Three lifecycle facts follow from that and are the whole operational model:
 *
 * - **Partitions open lazily.** The first request for a workspace builds it.
 *   A host with a thousand configured workspaces and one active user holds one
 *   partition.
 * - **Partitions close independently.** Restarting, evicting or idling one
 *   partition disposes exactly its runtime and its connections. Every other
 *   partition keeps serving, and the closed one rebuilds from its durable
 *   state on the next request.
 * - **Closing is idempotent.** A partition is disposed exactly once however
 *   many times it is asked, so a shutdown racing an eviction cannot
 *   double-close a SQLite handle.
 *
 * Time is injected (`now`) and idle sweeping is an explicit call, so partition
 * lifecycle is tested by driving it rather than by waiting for a clock.
 */
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import {
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import type { OutboxStore } from "@streamsy/effect-sink";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";
import type { Layer, ManagedRuntime } from "effect";
import { ManagedRuntime as ManagedRuntimeModule } from "effect";
import { issues } from "../domain/declaration.ts";
import * as AppConfigModule from "./config.ts";
import { PLAN_HASH, SCHEMA_VERSION } from "./config.ts";
import { drainNotifications } from "./application.ts";
import type { ApplicationServices } from "./application.ts";
import type { StreamGateway } from "./gateway.ts";
import {
  hostFailureResponse,
  HostClosed,
  PartitionLimitReached,
  PartitionUnavailable,
  type HostFailure,
} from "./host-errors.ts";
import { invalidSinkParamsResponse, resolveRoute } from "./host-routing.ts";
import type { NotificationTargetOptions } from "./notifications.ts";
import { handle } from "./router.ts";
import { applicationLayer } from "./runtime.ts";
import { memoryLayer, type IssueStore } from "./store.ts";
import { sqliteLayer } from "./store-sqlite.ts";

/** How many partitions may be open at once, and when an idle one is given up. */
export interface PartitionPolicy {
  /** Upper bound on simultaneously open partitions. */
  readonly maxOpen?: number;
  /** How long a partition may go unused before `sweepIdle` closes it. */
  readonly idleMillis?: number;
}

/**
 * How the host drives effect delivery.
 *
 * `manual` is the default because delivery is observable product state: a test
 * — and the HTTP smoke — asserts what a drain claimed, and a background timer
 * racing those assertions would make them lie. `interval` is what a running
 * host uses, and it calls exactly the same `drainDue` a manual caller does.
 */
export interface DeliveryPolicy {
  readonly mode?: "manual" | "interval";
  readonly intervalMs?: number;
  /** Upper bound on entries claimed per partition per pass. */
  readonly limit?: number;
}

export interface WorkspaceHostOptions {
  /** Put every partition's durable log and maintained state under this directory. */
  readonly databaseDirectory?: string;
  readonly deployment?: string;
  /** Per-partition durable-stream storage. Defaults to memory, or SQLite under `databaseDirectory`. */
  readonly adapter?: (workspaceId: string) => StorageAdapter;
  /**
   * Per-partition maintained state and outbox.
   *
   * A *factory*, not a layer: two partitions must never share one store, and a
   * single layer value would hand them the same backing.
   */
  readonly store?: (workspaceId: string) => Layer.Layer<IssueStore | OutboxStore>;
  readonly notifications?: NotificationTargetOptions;
  /** Test/host adapter seam for transport fault injection, per partition. */
  readonly applicationClient?: (
    client: StreamProtocolClient,
    workspaceId: string,
  ) => StreamProtocolClient;
  readonly partitions?: PartitionPolicy;
  readonly delivery?: DeliveryPolicy;
  /** Injected clock, so lifecycle policy is driven rather than awaited. */
  readonly now?: () => number;
  /** What to serve for paths that are not application paths. Static files, usually. */
  readonly fallback?: (request: Request) => Promise<Response>;
}

const DEFAULT_MAX_OPEN = 64;
const DEFAULT_IDLE_MILLIS = 300_000;
const DEFAULT_INTERVAL_MS = 1_000;

export interface PartitionDeliveryMetrics {
  readonly passes: number;
  readonly claimed: number;
  readonly delivered: number;
  readonly retried: number;
  readonly deadLettered: number;
  /** Passes that failed outright. A failing notifier is counted, never propagated. */
  readonly failures: number;
  readonly lastError: string | null;
}

export interface PartitionMetrics {
  readonly workspaceId: string;
  /** How many times this workspace has been opened, including after a restart. */
  readonly opens: number;
  readonly requests: number;
  readonly inFlight: number;
  readonly openedAtMs: number;
  readonly lastRequestAtMs: number;
  readonly delivery: PartitionDeliveryMetrics;
}

export interface HostMetrics {
  readonly deployment: string;
  readonly planHash: string;
  readonly open: number;
  readonly opened: number;
  readonly closed: number;
  /** Closed to make room for another partition. */
  readonly evicted: number;
  /** Closed by an idle sweep. */
  readonly idled: number;
  readonly restarted: number;
  /** Requests routed to a partition. */
  readonly requests: number;
  /** Requests answered by the host itself: health and metrics. */
  readonly hostRequests: number;
  /** Refusals by typed failure tag. */
  readonly failures: Readonly<Record<string, number>>;
  readonly delivery: Omit<PartitionDeliveryMetrics, "lastError">;
  readonly workspaces: readonly PartitionMetrics[];
}

/** What `/health` reports: the identity of the application this host carries. */
export interface HostHealth {
  readonly status: "ok";
  readonly deployment: string;
  readonly schemaVersion: string;
  readonly view: string;
  readonly planHash: string;
}

/** Everything the host itself answers with, as opposed to what a partition answers. */
type HostResponseBody =
  | HostHealth
  | HostMetrics
  | { readonly error: string; readonly detail: string };

/** One partition's delivery pass, as an operator reads it. */
export interface DeliveryPassReport {
  readonly workspaceId: string;
  readonly claimed: number;
  readonly delivered: number;
  readonly retried: number;
  readonly deadLettered: number;
  readonly failed: boolean;
  readonly detail?: string;
}

interface DeliveryCounters {
  passes: number;
  claimed: number;
  delivered: number;
  retried: number;
  deadLettered: number;
  failures: number;
  lastError: string | null;
}

/** One workspace's isolated runtime and the durable resources it owns. */
export interface Partition {
  readonly workspaceId: string;
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly gateway: { readonly fetch: (request: Request) => Promise<Response> };
  readonly runtime: ManagedRuntime.ManagedRuntime<ApplicationServices | StreamGateway, never>;
  readonly openedAtMs: number;
  opens: number;
  requests: number;
  inFlight: number;
  lastRequestAtMs: number;
  draining: boolean;
  delivery: DeliveryCounters;
  closing: Promise<void> | undefined;
}

export interface WorkspaceHost {
  readonly fetch: (request: Request) => Promise<Response>;
  /** Open (or reuse) one partition. The typed failure is a value, never a throw. */
  readonly partition: (workspaceId: string) => Partition | HostFailure;
  /** The ids of every currently open partition. */
  readonly openWorkspaces: () => readonly string[];
  /** Dispose one partition. It rebuilds from durable state on its next request. */
  readonly restart: (workspaceId: string) => Promise<boolean>;
  /** Close every partition unused for at least `idleMillis`. Returns the ids closed. */
  readonly sweepIdle: (nowMs?: number) => Promise<readonly string[]>;
  /** One delivery pass over every open partition. Failures are isolated per partition. */
  readonly drainDue: () => Promise<readonly DeliveryPassReport[]>;
  readonly metrics: () => HostMetrics;
  readonly close: () => Promise<void>;
}

export function createWorkspaceHost(options: WorkspaceHostOptions = {}): WorkspaceHost {
  const now = options.now ?? (() => Date.now());
  const maxOpen = options.partitions?.maxOpen ?? DEFAULT_MAX_OPEN;
  const idleMillis = options.partitions?.idleMillis ?? DEFAULT_IDLE_MILLIS;
  const deployment = options.deployment ?? "local";
  const deliveryMode = options.delivery?.mode ?? "manual";

  const partitions = new Map<string, Partition>();
  /** Survives a close, so `opens` still counts a workspace that was restarted. */
  const openCounts = new Map<string, number>();
  const totals = {
    opened: 0,
    closed: 0,
    evicted: 0,
    idled: 0,
    restarted: 0,
    requests: 0,
    hostRequests: 0,
  };
  const failures = new Map<string, number>();
  let closing: Promise<void> | undefined;

  const countFailure = (failure: HostFailure): HostFailure => {
    failures.set(failure._tag, (failures.get(failure._tag) ?? 0) + 1);
    return failure;
  };

  function open(workspaceId: string): Partition | HostFailure {
    const existing = partitions.get(workspaceId);
    if (existing !== undefined && existing.closing === undefined) return existing;
    if (closing !== undefined) return countFailure(new HostClosed({ pathname: workspaceId }));
    if (partitions.size >= maxOpen && !evictOne()) {
      return countFailure(new PartitionLimitReached({ workspaceId, maxOpen }));
    }
    try {
      const opens = (openCounts.get(workspaceId) ?? 0) + 1;
      openCounts.set(workspaceId, opens);
      const created = build(workspaceId, opens);
      partitions.set(workspaceId, created);
      totals.opened += 1;
      return created;
    } catch (cause) {
      return countFailure(
        new PartitionUnavailable({
          workspaceId,
          detail: cause instanceof Error ? cause.message : String(cause),
        }),
      );
    }
  }

  function build(workspaceId: string, opens: number): Partition {
    const directory =
      options.databaseDirectory === undefined
        ? undefined
        : partitionDirectory(options.databaseDirectory, workspaceId);
    const adapter =
      options.adapter?.(workspaceId) ??
      (directory === undefined
        ? createMemoryStorageAdapter()
        : createSqliteStorageAdapter({ filename: join(directory, "streams.sqlite") }));
    const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 5_000 });
    const client = directProtocolClient(protocol);
    const gateway = createHttpHandler({ protocol, pathPrefix: "/streams" });
    // One store layer per partition, and one SQLite connection inside it: the
    // maintained rows, the receipts and the outbox share a durable boundary, so
    // an accepted command and the effects it owes commit together.
    const store =
      options.store?.(workspaceId) ??
      (directory === undefined
        ? memoryLayer()
        : sqliteLayer({ filename: join(directory, "view.sqlite") }));
    const runtime = ManagedRuntimeModule.make(
      applicationLayer({
        client: options.applicationClient?.(client, workspaceId) ?? client,
        protocol,
        gateway,
        store,
        config: AppConfigModule.layer({ deployment }),
        notifications: options.notifications,
      }),
    );
    const openedAtMs = now();
    return {
      workspaceId,
      adapter,
      client,
      gateway,
      runtime,
      openedAtMs,
      opens,
      requests: 0,
      inFlight: 0,
      lastRequestAtMs: openedAtMs,
      draining: false,
      delivery: {
        passes: 0,
        claimed: 0,
        delivered: 0,
        retried: 0,
        deadLettered: 0,
        failures: 0,
        lastError: null,
      },
      closing: undefined,
    };
  }

  /**
   * Give up the least recently used partition that nothing is currently using.
   *
   * A partition with requests in flight is never evicted: its runtime is what
   * those requests are running in.
   */
  function evictOne(): boolean {
    let victim: Partition | undefined;
    for (const partition of partitions.values()) {
      if (partition.inFlight > 0 || partition.closing !== undefined) continue;
      if (victim === undefined || partition.lastRequestAtMs < victim.lastRequestAtMs) {
        victim = partition;
      }
    }
    if (victim === undefined) return false;
    totals.evicted += 1;
    void closePartition(victim);
    return true;
  }

  /**
   * Dispose one partition, exactly once.
   *
   * The map entry is dropped first, so a request arriving mid-shutdown opens a
   * fresh partition rather than joining a dying one. Disposal order matters:
   * the runtime's finalizers close the store's connection, and only then is the
   * protocol client released.
   */
  function closePartition(partition: Partition): Promise<void> {
    if (partition.closing !== undefined) return partition.closing;
    if (partitions.get(partition.workspaceId) === partition) {
      partitions.delete(partition.workspaceId);
    }
    const closed = (async () => {
      await partition.runtime.dispose();
      await partition.client.close();
      totals.closed += 1;
    })();
    partition.closing = closed;
    return closed;
  }

  async function fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (closing !== undefined) {
      return hostFailureResponse(countFailure(new HostClosed({ pathname: url.pathname })));
    }

    const resolution = resolveRoute(url.pathname);
    if (resolution.kind === "host") {
      totals.hostRequests += 1;
      return json(resolution.route === "health" ? healthBody() : metricsBody());
    }
    if (resolution.kind === "sink-params") return invalidSinkParamsResponse(request);
    if (resolution.kind === "asset") {
      return options.fallback === undefined
        ? json({ error: "not-found", detail: url.pathname }, 404)
        : options.fallback(request);
    }
    if (resolution.kind === "failure") return hostFailureResponse(countFailure(resolution.failure));

    const partition = open(resolution.workspaceId);
    if (!isPartition(partition)) return hostFailureResponse(partition);

    totals.requests += 1;
    partition.requests += 1;
    partition.lastRequestAtMs = now();
    partition.inFlight += 1;
    try {
      return resolution.target === "streams"
        ? await partition.gateway.fetch(request)
        : await partition.runtime.runPromise(handle(request));
    } finally {
      partition.inFlight -= 1;
    }
  }

  /**
   * One delivery pass per open partition.
   *
   * Every pass is caught. A notifier that is down, a poisoned payload, even a
   * defect in a handler, is recorded against its own partition and reported;
   * it never reaches a command, a maintenance pass or another workspace. A
   * partition already draining is skipped rather than queued, so passes stay
   * serialized per lane exactly as the outbox expects.
   */
  async function drainDue(): Promise<readonly DeliveryPassReport[]> {
    const reports: DeliveryPassReport[] = [];
    for (const partition of Array.from(partitions.values())) {
      if (partition.closing !== undefined || partition.draining) continue;
      partition.draining = true;
      try {
        const report = await partition.runtime.runPromise(
          drainNotifications(partition.workspaceId),
        );
        partition.delivery.passes += 1;
        partition.delivery.claimed += report.claimed;
        partition.delivery.delivered += report.delivered;
        partition.delivery.retried += report.retried;
        partition.delivery.deadLettered += report.deadLettered;
        reports.push({
          workspaceId: partition.workspaceId,
          claimed: report.claimed,
          delivered: report.delivered,
          retried: report.retried,
          deadLettered: report.deadLettered,
          failed: false,
        });
      } catch (cause) {
        const detail = describeFailure(cause);
        partition.delivery.passes += 1;
        partition.delivery.failures += 1;
        partition.delivery.lastError = detail.slice(0, 500);
        reports.push({
          workspaceId: partition.workspaceId,
          claimed: 0,
          delivered: 0,
          retried: 0,
          deadLettered: 0,
          failed: true,
          detail: detail.slice(0, 500),
        });
      } finally {
        partition.draining = false;
      }
    }
    return reports;
  }

  /** Close every partition that has gone unused for the whole idle window. */
  async function sweepIdle(nowMs = now()): Promise<readonly string[]> {
    const stale = Array.from(partitions.values()).filter(
      (partition) =>
        partition.inFlight === 0 &&
        partition.closing === undefined &&
        nowMs - partition.lastRequestAtMs >= idleMillis,
    );
    totals.idled += stale.length;
    await Promise.all(stale.map(closePartition));
    return stale.map((partition) => partition.workspaceId);
  }

  /**
   * The managed tick a *running* host uses: deliver what is due, then give up
   * what has gone idle. Both halves are the same calls a manual caller makes,
   * so the timer adds scheduling and nothing else — which is why every
   * lifecycle assertion in the tests can drive them directly instead of waiting.
   * A tick never overlaps its predecessor.
   */
  let ticking = false;
  const tick = async (): Promise<void> => {
    if (ticking || closing !== undefined) return;
    ticking = true;
    try {
      await drainDue();
      await sweepIdle();
    } finally {
      ticking = false;
    }
  };

  const timer =
    deliveryMode === "interval"
      ? setInterval(() => {
          void tick();
        }, options.delivery?.intervalMs ?? DEFAULT_INTERVAL_MS)
      : undefined;
  // A managed tick is not a reason to keep the process alive.
  timer?.unref?.();

  function healthBody(): HostHealth {
    return {
      status: "ok",
      deployment,
      schemaVersion: SCHEMA_VERSION,
      view: issues.name,
      planHash: PLAN_HASH,
    };
  }

  function metricsBody(): HostMetrics {
    const workspaces = [...partitions.values()].map(
      (partition): PartitionMetrics => ({
        workspaceId: partition.workspaceId,
        opens: partition.opens,
        requests: partition.requests,
        inFlight: partition.inFlight,
        openedAtMs: partition.openedAtMs,
        lastRequestAtMs: partition.lastRequestAtMs,
        delivery: { ...partition.delivery },
      }),
    );
    return {
      deployment,
      planHash: PLAN_HASH,
      open: partitions.size,
      opened: totals.opened,
      closed: totals.closed,
      evicted: totals.evicted,
      idled: totals.idled,
      restarted: totals.restarted,
      requests: totals.requests,
      hostRequests: totals.hostRequests,
      failures: Object.fromEntries(failures),
      delivery: workspaces.reduce(
        (sum, partition) => ({
          passes: sum.passes + partition.delivery.passes,
          claimed: sum.claimed + partition.delivery.claimed,
          delivered: sum.delivered + partition.delivery.delivered,
          retried: sum.retried + partition.delivery.retried,
          deadLettered: sum.deadLettered + partition.delivery.deadLettered,
          failures: sum.failures + partition.delivery.failures,
        }),
        { passes: 0, claimed: 0, delivered: 0, retried: 0, deadLettered: 0, failures: 0 },
      ),
      workspaces,
    };
  }

  return {
    fetch,
    partition: open,
    openWorkspaces: () => [...partitions.keys()],
    restart: async (workspaceId: string) => {
      const partition = partitions.get(workspaceId);
      if (partition === undefined) return false;
      totals.restarted += 1;
      await closePartition(partition);
      return true;
    },
    sweepIdle,
    drainDue,
    metrics: metricsBody,
    close: () => {
      if (closing !== undefined) return closing;
      if (timer !== undefined) clearInterval(timer);
      closing = Promise.all([...partitions.values()].map(closePartition)).then(() => undefined);
      return closing;
    },
  };
}

/**
 * Where one partition's databases live.
 *
 * The id is already checked against the domain's identifier pattern before it
 * reaches here, which admits no separator and no traversal, so the workspace is
 * a single directory name by construction rather than by escaping. The rule is
 * exported because a partition's on-disk layout is what a restart test and an
 * operator both need to name, and neither should have to guess it.
 */
export function partitionPath(root: string, workspaceId: string): string {
  return join(root, "workspaces", workspaceId);
}

function partitionDirectory(root: string, workspaceId: string): string {
  const directory = partitionPath(root, workspaceId);
  mkdirSync(directory, { recursive: true });
  return directory;
}

/**
 * A rejected pass, as an operator reads it.
 *
 * An Effect fiber failure is an `Error` whose `message` is empty and whose
 * string form carries the typed cause, so the message alone would report
 * nothing at all.
 */
function describeFailure(cause: unknown): string {
  if (cause instanceof Error && cause.message.length > 0) return cause.message;
  return String(cause);
}

function isPartition(value: Partition | HostFailure): value is Partition {
  return !("_tag" in value);
}

function json(body: HostResponseBody, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}
