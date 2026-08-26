/**
 * The keyed multi-domain host.
 *
 * One process, many partitions, one per *domain identity*. B3 keyed this host
 * by a workspace id because a workspace was the only thing a partition could
 * be. B4 keys it by a {@link PartitionKey} — a domain kind and an id — so a
 * workspace, a user and the singleton global partition are three instances of
 * one rule rather than one implementation and two special cases. Every
 * workspace route, its on-disk layout and its isolation argument are unchanged;
 * what changed is that "workspace" is now a value the host is given rather
 * than an assumption baked into it.
 *
 * A partition owns everything its domain needs and shares none of it. A
 * workspace partition owns durable stream storage, a protocol client and
 * gateway, a maintained-state store, an outbox and a `ManagedRuntime`. A user
 * partition owns one inbox. The global partition owns the exchange cursors.
 * Routing decides *which* partition before any application code runs, so a
 * request for workspace A cannot read or write B's rows — and no partition's
 * runtime can resolve another partition's services at all, because they are
 * different service instances behind different layers over different storage.
 *
 * Four lifecycle facts follow and are the whole operational model:
 *
 * - **Partitions open lazily.** The first request for a key builds it.
 * - **Partitions close independently.** Restarting, evicting or idling one
 *   disposes exactly its runtime and its connections.
 * - **Closing is idempotent.** A partition is disposed exactly once however
 *   many times it is asked.
 * - **A leased partition is not reclaimable.** The exchange holds leases while
 *   it moves records between two partitions, and neither eviction nor the idle
 *   sweep will close a partition that is leased — its runtime is what the pass
 *   is running in.
 *
 * Time is injected (`now`), and idle sweeping, effect delivery and the
 * exchange are all explicit calls, so lifecycle is tested by driving it rather
 * than by waiting for a clock.
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
import {
  globalKey,
  partitionKeyString,
  partitionSegments,
  userKey,
  workspaceKey,
  type GlobalKey,
  type PartitionKey,
  type UserKey,
  type WorkspaceKey,
} from "../domain/domains.ts";
import * as AppConfigModule from "./config.ts";
import { PLAN_HASH, SCHEMA_VERSION } from "./config.ts";
import { drainNotifications } from "./application.ts";
import type { ApplicationServices } from "./application.ts";
import {
  runExchange,
  type ExchangePassOptions,
  type ExchangePassReport,
  type ExchangeSession,
  type PartitionLease,
} from "./exchange.ts";

import type { StreamGateway } from "./gateway.ts";
import { globalLayer, handleGlobalRequest, type GlobalServices } from "./global-domain.ts";
import {
  hostFailureResponse,
  HostClosed,
  PartitionLimitReached,
  PartitionUnavailable,
  type HostFailure,
} from "./host-errors.ts";
import { invalidSinkParamsResponse, resolveRoute } from "./host-routing.ts";
import type { InboxStore } from "./inbox-store.ts";
import type { NotificationTargetOptions } from "./notifications.ts";
import { handle } from "./router.ts";
import { applicationLayer } from "./runtime.ts";
import { memoryLayer, type IssueStore } from "./store.ts";
import { sqliteLayer } from "./store-sqlite.ts";
import { handleUserRequest, userLayer, type UserServices } from "./user-domain.ts";

/** How many partitions may be open at once, and when an idle one is given up. */
export interface PartitionPolicy {
  /** Upper bound on simultaneously open partitions, across every domain. */
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

/** How the host drives the cross-domain exchange. Manual for the same reason. */
export interface ExchangePolicy {
  readonly mode?: "manual" | "interval";
  /** Upper bound on records read from one source in one pass. */
  readonly limit?: number;
  /** How many closed registered sources one pass may reopen. Zero disables it. */
  readonly coldSources?: number;
}

export interface WorkspaceHostOptions {
  /** Put every partition's durable state under this directory. */
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
  /** Per-user-partition inbox storage. A factory, for the same reason. */
  readonly inbox?: (userId: string) => Layer.Layer<InboxStore>;
  /** The global partition's exchange cursors and source registry. */
  readonly exchangeStore?: () => Layer.Layer<GlobalServices>;
  readonly notifications?: NotificationTargetOptions;
  /** Test/host adapter seam for transport fault injection, per partition. */
  readonly applicationClient?: (
    client: StreamProtocolClient,
    workspaceId: string,
  ) => StreamProtocolClient;
  readonly partitions?: PartitionPolicy;
  readonly delivery?: DeliveryPolicy;
  readonly exchange?: ExchangePolicy;
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
  /** The domain identity this partition serves. */
  readonly kind: PartitionKey["kind"];
  readonly id: string;
  /**
   * The workspace this partition serves.
   *
   * Present only on workspace partitions, so the B3 shape of this record is
   * unchanged for the domain it described.
   */
  readonly workspaceId?: string;
  /** How many times this key has been opened, including after a restart. */
  readonly opens: number;
  readonly requests: number;
  readonly inFlight: number;
  /** Leases held right now. A partition with any is neither evictable nor sweepable. */
  readonly leases: number;
  readonly openedAtMs: number;
  readonly lastRequestAtMs: number;
  readonly delivery: PartitionDeliveryMetrics;
}

export interface ExchangeMetrics {
  readonly passes: number;
  readonly scanned: number;
  readonly applied: number;
  readonly failures: number;
  readonly lastError: string | null;
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
  readonly exchange: ExchangeMetrics;
  /** Every open partition, in every domain. */
  readonly partitions: readonly PartitionMetrics[];
  /** The workspace partitions only, as B3 reported them. */
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

/** The mutable form of {@link ExchangeMetrics}, owned by the host. */
interface ExchangeCounters {
  passes: number;
  scanned: number;
  applied: number;
  failures: number;
  lastError: string | null;
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

/** What every partition has, whatever domain it serves. */
interface PartitionCommon {
  readonly key: PartitionKey;
  readonly openedAtMs: number;
  opens: number;
  requests: number;
  inFlight: number;
  /** Outstanding leases. Nonzero means a host-level pass is using this runtime. */
  leases: number;
  lastRequestAtMs: number;
  draining: boolean;
  delivery: DeliveryCounters;
  closing: Promise<void> | undefined;
  /** Release this partition's resources. Called once, by `closePartition`. */
  readonly dispose: () => Promise<void>;
}

/** One workspace's isolated runtime and the durable resources it owns. */
export interface WorkspacePartition extends PartitionCommon {
  readonly kind: "workspace";
  readonly workspaceId: string;
  readonly adapter: StorageAdapter;
  readonly client: StreamProtocolClient;
  readonly gateway: { readonly fetch: (request: Request) => Promise<Response> };
  readonly runtime: ManagedRuntime.ManagedRuntime<ApplicationServices | StreamGateway, never>;
}

/** One user's isolated runtime: an inbox and nothing else. */
export interface UserPartition extends PartitionCommon {
  readonly kind: "user";
  readonly userId: string;
  readonly runtime: ManagedRuntime.ManagedRuntime<UserServices, never>;
}

/** The singleton global partition: the exchange's cursors. */
export interface GlobalPartition extends PartitionCommon {
  readonly kind: "global";
  readonly runtime: ManagedRuntime.ManagedRuntime<GlobalServices, never>;
}

export type DomainPartition = WorkspacePartition | UserPartition | GlobalPartition;

/**
 * The workspace partition, under the name B3 gave it.
 *
 * Kept because a workspace partition is still what `createLocalHost` and every
 * pre-B4 caller means by "the partition", and renaming it would have churned
 * call sites without changing a single behaviour.
 */
export type Partition = WorkspacePartition;

/** A partition held open by an operator or a host-level pass. */
export interface HostLease {
  readonly key: PartitionKey;
  readonly release: () => void;
}

export interface WorkspaceHost {
  readonly fetch: (request: Request) => Promise<Response>;
  /**
   * Open (or reuse) one partition. The typed failure is a value, never a throw.
   *
   * The key's domain decides the partition's shape, and the overloads say so:
   * asking for a user key cannot hand back a workspace partition.
   */
  readonly partition: {
    (key: WorkspaceKey): WorkspacePartition | HostFailure;
    (key: UserKey): UserPartition | HostFailure;
    (key: GlobalKey): GlobalPartition | HostFailure;
    (key: PartitionKey): DomainPartition | HostFailure;
  };
  /** The ids of every currently open workspace partition. */
  readonly openWorkspaces: () => readonly string[];
  /** The keys of every currently open partition, in every domain. */
  readonly openPartitions: () => readonly PartitionKey[];
  /** Dispose one partition. It rebuilds from durable state on its next request. */
  readonly restart: (key: PartitionKey) => Promise<boolean>;
  /**
   * Hold one partition open for a host-level pass.
   *
   * A leased partition is neither evictable nor sweepable. The lease must be
   * released; the exchange does so in a `finally`. The public handle carries no
   * way to run in the partition — that capability belongs to the exchange, which
   * takes its leases through its own typed session.
   */
  readonly lease: (key: PartitionKey) => HostLease | HostFailure;
  /** Close every unleased partition unused for at least `idleMillis`. */
  readonly sweepIdle: (nowMs?: number) => Promise<readonly PartitionKey[]>;
  /** One delivery pass over every open workspace partition. Failures are isolated. */
  readonly drainDue: () => Promise<readonly DeliveryPassReport[]>;
  /** One exchange pass over every open source partition. Failures are isolated. */
  readonly exchange: () => Promise<readonly ExchangePassReport[]>;
  readonly metrics: () => HostMetrics;
  readonly close: () => Promise<void>;
}

export function createWorkspaceHost(options: WorkspaceHostOptions = {}): WorkspaceHost {
  const now = options.now ?? (() => Date.now());
  const maxOpen = options.partitions?.maxOpen ?? DEFAULT_MAX_OPEN;
  const idleMillis = options.partitions?.idleMillis ?? DEFAULT_IDLE_MILLIS;
  const deployment = options.deployment ?? "local";
  const deliveryMode = options.delivery?.mode ?? "manual";
  const exchangeMode = options.exchange?.mode ?? "manual";

  const partitions = new Map<string, DomainPartition>();
  /** Survives a close, so `opens` still counts a key that was restarted. */
  const openCounts = new Map<string, number>();
  /**
   * Every workspace this process has opened, whether or not it is open now.
   *
   * The exchange registers from this rather than from the open set: a workspace
   * that was idled out before the first pass ran is still a source, and losing
   * it would make an inbox's completeness depend on request timing.
   */
  const knownWorkspaces = new Set<string>();
  const totals = {
    opened: 0,
    closed: 0,
    evicted: 0,
    idled: 0,
    restarted: 0,
    requests: 0,
    hostRequests: 0,
  };
  const exchangeCounters: ExchangeCounters = {
    passes: 0,
    scanned: 0,
    applied: 0,
    failures: 0,
    lastError: null,
  };
  const failures = new Map<string, number>();
  let closing: Promise<void> | undefined;

  const countFailure = (failure: HostFailure): HostFailure => {
    failures.set(failure._tag, (failures.get(failure._tag) ?? 0) + 1);
    return failure;
  };

  function open(key: PartitionKey): DomainPartition | HostFailure {
    const id = partitionKeyString(key);
    const existing = partitions.get(id);
    if (existing !== undefined && existing.closing === undefined) return existing;
    if (closing !== undefined) return countFailure(new HostClosed({ pathname: id }));
    if (partitions.size >= maxOpen && !evictOne()) {
      return countFailure(new PartitionLimitReached({ partition: id, maxOpen }));
    }
    try {
      const opens = (openCounts.get(id) ?? 0) + 1;
      openCounts.set(id, opens);
      const created = build(key, opens);
      if (key.kind === "workspace") knownWorkspaces.add(key.id);
      partitions.set(id, created);
      totals.opened += 1;
      return created;
    } catch (cause) {
      return countFailure(
        new PartitionUnavailable({
          partition: id,
          detail: cause instanceof Error ? cause.message : String(cause),
        }),
      );
    }
  }

  /** One partition, built for the domain its key names. */
  function build(key: PartitionKey, opens: number): DomainPartition {
    const directory =
      options.databaseDirectory === undefined
        ? undefined
        : partitionDirectory(options.databaseDirectory, key);
    const common = {
      key,
      openedAtMs: now(),
      opens,
      requests: 0,
      inFlight: 0,
      leases: 0,
      lastRequestAtMs: now(),
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

    if (key.kind === "user") {
      const runtime = ManagedRuntimeModule.make(
        options.inbox?.(key.id) ??
          userLayer(directory === undefined ? {} : { filename: join(directory, "inbox.sqlite") }),
      );
      return {
        ...common,
        kind: "user",
        userId: key.id,
        runtime,
        dispose: () => runtime.dispose(),
      };
    }

    if (key.kind === "global") {
      const runtime = ManagedRuntimeModule.make(
        options.exchangeStore?.() ??
          globalLayer(
            directory === undefined ? {} : { filename: join(directory, "exchange.sqlite") },
          ),
      );
      return {
        ...common,
        kind: "global",
        runtime,
        dispose: () => runtime.dispose(),
      };
    }

    const workspaceId = key.id;
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
    return {
      ...common,
      kind: "workspace",
      workspaceId,
      adapter,
      client,
      gateway,
      runtime,
      // Disposal order matters: the runtime's finalizers close the store's
      // connection, and only then is the protocol client released.
      dispose: async () => {
        await runtime.dispose();
        await client.close();
      },
    };
  }

  /**
   * Give up the least recently used partition that nothing is currently using.
   *
   * A partition with requests in flight or leases held is never evicted: its
   * runtime is what those requests, or that exchange pass, are running in.
   */
  function evictOne(): boolean {
    let victim: DomainPartition | undefined;
    for (const partition of partitions.values()) {
      if (!reclaimable(partition)) continue;
      if (victim === undefined || partition.lastRequestAtMs < victim.lastRequestAtMs) {
        victim = partition;
      }
    }
    if (victim === undefined) return false;
    totals.evicted += 1;
    void closePartition(victim);
    return true;
  }

  const reclaimable = (partition: DomainPartition): boolean =>
    partition.inFlight === 0 && partition.leases === 0 && partition.closing === undefined;

  /**
   * Dispose one partition, exactly once.
   *
   * The map entry is dropped first, so a request arriving mid-shutdown opens a
   * fresh partition rather than joining a dying one.
   */
  function closePartition(partition: DomainPartition): Promise<void> {
    if (partition.closing !== undefined) return partition.closing;
    const id = partitionKeyString(partition.key);
    if (partitions.get(id) === partition) partitions.delete(id);
    const closed = (async () => {
      await partition.dispose();
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

    const partition = open(resolution.key);
    if (!isPartition(partition)) return hostFailureResponse(partition);

    totals.requests += 1;
    partition.requests += 1;
    partition.lastRequestAtMs = now();
    partition.inFlight += 1;
    try {
      if (partition.kind === "user") {
        return await partition.runtime.runPromise(handleUserRequest(request));
      }
      if (partition.kind === "global") {
        return await partition.runtime.runPromise(handleGlobalRequest(request));
      }
      return resolution.target === "streams"
        ? await partition.gateway.fetch(request)
        : await partition.runtime.runPromise(handle(request));
    } finally {
      partition.inFlight -= 1;
    }
  }

  /**
   * One delivery pass per open workspace partition.
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
      if (partition.kind !== "workspace") continue;
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

  /**
   * Take a lease on one partition.
   *
   * The lease increments a counter the reclamation policy reads, and the
   * returned handle is the only way to run in that partition. `release` is
   * idempotent, so a caller releasing twice cannot un-pin a partition somebody
   * else is using.
   */
  function takeLease<R>(key: PartitionKey): PartitionLease<R> | HostFailure {
    const partition = open(key);
    if (!isPartition(partition)) return partition;
    partition.leases += 1;
    partition.lastRequestAtMs = now();
    let released = false;
    // SAFETY: `key.kind` chose which runtime `build` made, and the typed
    // accessors below pass the matching `R` for that kind. This one erasure is
    // what lets a single lease implementation serve three differently-shaped
    // runtimes without three copies of the counter and the release guard.
    // oxlint-disable-next-line anti-slop/no-chained-type-assertions, anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
    const runtime = partition.runtime as ManagedRuntime.ManagedRuntime<R, never>;
    return {
      key,
      runPromise: (effect) => runtime.runPromise(effect),
      release: () => {
        if (released) return;
        released = true;
        partition.leases -= 1;
      },
    };
  }

  /**
   * The exchange's view of this host.
   *
   * Sources are the workspace partitions currently open. A workspace that has
   * been idled out is not exchanged until something opens it again, which is
   * the same laziness every other partition operation has: the host does not
   * reopen storage on a timer to look for work.
   */
  const session: ExchangeSession = {
    now,
    openSources: () =>
      [...partitions.values()]
        .filter((partition) => partition.kind === "workspace" && partition.closing === undefined)
        .map((partition) => partition.key),
    knownSources: () => [...knownWorkspaces].map(workspaceKey),
    leaseWorkspace: (workspaceId) =>
      takeLease<ApplicationServices | StreamGateway>(workspaceKey(workspaceId)),
    leaseUser: (userId) => takeLease<UserServices>(userKey(userId)),
    leaseGlobal: () => takeLease<GlobalServices>(globalKey()),
  };

  /** One exchange pass over every open source. Reports are counted, never thrown. */
  async function exchange(): Promise<readonly ExchangePassReport[]> {
    if (closing !== undefined) return [];
    const passOptions: ExchangePassOptions = {};
    if (options.exchange?.limit !== undefined) passOptions.limit = options.exchange.limit;
    if (options.exchange?.coldSources !== undefined) {
      passOptions.coldSources = options.exchange.coldSources;
    }
    const reports = await runExchange(session, passOptions);
    for (const report of reports) {
      exchangeCounters.passes += 1;
      exchangeCounters.scanned += report.scanned;
      exchangeCounters.applied += report.applied;
      if (report.failed) {
        exchangeCounters.failures += 1;
        exchangeCounters.lastError = (report.detail ?? "exchange pass failed").slice(0, 500);
      }
    }
    return reports;
  }

  /** Close every partition that has gone unused for the whole idle window. */
  async function sweepIdle(nowMs = now()): Promise<readonly PartitionKey[]> {
    const stale = Array.from(partitions.values()).filter(
      (partition) => reclaimable(partition) && nowMs - partition.lastRequestAtMs >= idleMillis,
    );
    totals.idled += stale.length;
    await Promise.all(stale.map(closePartition));
    return stale.map((partition) => partition.key);
  }

  /**
   * The managed tick a *running* host uses: deliver what is due, move what the
   * exchange owes, then give up what has gone idle. Every half is the same
   * call a manual caller makes, so the timer adds scheduling and nothing else —
   * which is why every lifecycle assertion in the tests can drive them directly
   * instead of waiting. A tick never overlaps its predecessor, and the sweep
   * runs last so the exchange's leases are already released.
   */
  let ticking = false;
  const tick = async (): Promise<void> => {
    if (ticking || closing !== undefined) return;
    ticking = true;
    try {
      await drainDue();
      if (exchangeMode === "interval") await exchange();
      await sweepIdle();
    } finally {
      ticking = false;
    }
  };

  const timer =
    deliveryMode === "interval" || exchangeMode === "interval"
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

  function partitionMetrics(partition: DomainPartition): PartitionMetrics {
    const base = {
      kind: partition.key.kind,
      id: partition.key.id,
      opens: partition.opens,
      requests: partition.requests,
      inFlight: partition.inFlight,
      leases: partition.leases,
      openedAtMs: partition.openedAtMs,
      lastRequestAtMs: partition.lastRequestAtMs,
      delivery: { ...partition.delivery },
    };
    return partition.kind === "workspace" ? { ...base, workspaceId: partition.workspaceId } : base;
  }

  function metricsBody(): HostMetrics {
    const all = [...partitions.values()].map(partitionMetrics);
    const workspaces = all.filter((entry) => entry.kind === "workspace");
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
      exchange: { ...exchangeCounters },
      partitions: all,
      workspaces,
    };
  }

  // SAFETY: `build` chooses a partition's shape from `key.kind`, so `open`
  // already returns the domain's own partition for the key it was given. The
  // assertion re-states that in the type system as the overload set, which one
  // implementation cannot express directly.
  // oxlint-disable-next-line anti-slop/no-chained-type-assertions, anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
  const partition = ((key: PartitionKey) => open(key)) as WorkspaceHost["partition"];

  return {
    fetch,
    partition,
    openWorkspaces: () =>
      [...partitions.values()]
        .filter((entry) => entry.kind === "workspace")
        .map((entry) => entry.key.id),
    openPartitions: () => [...partitions.values()].map((entry) => entry.key),
    restart: async (key: PartitionKey) => {
      const found = partitions.get(partitionKeyString(key));
      if (found === undefined) return false;
      totals.restarted += 1;
      await closePartition(found);
      return true;
    },
    lease: (key: PartitionKey) => {
      const taken = takeLease<never>(key);
      return "_tag" in taken ? taken : { key: taken.key, release: taken.release };
    },
    sweepIdle,
    drainDue,
    exchange,
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
 * reaches here, which admits no separator and no traversal, so the partition is
 * two directory names by construction rather than by escaping. A workspace key
 * still names `workspaces/<id>`, so a B3 data directory is read back unchanged.
 * The rule is exported because a partition's on-disk layout is what a restart
 * test and an operator both need to name, and neither should have to guess it.
 */
export function partitionPath(root: string, key: PartitionKey): string {
  return join(root, ...partitionSegments(key));
}

function partitionDirectory(root: string, key: PartitionKey): string {
  const directory = partitionPath(root, key);
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

function isPartition(value: DomainPartition | HostFailure): value is DomainPartition {
  return !("_tag" in value);
}

function json(body: HostResponseBody, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}
