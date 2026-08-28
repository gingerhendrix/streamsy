/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise -- Worker handlers and test-only interleaving gates are Promise-native platform edges. */
/* oxlint-disable effecttsgo/global-date -- Alarm timestamps and request ids are created at the Cloudflare platform edge. */
/* oxlint-disable effecttsgo/crypto-random-uuid -- Request ids are generated at the stateless Worker platform edge. */
import { SqliteClient } from "@effect/sql-sqlite-do";
import { DurableObject } from "cloudflare:workers";
import type { DurableObjectStorage, ExecutionContext } from "@cloudflare/workers-types";
import {
  createHttpHandler,
  directProtocolClient,
  StreamProtocol,
  type JsonValue,
} from "@streamsy/core";
import { Effect, Layer, ManagedRuntime, Schema } from "effect";
import { partitionKeyString, parsePartitionKey, workspaceKey } from "../domain/domains.ts";
import { issues } from "../domain/declaration.ts";
import { advance } from "./maintenance.ts";
import { drainNotifications, listNotifications } from "./application.ts";
import type { ApplicationServices } from "./application.ts";
import { PLAN_HASH, SCHEMA_VERSION } from "./config.ts";
import * as AppConfigModule from "./config.ts";
import { createWorkspaceStreamStorage } from "./cloudflare-stream-storage.ts";
import type { WorkspaceStreamStorage } from "./cloudflare-stream-storage.ts";
import { cloudflareNotificationTargetLayer } from "./cloudflare-notifications.ts";
import {
  DomainPlacementUnavailable,
  PartitionUnavailable,
  hostFailureResponse,
} from "./host-errors.ts";
import { invalidSinkParamsResponse, resolveRoute } from "./host-routing.ts";
import { handle } from "./router.ts";
import { applicationLayer } from "./runtime.ts";
import { migratedSqlLayer } from "./store-sql.ts";
import type { StreamGateway } from "./gateway.ts";

const PARTITION_HEADER = "x-streamsy-partition-key";
const REQUEST_ID_HEADER = "x-request-id";
const TEST_FAILPOINT_HEADER = "x-streamsy-test-failpoint";
const ALARM_FLOOR_MS = 1;
const MAINTENANCE_GUARD_MS = 1_000;
const EXPIRY_BATCH_SIZE = 8;

export interface CloudflareEnv {
  readonly WORKSPACES: {
    idFromName(name: string): NonNullable<unknown>;
    get(id: NonNullable<unknown>): { fetch(request: Request): Promise<Response> };
  };
  readonly ASSETS?: { fetch(request: Request): Promise<Response> };
  readonly DEPLOYMENT?: string;
  readonly TEST_FAILPOINTS?: string;
}

const json = (body: JsonValue, status = 200, requestId?: string): Response => {
  const headers = new Headers({
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  if (requestId !== undefined) headers.set(REQUEST_ID_HEADER, requestId);
  return new Response(JSON.stringify(body), { status, headers });
};

const requestId = (request: Request): string =>
  request.headers.get(REQUEST_ID_HEADER) ?? crypto.randomUUID();

function forward(request: Request, key: string, id: string): Request {
  const headers = new Headers(request.headers);
  headers.set(PARTITION_HEADER, key);
  headers.set(REQUEST_ID_HEADER, id);
  return new Request(request, { headers });
}

/** Stateless route ownership edge. No application store exists in this Worker. */
export default {
  async fetch(request: Request, env: CloudflareEnv, _ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const id = requestId(request);
    const resolution = resolveRoute(url.pathname);
    if (resolution.kind === "host") {
      return resolution.route === "health"
        ? json(
            {
              status: "ok",
              deployment: env.DEPLOYMENT ?? "cloudflare-local",
              schemaVersion: SCHEMA_VERSION,
              view: issues.name,
              planHash: PLAN_HASH,
            },
            200,
            id,
          )
        : json(
            { deployment: env.DEPLOYMENT ?? "cloudflare-local", placement: "workspace" },
            200,
            id,
          );
    }
    if (resolution.kind === "sink-params") {
      const response = invalidSinkParamsResponse(request);
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
    }
    if (resolution.kind === "asset") {
      if (env.ASSETS === undefined)
        return json({ error: "not-found", detail: url.pathname }, 404, id);
      const response = await env.ASSETS.fetch(request);
      const mutable = new Response(response.body, response);
      mutable.headers.set(REQUEST_ID_HEADER, id);
      return mutable;
    }
    if (resolution.kind === "failure") {
      const response = hostFailureResponse(resolution.failure);
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
    }
    if (resolution.key.kind !== "workspace") {
      const response = hostFailureResponse(
        new DomainPlacementUnavailable({
          domain: resolution.key.kind,
          id: resolution.key.id,
        }),
      );
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
    }
    const key = partitionKeyString(resolution.key);
    try {
      return await env.WORKSPACES.get(env.WORKSPACES.idFromName(key)).fetch(
        forward(request, key, id),
      );
    } catch (cause) {
      const response = hostFailureResponse(
        new PartitionUnavailable({
          partition: key,
          detail: cause instanceof Error ? cause.message : String(cause),
        }),
      );
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
    }
  },
};

interface ActorRuntime {
  readonly workspaceId: string;
  readonly protocol: StreamProtocol;
  readonly streams: WorkspaceStreamStorage;
  readonly gateway: { readonly fetch: (request: Request) => Promise<Response> };
  readonly runtime: ManagedRuntime.ManagedRuntime<ApplicationServices | StreamGateway, never>;
}

const StoredPartition = Schema.Struct({
  kind: Schema.Literal("workspace"),
  id: Schema.String,
});

/** One SQLite-backed actor for one canonical workspace partition key. */
export class WorkspacePartitionObject extends DurableObject<CloudflareEnv> {
  private runtime: Promise<ActorRuntime> | undefined;
  private turn: Promise<void> = Promise.resolve();
  private alarmTurn: Promise<void> = Promise.resolve();
  private guardedOperations = 0;
  private interruptNotificationAfterAccept = false;
  private interruptAfterStreamCreateCommit = false;
  private pauseRenewalUntilExpiry = false;
  private failNextMaintenance = false;
  private applicationGate: TestGate | undefined;
  private expiryGate: TestGate | undefined;
  private maintenanceCompletionGate: TestGate | undefined;

  private initialize(canonicalKey: string): Promise<ActorRuntime> {
    if (this.runtime !== undefined) return this.runtime;
    const initializing = this.ctx.blockConcurrencyWhile(async () => {
      const decoded = Schema.decodeUnknownSync(StoredPartition)(parsePartitionKey(canonicalKey));
      const expected = partitionKeyString(workspaceKey(decoded.id));
      if (expected !== canonicalKey) throw new Error(`non-canonical workspace key ${canonicalKey}`);
      const stored = this.ctx.storage.kv.get<string>(PARTITION_HEADER);
      if (stored !== undefined && stored !== canonicalKey) {
        throw new Error(`object is already bound to ${stored}`);
      }
      if (stored === undefined) this.ctx.storage.kv.put(PARTITION_HEADER, canonicalKey);

      let protocol!: StreamProtocol;
      const streams = createWorkspaceStreamStorage(this.ctx.storage, {
        afterCreateCommit: () => this.afterStreamCreateCommit(),
        beforeAppendCommit: () => this.beforeStreamAppendCommit(),
        afterAppendCommit: () => this.afterStreamAppendCommit(),
      });
      protocol = new StreamProtocol({
        storage: { adapter: streams.adapter },
        longPollTimeoutMs: 5_000,
      });
      const client = directProtocolClient(protocol);
      const gateway = createHttpHandler({ protocol, pathPrefix: "/streams" });
      const sql = SqliteClient.layer({ storage: this.ctx.storage });
      const layer = applicationLayer({
        client,
        protocol,
        gateway,
        store: Layer.orDie(migratedSqlLayer).pipe(Layer.provide(sql)),
        config: AppConfigModule.layer({ deployment: this.env.DEPLOYMENT ?? "cloudflare-local" }),
        notificationTarget: cloudflareNotificationTargetLayer(this.ctx.storage, () =>
          this.consumeNotificationInterrupt(),
        ),
      }).pipe(Layer.orDie);
      const runtime = ManagedRuntime.make(layer);
      // Building the runtime acquires the one shared DO client and completes all
      // migrations before a triggering request can resolve an application store.
      await runtime.runPromise(Effect.void);
      return { workspaceId: decoded.id, protocol, streams, gateway, runtime };
    });
    this.runtime = initializing;
    return initializing;
  }

  private async current(request?: Request): Promise<ActorRuntime> {
    const requested = request?.headers.get(PARTITION_HEADER) ?? undefined;
    const stored = this.ctx.storage.kv.get<string>(PARTITION_HEADER);
    const key = requested ?? stored;
    if (key === null || key === undefined) throw new Error("missing workspace partition identity");
    if (requested !== undefined && stored !== undefined && requested !== stored) {
      throw new Error(`request for ${requested} reached object bound to ${stored}`);
    }
    return this.initialize(key);
  }

  async fetch(request: Request): Promise<Response> {
    const id = requestId(request);
    try {
      if (new URL(request.url).pathname === "/_streamsy/maintenance") {
        const actor = await this.current(request);
        if (this.testFailpoint(request, "pause-maintenance-after-reconcile")) {
          this.maintenanceCompletionGate = makeTestGate();
        }
        const report = await this.runMaintenance();
        return json({ ...report, workspaceId: actor.workspaceId }, 200, id);
      }
      const actor = await this.current(request);
      const resolution = resolveRoute(new URL(request.url).pathname);
      if (
        resolution.kind !== "partition" ||
        resolution.key.kind !== "workspace" ||
        resolution.key.id !== actor.workspaceId
      ) {
        return json({ error: "wrong-workspace-object", detail: actor.workspaceId }, 409, id);
      }
      const headers = new Headers(request.headers);
      headers.delete(PARTITION_HEADER);
      headers.delete(TEST_FAILPOINT_HEADER);
      const clean = new Request(request, { headers });
      if (resolution.target === "streams") {
        await this.beginGuardedOperation();
        try {
          if (this.testFailpoint(request, "pause-next-expiry")) {
            this.expiryGate = makeTestGate();
          }
          if (this.testFailpoint(request, "pause-next-maintenance-after-reconcile")) {
            this.maintenanceCompletionGate = makeTestGate();
            this.ctx.storage.sql.exec(
              "CREATE TABLE IF NOT EXISTS issue_tracker_test_events (name TEXT PRIMARY KEY)",
            );
          }
          this.pauseRenewalUntilExpiry = this.testFailpoint(request, "pause-renewal-until-expiry");
          this.interruptAfterStreamCreateCommit = this.testFailpoint(
            request,
            "after-stream-create-commit",
          );
          const response = await actor.gateway.fetch(clean);
          response.headers.set(REQUEST_ID_HEADER, id);
          await this.finishGuardedOperation(actor);
          if (this.testFailpoint(request, "release-paused-application")) {
            const gate = this.applicationGate ?? (this.applicationGate = makeTestGate());
            await gate.reached;
            gate.release();
          }
          if (this.testFailpoint(request, "release-maintenance-completion")) {
            const gate =
              this.maintenanceCompletionGate ?? (this.maintenanceCompletionGate = makeTestGate());
            await gate.reached;
            gate.release();
          }
          return response;
        } catch (cause) {
          await this.abandonGuardedOperation();
          throw cause;
        } finally {
          this.interruptAfterStreamCreateCommit = false;
          this.pauseRenewalUntilExpiry = false;
        }
      }
      return await this.serialized(async () => {
        await this.beginGuardedOperation();
        try {
          if (this.testFailpoint(request, "pause-after-prearm-then-fail")) {
            const gate = this.applicationGate ?? (this.applicationGate = makeTestGate());
            gate.arrive();
            await gate.released;
          }
          this.interruptNotificationAfterAccept = this.testFailpoint(
            request,
            "notification-after-accept",
          );
          if (this.testFailpoint(request, "after-application-commit-with-maintenance-failure")) {
            this.failNextMaintenance = true;
            this.ctx.storage.sql.exec(
              "CREATE TABLE IF NOT EXISTS issue_tracker_test_events (name TEXT PRIMARY KEY)",
            );
          }
          const response = await actor.runtime.runPromise(handle(clean));
          response.headers.set(REQUEST_ID_HEADER, id);
          if (this.testFailpoint(request, "notification-after-accept") && response.status === 499) {
            throw new Error("injected interruption after notification acceptance");
          }
          if (
            this.testFailpoint(request, "after-application-commit") ||
            this.testFailpoint(request, "pause-after-prearm-then-fail") ||
            this.testFailpoint(request, "after-application-commit-with-maintenance-failure")
          ) {
            throw new Error("injected failure after application commit");
          }
          await this.finishGuardedOperation(actor);
          return response;
        } catch (cause) {
          await this.abandonGuardedOperation();
          throw cause;
        } finally {
          this.interruptNotificationAfterAccept = false;
        }
      });
    } catch (cause) {
      const key = request.headers.get(PARTITION_HEADER) ?? "workspace:unknown";
      const response = hostFailureResponse(
        new PartitionUnavailable({
          partition: key,
          detail: cause instanceof Error ? cause.message : String(cause),
        }),
      );
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
    }
  }

  async alarm(): Promise<void> {
    await this.runMaintenance();
  }

  /** Idempotent maintenance RPC; the alarm handler and local harness share it. */
  private async runMaintenance(): Promise<{ workspaceId: string; delivered: number }> {
    return await this.serialized(async () => {
      await this.beginGuardedOperation(true);
      try {
        const actor = await this.current();
        if (this.failNextMaintenance) {
          this.failNextMaintenance = false;
          this.ctx.storage.sql.exec(
            "INSERT OR REPLACE INTO issue_tracker_test_events(name) VALUES ('maintenance-failed')",
          );
          throw new Error("injected alarm maintenance body failure");
        }
        const now = Date.now();
        const due = actor.streams.dueExpiryStreamIds(now, EXPIRY_BATCH_SIZE);
        if (due.length > 0 && this.expiryGate !== undefined) {
          const gate = this.expiryGate;
          gate.arrive();
          await gate.released;
          this.expiryGate = undefined;
        }
        for (const streamId of due) {
          actor.streams.expireStreamIfDue(streamId, now);
        }
        // Expiry is deliberately bounded; notification/view work always gets a
        // fair share of this alarm turn before any remaining due batch.
        await actor.runtime.runPromise(advance(actor.workspaceId));
        const delivery = await actor.runtime.runPromise(drainNotifications(actor.workspaceId));
        await this.finishGuardedOperation(actor);
        if (this.maintenanceCompletionGate !== undefined) {
          const gate = this.maintenanceCompletionGate;
          this.ctx.storage.sql.exec(
            "INSERT OR REPLACE INTO issue_tracker_test_events(name) VALUES ('maintenance-paused')",
          );
          gate.arrive();
          await gate.released;
          this.maintenanceCompletionGate = undefined;
        }
        return { workspaceId: actor.workspaceId, delivered: delivery.delivered };
      } catch (cause) {
        await this.abandonGuardedOperation();
        throw cause;
      }
    });
  }

  private async reconcileOwedWork(actor: ActorRuntime): Promise<void> {
    const listed = await actor.runtime.runPromise(listNotifications(actor.workspaceId));
    const nextNotification = listed.entries
      .filter((entry) => entry.state === "pending")
      .reduce<number | undefined>(
        (earliest, entry) =>
          earliest === undefined
            ? entry.nextAttemptAtMs
            : Math.min(earliest, entry.nextAttemptAtMs),
        undefined,
      );
    const nextExpiry = actor.streams.nextExpiryAt();
    const next =
      nextNotification === undefined
        ? nextExpiry
        : nextExpiry === undefined
          ? nextNotification
          : Math.min(nextNotification, nextExpiry);
    if (this.guardedOperations > 0) {
      const guard = Date.now() + MAINTENANCE_GUARD_MS;
      const desired = next === undefined ? guard : Math.min(guard, next);
      const current = await this.ctx.storage.getAlarm();
      if (current === null || current > desired) await this.ctx.storage.setAlarm(desired);
      return;
    }
    if (next !== undefined)
      await this.ctx.storage.setAlarm(Math.max(Date.now() + ALARM_FLOOR_MS, next));
    else await this.ctx.storage.deleteAlarm();
  }

  private async beginGuardedOperation(replaceFiredAlarm = false): Promise<void> {
    await this.serializedAlarm(async () => {
      this.guardedOperations += 1;
      if (replaceFiredAlarm) {
        await this.ctx.storage.setAlarm(Date.now() + MAINTENANCE_GUARD_MS);
        return;
      }
      await this.preserveOrArmGuard();
    });
  }

  private async finishGuardedOperation(actor: ActorRuntime): Promise<void> {
    await this.serializedAlarm(async () => {
      this.guardedOperations -= 1;
      await this.reconcileOwedWork(actor);
    });
  }

  private async abandonGuardedOperation(): Promise<void> {
    await this.serializedAlarm(async () => {
      this.guardedOperations -= 1;
      await this.preserveOrArmGuard();
    });
  }

  private async preserveOrArmGuard(): Promise<void> {
    const guard = Date.now() + MAINTENANCE_GUARD_MS;
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > guard) await this.ctx.storage.setAlarm(guard);
  }

  private async afterStreamCreateCommit(): Promise<void> {
    if (!this.interruptAfterStreamCreateCommit) return;
    this.interruptAfterStreamCreateCommit = false;
    throw new Error("injected interruption after atomic stream create commit");
  }

  private async beforeStreamAppendCommit(): Promise<void> {
    if (!this.pauseRenewalUntilExpiry) return;
    const gate = this.expiryGate ?? (this.expiryGate = makeTestGate());
    await gate.reached;
  }

  private async afterStreamAppendCommit(): Promise<void> {
    if (this.pauseRenewalUntilExpiry) this.expiryGate?.release();
  }

  private testFailpoint(request: Request, name: string): boolean {
    return (
      this.env.TEST_FAILPOINTS === "enabled" && request.headers.get(TEST_FAILPOINT_HEADER) === name
    );
  }

  private consumeNotificationInterrupt(): boolean {
    if (!this.interruptNotificationAfterAccept) return false;
    this.interruptNotificationAfterAccept = false;
    return true;
  }

  private serialized<A>(work: () => Promise<A>): Promise<A> {
    const result = this.turn.then(work);
    this.turn = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private serializedAlarm<A>(work: () => Promise<A>): Promise<A> {
    const result = this.alarmTurn.then(work);
    this.alarmTurn = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

interface TestGate {
  readonly reached: Promise<void>;
  readonly released: Promise<void>;
  readonly arrive: () => void;
  readonly release: () => void;
}

function makeTestGate(): TestGate {
  let arrive!: () => void;
  let release!: () => void;
  return {
    reached: new Promise<void>((resolve) => {
      arrive = resolve;
    }),
    released: new Promise<void>((resolve) => {
      release = resolve;
    }),
    arrive: () => arrive(),
    release: () => release(),
  };
}

export type WorkspaceObjectStorage = DurableObjectStorage;
