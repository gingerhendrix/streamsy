/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise -- Worker handlers and test-only interleaving gates are Promise-native platform edges. */
/* oxlint-disable effecttsgo/global-date -- Alarm timestamps and request ids are created at the Cloudflare platform edge. */
/* oxlint-disable effecttsgo/crypto-random-uuid -- Request ids are generated at the stateless Worker platform edge. */
import { SqliteClient } from "@effect/sql-sqlite-do";
import { DurableObject } from "cloudflare:workers";
import type {
  AlarmInvocationInfo,
  DurableObjectStorage,
  ExecutionContext,
} from "@cloudflare/workers-types";
import type { IssueTrackerCloudflareEnv } from "../alchemy.run.ts";
import {
  createHttpHandler,
  directProtocolClient,
  StreamProtocol,
  type JsonValue,
} from "@streamsy/core";
import { Effect, Layer, ManagedRuntime, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { globalKey, partitionKeyString, parsePartitionKey, userKey, workspaceKey } from "../domain/domains.ts";
import {
  ApplyInboxBatchRequest,
  ApplyInboxBatchResult,
  EXCHANGE_NAME,
  EXCHANGE_VERSION,
  ReadAssignmentPageRequest,
  ReadAssignmentPageResult,
  RegisterSourceRequest,
  stableHash,
} from "../domain/exchange-rpc.ts";
import { assignmentInbox } from "../domain/exchange.ts";
import type { InboxRow } from "../domain/inbox.ts";
import { issues } from "../domain/declaration.ts";
import { advance } from "./maintenance.ts";
import { drainNotifications, listNotifications } from "./application.ts";
import type { ApplicationServices } from "./application.ts";
import { PLAN_HASH, SCHEMA_VERSION } from "./config.ts";
import * as AppConfigModule from "./config.ts";
import { createWorkspaceStreamStorage } from "./cloudflare-stream-storage.ts";
import type { WorkspaceStreamStorage } from "./cloudflare-stream-storage.ts";
import { cloudflareNotificationTargetLayer } from "./cloudflare-notifications.ts";
import { PartitionUnavailable, hostFailureResponse } from "./host-errors.ts";
import { invalidSinkParamsResponse, resolveRoute } from "./host-routing.ts";
import { handle } from "./router.ts";
import { applicationLayer } from "./runtime.ts";
import { migratedSqlLayer } from "./store-sql.ts";
import type { StreamGateway } from "./gateway.ts";
import { readAssignmentActivity, EXCHANGE_SOURCE_PAGE_LIMIT } from "./exchange-source.ts";
import { InboxStore, migratedInboxSqlLayer } from "./inbox-store.ts";
import { handleUserRequest, type UserServices } from "./user-domain.ts";
import { handleGlobalRequest, migratedGlobalSqlLayer, type GlobalServices } from "./global-domain.ts";
import { ExchangeCursorStore } from "./exchange-store.ts";

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
  readonly USERS: {
    idFromName(name: string): NonNullable<unknown>;
    get(id: NonNullable<unknown>): { fetch(request: Request): Promise<Response> };
  };
  readonly GLOBALS: {
    idFromName(name: string): NonNullable<unknown>;
    get(id: NonNullable<unknown>): { fetch(request: Request): Promise<Response> };
  };
  readonly ASSETS?: { fetch(request: Request): Promise<Response> };
  readonly DEPLOYMENT?: string;
  /** Test harness capability. The deployed topology intentionally omits it. */
  readonly TEST_FAILPOINTS?: string;
}

type AssertTrue<T extends true> = T;

/** Compile-time proof that every derived deployed binding fits the host edge. */
export type DerivedEnvironmentCoversHost = AssertTrue<
  IssueTrackerCloudflareEnv extends CloudflareEnv ? true : false
>;

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
    const key = partitionKeyString(resolution.key);
    try {
      if (resolution.key.kind === "workspace") {
        const globalName = partitionKeyString(globalKey());
        const register: typeof RegisterSourceRequest.Type = {
          operationId: `register/${EXCHANGE_NAME}/${EXCHANGE_VERSION}/${key}`,
          exchange: EXCHANGE_NAME,
          version: EXCHANGE_VERSION,
          source: resolution.key,
        };
        const registered = await env.GLOBALS.get(env.GLOBALS.idFromName(globalName)).fetch(
          new Request("http://global.internal/_streamsy/exchange/register", {
            method: "POST",
            headers: { "content-type": "application/json", [PARTITION_HEADER]: globalName },
            body: JSON.stringify(register),
          }),
        );
        if (!registered.ok) return new Response(registered.body, registered);
        return await env.WORKSPACES.get(env.WORKSPACES.idFromName(key)).fetch(forward(request, key, id));
      }
      const namespace = resolution.key.kind === "user" ? env.USERS : env.GLOBALS;
      return await namespace.get(namespace.idFromName(key)).fetch(forward(request, key, id));
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
  readonly sql: SqlClient.SqlClient;
  readonly sqlRuntime: ManagedRuntime.ManagedRuntime<SqlClient.SqlClient, never>;
}

interface GuardedOperation {
  state: "active" | "consumed";
}

type AlarmFailurePoint =
  | "begin-get"
  | "begin-set"
  | "notification-discovery"
  | "reconcile-get"
  | "reconcile-set"
  | "reconcile-delete";

function isAlarmFailurePoint(value: string): value is AlarmFailurePoint {
  return (
    value === "begin-get" ||
    value === "begin-set" ||
    value === "notification-discovery" ||
    value === "reconcile-get" ||
    value === "reconcile-set" ||
    value === "reconcile-delete"
  );
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
  private releaseCancelAfterCreate = false;
  private applicationGate: TestGate | undefined;
  private expiryGate: TestGate | undefined;
  private maintenanceCompletionGate: TestGate | undefined;
  private maintenanceBeginGate: TestGate | undefined;
  private expiryDeleteGate: TestGate | undefined;
  private cancelExpiryGate: TestGate | undefined;
  private alarmFailure: AlarmFailurePoint | undefined;

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
        beforeExpiryDeleteCommit: () => this.beforeExpiryDeleteCommit(),
        beforeCancelExpiry: () => this.beforeCancelExpiry(),
      });
      protocol = new StreamProtocol({
        storage: { adapter: streams.adapter },
        longPollTimeoutMs: 5_000,
      });
      const client = directProtocolClient(protocol);
      const gateway = createHttpHandler({ protocol, pathPrefix: "/streams" });
      const sqlLayer = SqliteClient.layer({ storage: this.ctx.storage }).pipe(Layer.orDie);
      const sqlRuntime = ManagedRuntime.make(sqlLayer);
      const sql = sqlRuntime.runSync(SqlClient.SqlClient);
      const layer = applicationLayer({
        client,
        protocol,
        gateway,
        store: Layer.orDie(migratedSqlLayer).pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
        ),
        config: AppConfigModule.layer({ deployment: this.env.DEPLOYMENT ?? "cloudflare-local" }),
        notificationTarget: cloudflareNotificationTargetLayer(this.ctx.storage, () =>
          this.consumeNotificationInterrupt(),
        ),
      }).pipe(Layer.orDie);
      const runtime = ManagedRuntime.make(layer);
      // Building the runtime acquires the one shared DO client and completes all
      // migrations before a triggering request can resolve an application store.
      await runtime.runPromise(Effect.void);
      return { workspaceId: decoded.id, protocol, streams, gateway, runtime, sql, sqlRuntime };
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
      const url = new URL(request.url);
      if (url.pathname === "/_streamsy/exchange/read-page") {
        const actor = await this.current(request);
        return await this.readExchangePage(request, actor, id);
      }
      if (url.pathname === "/_streamsy/maintenance") {
        const actor = await this.current(request);
        if (this.env.TEST_FAILPOINTS === "enabled") {
          const alarmControl = url.searchParams.get("test-alarm");
          if (alarmControl === "set") {
            const at = Number(url.searchParams.get("at"));
            if (!Number.isFinite(at)) throw new Error("invalid test alarm timestamp");
            await this.ctx.storage.setAlarm(at);
            return json({ alarm: at }, 200, id);
          }
          if (alarmControl === "get") {
            return json({ alarm: await this.ctx.storage.getAlarm() }, 200, id);
          }
          if (alarmControl === "release-maintenance-begin") {
            this.maintenanceBeginGate?.release();
            return json({ alarm: await this.ctx.storage.getAlarm() }, 200, id);
          }
        }
        this.configureAlarmFailure(request);
        if (this.testFailpoint(request, "pause-maintenance-after-begin")) {
          this.maintenanceBeginGate = makeTestGate();
        }
        if (this.testFailpoint(request, "pause-maintenance-after-reconcile")) {
          this.maintenanceCompletionGate = makeTestGate();
        }
        const report = await this.runMaintenance();
        return json({ ...report, workspaceId: actor.workspaceId }, 200, id);
      }
      const actor = await this.current(request);
      this.configureAlarmFailure(request);
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
        let operation: GuardedOperation | undefined;
        try {
          operation = await this.beginGuardedOperation();
          if (this.testFailpoint(request, "release-maintenance-begin")) {
            const gate = this.maintenanceBeginGate ?? (this.maintenanceBeginGate = makeTestGate());
            await gate.reached;
            gate.release();
          }
          if (this.testFailpoint(request, "pause-next-expiry")) {
            this.expiryGate = makeTestGate();
          }
          if (this.testFailpoint(request, "pause-next-maintenance-after-reconcile")) {
            this.maintenanceCompletionGate = makeTestGate();
            this.recordTestEvent();
          }
          if (this.testFailpoint(request, "pause-renewal-until-expiry")) {
            this.pauseRenewalUntilExpiry = true;
          }
          if (this.testFailpoint(request, "pause-lazy-expiry-delete")) {
            this.expiryDeleteGate = this.expiryGate ?? makeTestGate();
          }
          if (this.testFailpoint(request, "pause-delete-cancellation")) {
            this.cancelExpiryGate = makeTestGate();
          }
          this.releaseCancelAfterCreate = this.testFailpoint(
            request,
            "release-delete-cancellation-after-create",
          );
          this.interruptAfterStreamCreateCommit = this.testFailpoint(
            request,
            "after-stream-create-commit",
          );
          const response = await actor.gateway.fetch(clean);
          response.headers.set(REQUEST_ID_HEADER, id);
          await this.finishGuardedOperation(operation, actor);
          if (this.env.TEST_FAILPOINTS === "enabled") {
            response.headers.set(
              "x-streamsy-test-guarded-operations",
              String(this.guardedOperations),
            );
          }
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
          if (operation !== undefined) await this.abandonGuardedOperation(operation);
          throw cause;
        } finally {
          this.interruptAfterStreamCreateCommit = false;
          this.pauseRenewalUntilExpiry = false;
          this.releaseCancelAfterCreate = false;
        }
      }
      return await this.serialized(async () => {
        let operation: GuardedOperation | undefined;
        try {
          operation = await this.beginGuardedOperation();
          if (this.testFailpoint(request, "pause-after-prearm-then-fail")) {
            const gate = this.applicationGate ?? (this.applicationGate = makeTestGate());
            // Enabled-binding-only proof that beginGuardedOperation completed:
            // this request owns a counted guard before the test interleaves a
            // reconciliation failure with it.
            this.recordTestEvent("application-guard-paused");
            gate.arrive();
            await gate.released;
          }
          this.interruptNotificationAfterAccept = this.testFailpoint(
            request,
            "notification-after-accept",
          );
          if (this.testFailpoint(request, "after-application-commit-with-maintenance-failure")) {
            this.failNextMaintenance = true;
            this.recordTestEvent();
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
          await this.finishGuardedOperation(operation, actor);
          return response;
        } catch (cause) {
          if (operation !== undefined) await this.abandonGuardedOperation(operation);
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

  async alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> {
    await this.runMaintenance(alarmInfo);
  }

  private async readExchangePage(request: Request, actor: ActorRuntime, id: string): Promise<Response> {
    if (request.method !== "POST") return json({ error: "method-not-allowed" }, 405, id);
    const input = Schema.decodeUnknownSync(ReadAssignmentPageRequest)(await request.json());
    if (partitionKeyString(input.source) !== `workspace:${actor.workspaceId}`) {
      return json({ error: "wrong-workspace-object", detail: actor.workspaceId }, 409, id);
    }
    if (input.limit < 1 || input.limit > EXCHANGE_SOURCE_PAGE_LIMIT) {
      return json({ error: "invalid-page-limit" }, 400, id);
    }
    const requestHash = await stableHash(input);
    const sql = actor.sql;
    await Effect.runPromise(sql.unsafe<Record<string, never>>(
      "CREATE TABLE IF NOT EXISTS exchange_read_receipts (operation_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, response TEXT NOT NULL, created_at_ms INTEGER NOT NULL)",
    ));
    const existing = (await Effect.runPromise(sql.unsafe<{ readonly request_hash: string; readonly response: string }>(
      "SELECT request_hash, response FROM exchange_read_receipts WHERE operation_id = ?",
      [input.operationId],
    )))[0];
    if (existing !== undefined) {
      if (existing.request_hash !== requestHash) return json({ error: "operation-id-conflict" }, 409, id);
      return new Response(existing.response, { headers: { "content-type": "application/json" } });
    }
    const page = await actor.runtime.runPromise(
      readAssignmentActivity(actor.workspaceId, input.afterArrival, input.limit),
    );
    const result: typeof ReadAssignmentPageResult.Type = {
      operationId: input.operationId,
      requestHash,
      source: input.source,
      fromArrival: input.afterArrival,
      toArrival: page.arrival,
      upToDate: page.upToDate,
      records: page.records,
    };
    const encoded = JSON.stringify(result);
    await Effect.runPromise(sql.withTransaction(sql.unsafe<Record<string, never>>(
      "INSERT INTO exchange_read_receipts (operation_id, request_hash, response, created_at_ms) VALUES (?, ?, ?, ?)",
      [input.operationId, requestHash, encoded, Date.now()],
    ).pipe(Effect.asVoid)));
    return new Response(encoded, { headers: { "content-type": "application/json" } });
  }

  /** Idempotent maintenance RPC; the alarm handler and local harness share it. */
  private async runMaintenance(
    alarmInfo?: AlarmInvocationInfo,
  ): Promise<{ workspaceId: string; delivered: number }> {
    return await this.serialized(async () => {
      let operation: GuardedOperation | undefined;
      try {
        operation = await this.beginGuardedOperation();
        if (this.maintenanceBeginGate !== undefined) {
          const gate = this.maintenanceBeginGate;
          this.recordTestEvent("maintenance-begin-paused");
          gate.arrive();
          await gate.released;
          this.maintenanceBeginGate = undefined;
        }
        const actor = await this.current();
        if (alarmInfo !== undefined) {
          this.recordTestEvent(`alarm-start:${alarmInfo.retryCount}:${alarmInfo.isRetry}`);
        }
        if (this.failNextMaintenance) {
          this.failNextMaintenance = false;
          this.recordTestEvent("maintenance-failed");
          if (alarmInfo !== undefined) {
            this.recordTestEvent(`alarm-failed:${alarmInfo.retryCount}:${alarmInfo.isRetry}`);
          }
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
        if (alarmInfo !== undefined) {
          this.recordTestEvent(`alarm-succeeded:${alarmInfo.retryCount}:${alarmInfo.isRetry}`);
        }
        await this.finishGuardedOperation(operation, actor);
        if (this.maintenanceCompletionGate !== undefined) {
          const gate = this.maintenanceCompletionGate;
          this.recordTestEvent("maintenance-paused");
          gate.arrive();
          await gate.released;
          this.maintenanceCompletionGate = undefined;
        }
        return { workspaceId: actor.workspaceId, delivered: delivery.delivered };
      } catch (cause) {
        if (operation !== undefined) await this.abandonGuardedOperation(operation);
        throw cause;
      }
    });
  }

  private async reconcileOwedWork(actor: ActorRuntime): Promise<void> {
    this.failAlarmPhase("notification-discovery");
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
      const current = await this.getAlarm("reconcile-get");
      if (current === null || current > desired) await this.setAlarm(desired, "reconcile-set");
      return;
    }
    if (next !== undefined)
      await this.setAlarm(Math.max(Date.now() + ALARM_FLOOR_MS, next), "reconcile-set");
    else await this.deleteAlarm("reconcile-delete");
  }

  private async beginGuardedOperation(): Promise<GuardedOperation> {
    return await this.serializedAlarm(async () => {
      await this.preserveOrArmGuard();
      const operation: GuardedOperation = { state: "active" };
      this.guardedOperations += 1;
      return operation;
    });
  }

  private async finishGuardedOperation(
    operation: GuardedOperation,
    actor: ActorRuntime,
  ): Promise<void> {
    await this.serializedAlarm(async () => {
      this.consumeGuardedOperation(operation);
      await this.reconcileOwedWork(actor);
    });
  }

  private async abandonGuardedOperation(operation: GuardedOperation): Promise<void> {
    await this.serializedAlarm(async () => {
      if (operation.state === "consumed") return;
      this.consumeGuardedOperation(operation);
      await this.preserveOrArmGuard();
    });
  }

  private consumeGuardedOperation(operation: GuardedOperation): void {
    if (operation.state !== "active") throw new Error("guarded operation already consumed");
    operation.state = "consumed";
    this.guardedOperations -= 1;
    if (this.guardedOperations < 0) throw new Error("guarded operation count became negative");
  }

  private async preserveOrArmGuard(): Promise<void> {
    const guard = Date.now() + MAINTENANCE_GUARD_MS;
    const current = await this.getAlarm("begin-get");
    if (current === null || current > guard) await this.setAlarm(guard, "begin-set");
  }

  private async getAlarm(point: "begin-get" | "reconcile-get"): Promise<number | null> {
    this.failAlarmPhase(point);
    return await this.ctx.storage.getAlarm();
  }

  private async setAlarm(at: number, point: "begin-set" | "reconcile-set"): Promise<void> {
    this.failAlarmPhase(point);
    await this.ctx.storage.setAlarm(at);
  }

  private async deleteAlarm(point: "reconcile-delete"): Promise<void> {
    this.failAlarmPhase(point);
    await this.ctx.storage.deleteAlarm();
  }

  private failAlarmPhase(point: AlarmFailurePoint): void {
    if (this.alarmFailure !== point) return;
    this.alarmFailure = undefined;
    throw new Error(`injected alarm phase failure: ${point}`);
  }

  private configureAlarmFailure(request: Request): void {
    if (this.env.TEST_FAILPOINTS !== "enabled") return;
    const value = request.headers.get(TEST_FAILPOINT_HEADER);
    if (value?.startsWith("fail-alarm-") !== true) return;
    const point = value.slice("fail-alarm-".length);
    if (isAlarmFailurePoint(point)) this.alarmFailure = point;
  }

  private recordTestEvent(name?: string): void {
    if (this.env.TEST_FAILPOINTS !== "enabled") return;
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS issue_tracker_test_events (name TEXT PRIMARY KEY)",
    );
    if (name !== undefined) {
      this.ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO issue_tracker_test_events(name) VALUES (?)",
        name,
      );
    }
  }

  private async afterStreamCreateCommit(): Promise<void> {
    if (this.releaseCancelAfterCreate) {
      const gate = this.cancelExpiryGate ?? (this.cancelExpiryGate = makeTestGate());
      await gate.reached;
      gate.release();
    }
    if (!this.interruptAfterStreamCreateCommit) return;
    this.interruptAfterStreamCreateCommit = false;
    throw new Error("injected interruption after atomic stream create commit");
  }

  private async beforeStreamAppendCommit(): Promise<void> {
    if (!this.pauseRenewalUntilExpiry) return;
    this.recordTestEvent("renewal-append-paused");
    const gate = this.expiryGate ?? (this.expiryGate = makeTestGate());
    await gate.reached;
  }

  private async afterStreamAppendCommit(): Promise<void> {
    if (this.pauseRenewalUntilExpiry) this.expiryGate?.release();
  }

  private async beforeExpiryDeleteCommit(): Promise<void> {
    const gate = this.expiryDeleteGate;
    if (gate === undefined) return;
    this.expiryDeleteGate = undefined;
    this.recordTestEvent("lazy-expiry-delete-paused");
    gate.arrive();
    await gate.released;
  }

  private async beforeCancelExpiry(): Promise<void> {
    const gate = this.cancelExpiryGate;
    if (gate === undefined) return;
    this.recordTestEvent("delete-cancellation-paused");
    gate.arrive();
    await gate.released;
    this.cancelExpiryGate = undefined;
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

const StoredUserPartition = Schema.Struct({ kind: Schema.Literal("user"), id: Schema.String });

/** One Effect-SQL-backed inbox actor per canonical user key. */
export class UserPartitionObject extends DurableObject<CloudflareEnv> {
  private runtime: Promise<{ readonly userId: string; readonly runtime: ManagedRuntime.ManagedRuntime<UserServices, never> }> | undefined;

  private initialize(canonicalKey: string) {
    if (this.runtime !== undefined) return this.runtime;
    this.runtime = this.ctx.blockConcurrencyWhile(async () => {
      const decoded = Schema.decodeUnknownSync(StoredUserPartition)(parsePartitionKey(canonicalKey));
      if (partitionKeyString(userKey(decoded.id)) !== canonicalKey) throw new Error(`non-canonical user key ${canonicalKey}`);
      const stored = this.ctx.storage.kv.get<string>(PARTITION_HEADER);
      if (stored !== undefined && stored !== canonicalKey) throw new Error(`object is already bound to ${stored}`);
      if (stored === undefined) this.ctx.storage.kv.put(PARTITION_HEADER, canonicalKey);
      const sqlLayer = SqliteClient.layer({ storage: this.ctx.storage }).pipe(Layer.orDie);
      const layer = migratedInboxSqlLayer.pipe(Layer.orDie, Layer.provide(sqlLayer));
      const runtime = ManagedRuntime.make(layer);
      await runtime.runPromise(InboxStore);
      return { userId: decoded.id, runtime };
    });
    return this.runtime;
  }

  async fetch(request: Request): Promise<Response> {
    const id = requestId(request);
    try {
      const key = request.headers.get(PARTITION_HEADER) ?? this.ctx.storage.kv.get<string>(PARTITION_HEADER);
      if (key === null || key === undefined) throw new Error("missing user partition identity");
      const actor = await this.initialize(key);
      const url = new URL(request.url);
      if (url.pathname === "/_streamsy/exchange/apply-inbox") {
        const input = Schema.decodeUnknownSync(ApplyInboxBatchRequest)(await request.json());
        if (input.destination.id !== actor.userId) return json({ error: "wrong-user-object" }, 409, id);
        const expectedHash = await stableHash({ source: input.source, destination: input.destination, fromArrival: input.fromArrival, toArrival: input.toArrival, rows: input.rows });
        if (expectedHash !== input.payloadHash) return json({ error: "payload-hash-mismatch" }, 409, id);
        const result = await actor.runtime.runPromise(Effect.gen(function* () {
          const inbox = yield* InboxStore;
          return yield* inbox.applyBatch(actor.userId, { operationId: input.operationId, payloadHash: input.payloadHash, rows: input.rows });
        }));
        return json(Schema.encodeSync(ApplyInboxBatchResult)(result), 200, id);
      }
      const resolution = resolveRoute(url.pathname);
      if (resolution.kind !== "partition" || resolution.key.kind !== "user" || resolution.key.id !== actor.userId) {
        return json({ error: "wrong-user-object", detail: actor.userId }, 409, id);
      }
      const response = await actor.runtime.runPromise(handleUserRequest(request));
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
    } catch (cause) {
      return json({ error: "partition-unavailable", detail: cause instanceof Error ? cause.message : String(cause) }, 503, id);
    }
  }
}

interface ScheduleSourceRow {
  readonly source: string;
  readonly failure_count: number;
  readonly next_eligible_at_ms: number;
  readonly last_exchanged_at_ms: number;
}

/** Singleton global owner of registry, cursors, attempts, scheduling and alarms. */
export class GlobalExchangeObject extends DurableObject<CloudflareEnv> {
  private runtime: Promise<{
    readonly runtime: ManagedRuntime.ManagedRuntime<GlobalServices, never>;
    readonly sql: SqlClient.SqlClient;
    readonly sqlRuntime: ManagedRuntime.ManagedRuntime<SqlClient.SqlClient, never>;
  }> | undefined;
  private turn: Promise<void> = Promise.resolve();

  private initialize(canonicalKey: string) {
    if (this.runtime !== undefined) return this.runtime;
    this.runtime = this.ctx.blockConcurrencyWhile(async () => {
      if (canonicalKey !== partitionKeyString(globalKey())) throw new Error(`non-canonical global key ${canonicalKey}`);
      const stored = this.ctx.storage.kv.get<string>(PARTITION_HEADER);
      if (stored !== undefined && stored !== canonicalKey) throw new Error(`object is already bound to ${stored}`);
      if (stored === undefined) this.ctx.storage.kv.put(PARTITION_HEADER, canonicalKey);
      const sqlLayer = SqliteClient.layer({ storage: this.ctx.storage }).pipe(Layer.orDie);
      const sqlRuntime = ManagedRuntime.make(sqlLayer);
      const sql = sqlRuntime.runSync(SqlClient.SqlClient);
      const runtime = ManagedRuntime.make(
        migratedGlobalSqlLayer.pipe(
          Layer.orDie,
          Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
        ),
      );
      await runtime.runPromise(ExchangeCursorStore);
      await Effect.runPromise(Effect.gen(function* () {
        yield* sql.unsafe<Record<string, never>>(`CREATE TABLE IF NOT EXISTS exchange_registration_receipts (
          operation_id TEXT PRIMARY KEY, request_hash TEXT NOT NULL, response TEXT NOT NULL, created_at_ms INTEGER NOT NULL
        )`);
        yield* sql.unsafe<Record<string, never>>(`CREATE TABLE IF NOT EXISTS exchange_schedule (
          source TEXT PRIMARY KEY, failure_count INTEGER NOT NULL DEFAULT 0,
          next_eligible_at_ms INTEGER NOT NULL DEFAULT 0, last_error TEXT
        )`);
        yield* sql.unsafe<Record<string, never>>(`CREATE TABLE IF NOT EXISTS exchange_attempts (
          attempt_id TEXT PRIMARY KEY, source TEXT NOT NULL, expected_arrival INTEGER NOT NULL,
          status TEXT NOT NULL, request_hash TEXT NOT NULL, applied INTEGER NOT NULL DEFAULT 0,
          created_at_ms INTEGER NOT NULL, completed_at_ms INTEGER
        )`);
      }));
      return { runtime, sql, sqlRuntime };
    });
    return this.runtime;
  }

  private current() { return this.initialize(partitionKeyString(globalKey())); }

  async fetch(request: Request): Promise<Response> {
    const id = requestId(request);
    try {
      const actor = await this.current();
      const url = new URL(request.url);
      if (url.pathname === "/_streamsy/exchange/register") {
        const input = Schema.decodeUnknownSync(RegisterSourceRequest)(await request.json());
        const requestHash = await stableHash(input);
        const result = await Effect.runPromise(Effect.gen(function* () {
          const sql = actor.sql;
          const receipt = (yield* sql.unsafe<{ readonly request_hash: string; readonly response: string }>(
            "SELECT request_hash, response FROM exchange_registration_receipts WHERE operation_id = ?", [input.operationId],
          ))[0];
          if (receipt !== undefined) {
            if (receipt.request_hash !== requestHash) return yield* Effect.fail("operation-id-conflict");
            return receipt.response;
          }
          const response = JSON.stringify({ operationId: input.operationId, source: input.source, registered: true });
          yield* sql.withTransaction(Effect.gen(function* () {
            yield* sql.unsafe<Record<string, never>>(
              "INSERT INTO exchange_sources (source, registered_at_ms, last_exchanged_at_ms) VALUES (?, ?, 0) ON CONFLICT(source) DO NOTHING",
              [partitionKeyString(input.source), Date.now()],
            );
            yield* sql.unsafe<Record<string, never>>("INSERT INTO exchange_schedule (source) VALUES (?) ON CONFLICT(source) DO NOTHING", [partitionKeyString(input.source)]);
            yield* sql.unsafe<Record<string, never>>("INSERT INTO exchange_registration_receipts (operation_id, request_hash, response, created_at_ms) VALUES (?, ?, ?, ?)", [input.operationId, requestHash, response, Date.now()]);
          }));
          return response;
        }));
        await this.ensureAlarm(Date.now() + ALARM_FLOOR_MS);
        return new Response(result, { headers: { "content-type": "application/json" } });
      }
      if (url.pathname === "/_streamsy/exchange/run" && this.env.TEST_FAILPOINTS === "enabled") {
        await this.runTurn();
        return json({ ran: true }, 200, id);
      }
      const response = await actor.runtime.runPromise(handleGlobalRequest(request));
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
    } catch (cause) {
      return json({ error: "partition-unavailable", detail: cause instanceof Error ? cause.message : String(cause) }, 503, id);
    }
  }

  async alarm(): Promise<void> { await this.serialized(() => this.runTurn()); }

  private async runTurn(): Promise<void> {
    const actor = await this.current();
    await this.ensureAlarm(Date.now() + MAINTENANCE_GUARD_MS);
    const sources = await Effect.runPromise(Effect.gen(function* () {
      const sql = actor.sql;
      return yield* sql.unsafe<ScheduleSourceRow>(
        "SELECT s.source, COALESCE(q.failure_count,0) failure_count, COALESCE(q.next_eligible_at_ms,0) next_eligible_at_ms, s.last_exchanged_at_ms" +
          " FROM exchange_sources s LEFT JOIN exchange_schedule q ON q.source=s.source" +
          " WHERE COALESCE(q.next_eligible_at_ms,0) <= ? ORDER BY s.last_exchanged_at_ms, s.source LIMIT 2",
        [Date.now()],
      );
    }));
    let immediate = false;
    for (const source of sources) {
      try { immediate = (await this.exchangeSource(source.source)) || immediate; }
      catch (cause) { await this.recordFailure(source, cause); }
    }
    await this.ctx.storage.setAlarm(Date.now() + (immediate ? ALARM_FLOOR_MS : 1_000));
  }

  private async exchangeSource(sourceName: string): Promise<boolean> {
    const parsed = parsePartitionKey(sourceName);
    if (parsed?.kind !== "workspace") throw new Error(`invalid source ${sourceName}`);
    const actor = await this.current();
    const cursor = await actor.runtime.runPromise(Effect.gen(function* () {
      return yield* (yield* ExchangeCursorStore).read(EXCHANGE_NAME, EXCHANGE_VERSION, parsed);
    }));
    const attemptId = `attempt/${EXCHANGE_NAME}/${EXCHANGE_VERSION}/${sourceName}/${cursor.arrival}`;
    const pageRequest: typeof ReadAssignmentPageRequest.Type = {
      operationId: `${attemptId}/page`, exchange: EXCHANGE_NAME, version: EXCHANGE_VERSION,
      source: parsed, afterArrival: cursor.arrival, limit: EXCHANGE_SOURCE_PAGE_LIMIT,
    };
    const attemptHash = await stableHash(pageRequest);
    await Effect.runPromise(actor.sql.withTransaction(Effect.gen(function* () {
      const existing = (yield* actor.sql.unsafe<{ readonly request_hash: string }>(
        "SELECT request_hash FROM exchange_attempts WHERE attempt_id = ?", [attemptId],
      ))[0];
      if (existing !== undefined && existing.request_hash !== attemptHash) {
        return yield* Effect.fail("attempt-identity-conflict");
      }
      yield* actor.sql.unsafe<Record<string, never>>(
        "INSERT INTO exchange_attempts (attempt_id, source, expected_arrival, status, request_hash, created_at_ms)" +
          " VALUES (?, ?, ?, 'allocated', ?, ?) ON CONFLICT(attempt_id) DO NOTHING",
        [attemptId, sourceName, cursor.arrival, attemptHash, Date.now()],
      );
      return undefined;
    })));
    const pageResponse = await this.env.WORKSPACES.get(this.env.WORKSPACES.idFromName(sourceName)).fetch(
      new Request("http://workspace.internal/_streamsy/exchange/read-page", { method: "POST", headers: { "content-type": "application/json", [PARTITION_HEADER]: sourceName }, body: JSON.stringify(pageRequest) }),
    );
    if (!pageResponse.ok) throw new Error(`source-read:${pageResponse.status}:${await pageResponse.text()}`);
    const page = Schema.decodeUnknownSync(ReadAssignmentPageResult)(await pageResponse.json());
    const grouped = new Map<string, InboxRow[]>();
    for (const record of page.records) {
      const source = assignmentInbox.sourceKey(record);
      if ("_tag" in source || partitionKeyString(source) !== sourceName) {
        throw new Error("source-poison:source-key");
      }
      const destination = assignmentInbox.destinationKey(record);
      if ("_tag" in destination || destination.kind !== "user") throw new Error("source-poison:destination-key");
      const row = assignmentInbox.rowFor(record, destination);
      if ("_tag" in row) throw new Error("source-poison:row-key");
      const rows = grouped.get(destination.id);
      if (rows === undefined) grouped.set(destination.id, [row]);
      else rows.push(row);
    }
    let applied = 0;
    for (const [userId, rows] of grouped) {
      const payload = { source: parsed, destination: userKey(userId), fromArrival: page.fromArrival, toArrival: page.toArrival, rows };
      const input: typeof ApplyInboxBatchRequest.Type = {
        operationId: `${attemptId}/user/${userId}`, exchange: EXCHANGE_NAME, version: EXCHANGE_VERSION,
        ...payload, payloadHash: await stableHash(payload),
      };
      const response = await this.env.USERS.get(this.env.USERS.idFromName(`user:${userId}`)).fetch(
        new Request("http://user.internal/_streamsy/exchange/apply-inbox", { method: "POST", headers: { "content-type": "application/json", [PARTITION_HEADER]: `user:${userId}` }, body: JSON.stringify(input) }),
      );
      if (!response.ok) throw new Error(`destination:${response.status}:${await response.text()}`);
      applied += Schema.decodeUnknownSync(ApplyInboxBatchResult)(await response.json()).applied;
    }
    await actor.runtime.runPromise(Effect.gen(function* () {
      const sql = actor.sql;
      const cursors = yield* ExchangeCursorStore;
      yield* sql.withTransaction(Effect.gen(function* () {
        const current = yield* cursors.read(EXCHANGE_NAME, EXCHANGE_VERSION, parsed);
        if (current.arrival === cursor.arrival) {
          yield* cursors.advance({ ...current, arrival: page.toArrival, applied: current.applied + applied });
          yield* sql.unsafe<Record<string, never>>("UPDATE exchange_sources SET last_exchanged_at_ms = ? WHERE source = ?", [Date.now(), sourceName]);
          yield* sql.unsafe<Record<string, never>>("UPDATE exchange_schedule SET failure_count=0, next_eligible_at_ms=0, last_error=NULL WHERE source = ?", [sourceName]);
          yield* sql.unsafe<Record<string, never>>("INSERT INTO exchange_attempts (attempt_id, source, expected_arrival, status, request_hash, applied, created_at_ms, completed_at_ms) VALUES (?, ?, ?, 'completed', ?, ?, ?, ?) ON CONFLICT(attempt_id) DO UPDATE SET status='completed', completed_at_ms=excluded.completed_at_ms", [attemptId, sourceName, cursor.arrival, page.requestHash, applied, Date.now(), Date.now()]);
        }
      }));
    }));
    return !page.upToDate;
  }

  private async recordFailure(source: ScheduleSourceRow, cause: unknown): Promise<void> {
    const actor = await this.current();
    const count = source.failure_count + 1;
    const delay = Math.min(60_000, 250 * 2 ** Math.min(count, 8));
    await Effect.runPromise(Effect.gen(function* () {
      const sql = actor.sql;
      yield* sql.unsafe<Record<string, never>>("UPDATE exchange_schedule SET failure_count=?, next_eligible_at_ms=?, last_error=? WHERE source=?", [count, Date.now() + delay, String(cause).slice(0, 1_000), source.source]);
      yield* sql.unsafe<Record<string, never>>("UPDATE exchange_attempts SET status='failed' WHERE source=? AND status='allocated'", [source.source]);
    }));
  }

  private async ensureAlarm(at: number): Promise<void> {
    const current = await this.ctx.storage.getAlarm();
    if (current === null || current > at) await this.ctx.storage.setAlarm(at);
  }
  private serialized<A>(work: () => Promise<A>): Promise<A> {
    const result = this.turn.then(work);
    this.turn = result.then(() => undefined, () => undefined);
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
