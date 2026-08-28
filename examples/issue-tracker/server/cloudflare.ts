/* oxlint-disable effecttsgo/async-function -- Worker and Durable Object handlers are Promise-native platform edges. */
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
const ALARM_FLOOR_MS = 1;

export interface CloudflareEnv {
  readonly WORKSPACES: {
    idFromName(name: string): NonNullable<unknown>;
    get(id: NonNullable<unknown>): { fetch(request: Request): Promise<Response> };
  };
  readonly ASSETS?: { fetch(request: Request): Promise<Response> };
  readonly DEPLOYMENT?: string;
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
      response.headers.set(REQUEST_ID_HEADER, id);
      return response;
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
      const adapter = createWorkspaceStreamStorage(this.ctx.storage, () => {
        void this.scheduleAlarm(ALARM_FLOOR_MS);
      });
      protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 5_000 });
      const client = directProtocolClient(protocol);
      const gateway = createHttpHandler({ protocol, pathPrefix: "/streams" });
      const sql = SqliteClient.layer({ storage: this.ctx.storage });
      const layer = applicationLayer({
        client,
        protocol,
        gateway,
        store: Layer.orDie(migratedSqlLayer).pipe(Layer.provide(sql)),
        config: AppConfigModule.layer({ deployment: this.env.DEPLOYMENT ?? "cloudflare-local" }),
      }).pipe(Layer.orDie);
      const runtime = ManagedRuntime.make(layer);
      // Building the runtime acquires the one shared DO client and completes all
      // migrations before a triggering request can resolve an application store.
      await runtime.runPromise(Effect.void);
      return { workspaceId: decoded.id, protocol, gateway, runtime };
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
      const clean = new Request(request, { headers });
      if (resolution.target === "streams") {
        const response = await actor.gateway.fetch(clean);
        response.headers.set(REQUEST_ID_HEADER, id);
        return response;
      }
      return await this.serialized(async () => {
        const response = await actor.runtime.runPromise(handle(clean));
        response.headers.set(REQUEST_ID_HEADER, id);
        await this.scheduleOwedWork(actor);
        return response;
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
      const actor = await this.current();
      await actor.runtime.runPromise(advance(actor.workspaceId));
      const delivery = await actor.runtime.runPromise(drainNotifications(actor.workspaceId));
      await this.scheduleOwedWork(actor);
      return { workspaceId: actor.workspaceId, delivered: delivery.delivered };
    });
  }

  private async scheduleOwedWork(actor: ActorRuntime): Promise<void> {
    const listed = await actor.runtime.runPromise(listNotifications(actor.workspaceId));
    const next = listed.entries
      .filter((entry) => entry.state === "pending")
      .reduce<number | undefined>(
        (earliest, entry) =>
          earliest === undefined
            ? entry.nextAttemptAtMs
            : Math.min(earliest, entry.nextAttemptAtMs),
        undefined,
      );
    if (next !== undefined) {
      await this.ctx.storage.setAlarm(Math.max(Date.now() + ALARM_FLOOR_MS, next));
    }
  }

  private scheduleAlarm(delayMs: number): Promise<void> {
    return this.ctx.storage.setAlarm(Date.now() + Math.max(ALARM_FLOOR_MS, delayMs));
  }

  private serialized<A>(work: () => Promise<A>): Promise<A> {
    const result = this.turn.then(work);
    this.turn = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

export type WorkspaceObjectStorage = DurableObjectStorage;
