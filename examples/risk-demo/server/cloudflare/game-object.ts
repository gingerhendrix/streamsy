/* oxlint-disable effecttsgo/async-function -- Durable Object fetch, alarm, and constructor hooks are Promise-native platform callbacks over the shared game app. */
/* oxlint-disable effecttsgo/global-date -- Durable Object alarms take millisecond wall-clock timestamps at this platform boundary. */
/**
 * One SQLite Durable Object per game.
 *
 * The object owns every stream of its game (canonical events, private action
 * streams, and board generations), plus the capability verifiers, command
 * receipts, game record, and generation records. The Bun host runs the same
 * `buildApp` over BunStorage. Here it runs over DurableObjectStorage and the
 * object's storage alarm.
 */
import type { DurableObjectState, SqlStorage } from "@cloudflare/workers-types";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { ManagedRuntime, Schema } from "effect";
import { buildApp, type App } from "../http/app.ts";
import type { Stores } from "../persistence/stores.ts";
import { gameStreams, withCheckpoints, type GameStreams } from "../game/streams.ts";
import { RULES } from "../../src/domain/map.ts";
import { createGameStores } from "./game-stores.ts";
import { createAlarmScheduler, type AlarmScheduler } from "./alarm-scheduler.ts";
import { GAME_ID_HEADER, GAME_ID_PATTERN, jsonError, type GameWorkerEnv } from "./routing.ts";

const IDENTITY_SCHEMA = `create table if not exists risk_object (
  singleton integer primary key check (singleton = 1),
  game_id text not null
)`;

const IdentityRow = Schema.Struct({ game_id: Schema.String });
const decodeIdentity = Schema.decodeUnknownSync(IdentityRow);
const DefenseTimeout = Schema.NumberFromString.check(Schema.isInt(), Schema.isGreaterThan(0));
const decodeDefenseTimeout = Schema.decodeUnknownSync(DefenseTimeout);

/** The game this object was created for, or null. Reads only; never creates a table. */
function boundGameId(sql: SqlStorage): string | null {
  const table = sql
    .exec("select name from sqlite_master where type = 'table' and name = 'risk_object'")
    .toArray();
  if (table.length === 0) return null;
  const row = sql.exec("select game_id from risk_object where singleton = 1").toArray()[0];
  return row === undefined ? null : decodeIdentity(row).game_id;
}

function bindGameId(sql: SqlStorage, gameId: string): void {
  sql.exec(IDENTITY_SCHEMA);
  sql.exec("insert into risk_object (singleton, game_id) values (1, ?)", gameId);
}

const now = () => Date.now();

function defenseTimeoutMs(env: GameWorkerEnv): number | undefined {
  const configured = env.DEFENSE_TIMEOUT_MS;
  return configured === undefined || configured === ""
    ? undefined
    : decodeDefenseTimeout(configured);
}

interface GameHost {
  readonly gameId: string;
  readonly app: App;
  readonly stores: Stores;
  readonly scheduler: AlarmScheduler;
}

export class GameDurableObject {
  private host: GameHost | null = null;
  private readonly defenseTimeoutMs: number | undefined;

  constructor(
    private readonly ctx: DurableObjectState,
    env: GameWorkerEnv,
  ) {
    this.defenseTimeoutMs = defenseTimeoutMs(env);
    const gameId = boundGameId(ctx.storage.sql);
    if (gameId !== null) {
      const host = this.open(gameId);
      // Recreated object: re-derive the pending defence from canonical history,
      // resolve a deadline that passed while it was away, re-arm a future one,
      // and repair publication before serving any request.
      void ctx.blockConcurrencyWhile(() => this.reconcile(host));
    }
  }

  private open(gameId: string): GameHost {
    const streams: GameStreams = ManagedRuntime.make(
      withCheckpoints(
        DurableObjectStorage.layer(gameStreams, { client: { storage: this.ctx.storage } }),
      ),
    );
    const scheduler = createAlarmScheduler(this.ctx.storage, now);
    const stores = createGameStores(this.ctx.storage);
    const app = buildApp({
      streams,
      stores,
      scheduler,
      now,
      createGameId: () => gameId,
      defenseTimeoutMs: this.defenseTimeoutMs,
    });
    this.host = { gameId, app, stores, scheduler };
    return this.host;
  }

  private async reconcile(host: GameHost): Promise<void> {
    await host.app.defenseTimers.recover();
    if (host.stores.games.get(host.gameId)) await host.app.publish(host.gameId);
    await host.scheduler.settled();
  }

  /**
   * Arm a recovery alarm before a request can append canonical events. If the
   * object fails after the append but before the command arms its deadline or
   * publishes, this alarm still wakes it to reconcile. A command that opens a
   * defence moves the alarm to the canonical deadline, which is never earlier.
   */
  private async armRecoveryAlarm(): Promise<void> {
    if ((await this.ctx.storage.getAlarm()) !== null) return;
    await this.ctx.storage.setAlarm(Date.now() + (this.defenseTimeoutMs ?? RULES.defenseTimeoutMs));
  }

  async fetch(incoming: Request): Promise<Response> {
    const requested = incoming.headers.get(GAME_ID_HEADER);
    if (requested === null || !GAME_ID_PATTERN.test(requested)) {
      return jsonError(400, "BAD_REQUEST", "Missing game identity.");
    }
    let host = this.host;
    if (host === null) {
      // Only the edge's create route may bind a fresh object. Any other request
      // for an unknown game answers 404 without writing storage.
      const url = new URL(incoming.url);
      if (incoming.method !== "POST" || url.pathname !== "/v1/games") {
        return jsonError(404, "GAME_NOT_FOUND", "Unknown game.");
      }
      bindGameId(this.ctx.storage.sql, requested);
      host = this.open(requested);
    } else if (host.gameId !== requested) {
      return jsonError(409, "WRONG_GAME", "This object belongs to another game.");
    }

    if (incoming.method !== "GET" && incoming.method !== "HEAD") await this.armRecoveryAlarm();
    const headers = new Headers(incoming.headers);
    headers.delete(GAME_ID_HEADER);
    const response = await host.app.fetch(new Request(incoming, { headers }));
    await host.scheduler.settled();
    return response;
  }

  async alarm(): Promise<void> {
    // A throw makes the runtime retry the alarm. Recovery is idempotent: the
    // timeout command id is stable, and a resolved defence is never pending.
    if (this.host !== null) await this.reconcile(this.host);
  }
}
