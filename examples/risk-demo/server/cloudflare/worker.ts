/* oxlint-disable effecttsgo/async-function -- Cloudflare Durable Object storage and fetch handlers are Promise-native platform adapters over the shared game services. */
/* oxlint-disable effecttsgo/global-date -- Cloudflare Durable Object alarms expose millisecond timestamps at this platform adapter boundary. */
import { createStreamProtocol } from "@streamsy/core";
import type { DurableObjectState } from "@cloudflare/workers-types";

import { buildApp, type App } from "../http/app.ts";
import { openApiDocument } from "../http/openapi.ts";
import { createGameStorageAdapter } from "./game-storage.ts";
import { createGameStores } from "./game-stores.ts";

const GAME_ID_HEADER = "x-risk-game-id";
const GAME_ID_PATTERN = /^game_[0-9a-f]{24}$/;

export interface RiskWorkerEnv {
  GAME: {
    idFromName(name: string): unknown;
    get(id: unknown): { fetch(request: Request): Promise<Response> };
  };
  ASSETS: { fetch(request: Request): Promise<Response> };
}

function randomGameId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(12));
  return `game_${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function gameIdFromPath(pathname: string): string | null {
  const match =
    /^\/v1\/games\/([^/]+)(?:\/|$)/.exec(pathname) ??
    /^\/streams\/games\/([^/]+)(?:\/|$)/.exec(pathname);
  if (!match) return null;
  const gameId = decodeURIComponent(match[1]!);
  return GAME_ID_PATTERN.test(gameId) ? gameId : null;
}

async function forwardToGame(request: Request, env: RiskWorkerEnv, gameId: string) {
  const headers = new Headers(request.headers);
  headers.set(GAME_ID_HEADER, gameId);
  const routed = new Request(request, { headers });
  return env.GAME.get(env.GAME.idFromName(gameId)).fetch(routed);
}

export default {
  async fetch(request: Request, env: RiskWorkerEnv): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/v1/games") {
      return forwardToGame(request, env, randomGameId());
    }
    if (url.pathname === "/openapi.json") return json(openApiDocument);
    if (url.pathname === "/healthz") return json({ ok: true });

    const gameId = gameIdFromPath(url.pathname);
    if (gameId) return forwardToGame(request, env, gameId);

    if (
      url.pathname.startsWith("/v1/") ||
      url.pathname.startsWith("/streams/") ||
      url.pathname.startsWith("/agent/") ||
      url.pathname.startsWith("/agent-seat/")
    ) {
      return json({ error: { code: "NOT_FOUND", message: "Unknown game-scoped route." } }, 404);
    }
    return env.ASSETS.fetch(request);
  },
};

/**
 * One primary actor per game. Every Streamsy stream and every metadata table in
 * this class uses this object's SQLite storage; no stream id is routed to a
 * second Durable Object.
 */
export class GameDurableObject {
  private readonly app: App;
  private gameId: string | null;

  constructor(
    private readonly ctx: DurableObjectState,
    _env: RiskWorkerEnv,
  ) {
    this.gameId = ctx.storage.kv.get<string>("risk:game-id") ?? null;
    const adapter = createGameStorageAdapter(ctx.storage);
    const protocol = createStreamProtocol({ storage: { adapter } });
    const stores = createGameStores(ctx.storage);
    const scheduler = {
      schedule: (_timerId: string, delayMs: number) => {
        void ctx.storage.setAlarm(Date.now() + Math.max(0, delayMs));
      },
      cancel: (_timerId: string) => undefined,
      cancelAll: () => undefined,
    };
    this.app = buildApp({
      protocol,
      stores,
      scheduler,
      createGameId: () => {
        if (!this.gameId) throw new Error("Game Durable Object is not initialized");
        return this.gameId;
      },
    });
    if (this.gameId) {
      void ctx.blockConcurrencyWhile(() => this.app.defenseTimers.recover());
    }
  }

  private bindGameId(request: Request): Response | null {
    const requested = request.headers.get(GAME_ID_HEADER);
    if (!requested || !GAME_ID_PATTERN.test(requested)) {
      return json(
        { error: { code: "INVALID_GAME_ROUTE", message: "Missing game identity." } },
        400,
      );
    }
    if (this.gameId && this.gameId !== requested) {
      return json(
        { error: { code: "WRONG_GAME", message: "Durable Object is bound to another game." } },
        409,
      );
    }
    if (!this.gameId) {
      this.gameId = requested;
      this.ctx.storage.kv.put("risk:game-id", requested);
    }
    return null;
  }

  async fetch(request: Request): Promise<Response> {
    const invalid = this.bindGameId(request);
    if (invalid) return invalid;
    const headers = new Headers(request.headers);
    headers.delete(GAME_ID_HEADER);
    return this.app.fetch(new Request(request, { headers }));
  }

  async alarm(): Promise<void> {
    if (!this.gameId) return;
    await this.app.defenseTimers.recover();
    // Materialize the canonical timeout result before the actor can be evicted
    // again, so public board readers see the alarm's effect immediately.
    await this.app.fetch(
      new Request(`https://game.internal/v1/games/${encodeURIComponent(this.gameId)}`),
    );
  }
}
