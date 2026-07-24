/** Web-standard Risk API and narrowly scoped read-only Streamsy facade. */
import { createReadOnlyHttpHandler, type StreamProtocolFactory } from "@streamsy/core";

import type { Rng } from "../../src/domain/rng.ts";
import { createSeededRng } from "../../src/domain/rng.ts";
import {
  bearerToken,
  constantTimeEqual,
  issueCapability,
  parseToken,
  sha256Hex,
  type CapabilityRole,
} from "../capabilities.ts";
import { createBoardRuntimeCache, type BoardRuntimeCache } from "../game/board.ts";
import { createBoardRuntimeCacheV2, type BoardRuntimeCacheV2 } from "../game/board-v2.ts";
import type { CommandServiceDeps } from "../game/command-service.ts";
import {
  createDefenseTimers,
  type DefenseTimers,
  type TimerScheduler,
} from "../game/defense-timer.ts";
import { createRouter, error } from "./router.ts";
import { BOARD_GENERATION, boardStreamId } from "../game/names.ts";
import { createRiskRoutes } from "./routes.ts";
import type { CapabilityRow, Stores } from "../persistence/stores.ts";

export interface AppDeps {
  protocol: StreamProtocolFactory;
  stores: Stores;
  rng?: Rng;
  now?: () => number;
  boardCache?: BoardRuntimeCache;
  boardCacheV2?: BoardRuntimeCacheV2;
  /** `risk-demo-v2` defence window; injectable so tests need not wait 15s. */
  defenseTimeoutMs?: number;
  /** Delayed-execution primitive for defence timeouts; manual in tests. */
  scheduler?: TimerScheduler;
}

export interface App {
  fetch: (request: Request) => Promise<Response>;
  /** Durable defence timers, exposed for restart recovery and for tests. */
  defenseTimers: DefenseTimers;
}

export interface AppContext {
  protocol: StreamProtocolFactory;
  stores: Stores;
  now: () => number;
  boardCache: BoardRuntimeCache;
  boardCacheV2: BoardRuntimeCacheV2;
  commandService: CommandServiceDeps;
  defenseTimers: DefenseTimers;
  activeGeneration(gameId: string): string;
  requireCapability(
    request: Request,
    gameId: string,
    role?: CapabilityRole,
  ): Promise<CapabilityRow | Response>;
  issueAndStore(gameId: string, playerId: string, role: CapabilityRole): Promise<string>;
}

export function buildApp(deps: AppDeps): App {
  const now = deps.now ?? (() => Date.now());
  const boardCache = deps.boardCache ?? createBoardRuntimeCache();
  const boardCacheV2 = deps.boardCacheV2 ?? createBoardRuntimeCacheV2();
  const commandService: CommandServiceDeps = {
    protocol: deps.protocol,
    commands: deps.stores.commands,
    rng: deps.rng ?? createSeededRng(0x1215_9ee5),
    now,
    defenseTimeoutMs: deps.defenseTimeoutMs,
  };
  const defenseTimers = createDefenseTimers({
    protocol: deps.protocol,
    commandService,
    games: deps.stores.games,
    scheduler: deps.scheduler,
    now,
  });

  async function authenticate(request: Request): Promise<CapabilityRow | null> {
    const token = bearerToken(request.headers.get("authorization"));
    if (!token) return null;
    const parsed = parseToken(token);
    if (!parsed) return null;
    const row = deps.stores.capabilities.getByTokenId(parsed.tokenId);
    if (!row) return null;
    const hash = await sha256Hex(parsed.secret);
    return constantTimeEqual(hash, row.verifierHash) ? row : null;
  }

  async function requireCapability(
    request: Request,
    gameId: string,
    role?: CapabilityRole,
  ): Promise<CapabilityRow | Response> {
    const cap = await authenticate(request);
    if (!cap) return error(401, "UNAUTHORIZED", "Missing or invalid bearer capability.");
    if (cap.gameId !== gameId) {
      return error(403, "WRONG_GAME", "Capability is scoped to another game.");
    }
    if (role && cap.role !== role) {
      return error(403, "FORBIDDEN", `This action requires the ${role} capability.`);
    }
    return cap;
  }

  async function issueAndStore(
    gameId: string,
    playerId: string,
    role: CapabilityRole,
  ): Promise<string> {
    const issued = await issueCapability({ gameId, playerId, role });
    deps.stores.capabilities.put({
      tokenId: issued.tokenId,
      verifierHash: issued.verifierHash,
      gameId,
      playerId,
      role,
      createdAt: now(),
    });
    return issued.token;
  }

  const context: AppContext = {
    protocol: deps.protocol,
    stores: deps.stores,
    now,
    boardCache,
    boardCacheV2,
    commandService,
    defenseTimers,
    activeGeneration: (gameId) => deps.stores.games.get(gameId)?.generation ?? BOARD_GENERATION,
    requireCapability,
    issueAndStore,
  };
  const api = createRouter(createRiskRoutes(context));
  const streams = createReadOnlyHttpHandler({ protocol: deps.protocol, pathPrefix: "/streams" });

  return {
    defenseTimers,
    async fetch(request) {
      const url = new URL(request.url);
      if (!url.pathname.startsWith("/streams/")) return api(request);

      // Public spectators may read only the currently-active board projection.
      // Canonical events, command logs, turn streams, and retired generations
      // remain unreachable through this facade.
      const match = /^\/streams\/games\/([^/]+)\/projections\/board\/([^/]+)$/.exec(url.pathname);
      if (!match) return error(404, "NOT_FOUND", "Only active board streams are exposed.");
      const gameId = decodeURIComponent(match[1]!);
      const generation = decodeURIComponent(match[2]!);
      const game = deps.stores.games.get(gameId);
      if (!game || boardStreamId(gameId, game.generation) !== boardStreamId(gameId, generation)) {
        return error(404, "NOT_FOUND", "Board stream is not active.");
      }
      return streams.fetch(request);
    },
  };
}
