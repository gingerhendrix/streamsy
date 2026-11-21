/* oxlint-disable effecttsgo/async-function -- Web-standard fetch handlers are Promise-native framework adapters; they delegate game and projection work to the existing application services and runtime. */
/* oxlint-disable effecttsgo/global-date -- The Web request adapter supplies the current command timestamp to the domain contract. */
/** Web-standard Risk API and narrowly scoped read-only Streamsy facade. */
import { Serve } from "@streamsy/serve";
import { HttpRouter } from "effect/http";
import { Effect, Layer } from "effect";
import { BoardStream, type GameStreams } from "../game/streams.ts";
import { BOARD_REDUCER_VERSION } from "../../src/board/board-projection.ts";
import { catchUpActions } from "../game/action-notifier.ts";
import { materializeBoard } from "../game/board.ts";

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
  streams: GameStreams;
  stores: Stores;
  /** Runtime-specific game id minting (the Cloudflare edge routes this id to its DO). */
  createGameId?: () => string;
  rng?: Rng;
  now?: () => number;
  /** `Hex Domination` defence window; injectable so tests need not wait 15s. */
  defenseTimeoutMs?: number;
  /** Delayed-execution primitive for defence timeouts; manual in tests. */
  scheduler?: TimerScheduler;
  /**
   * How long one actions SSE connection is held before the server closes it and
   * the client reconnects. Defaults to `ACTIONS_STREAM_TIMEOUT_MS` (30s);
   * injectable so a test can prove the bound without waiting 30 real seconds.
   */
  actionsStreamTimeoutMs?: number;
}

export interface App {
  fetch: (request: Request) => Promise<Response>;
  /** Durable defence timers, exposed for restart recovery and for tests. */
  defenseTimers: DefenseTimers;
  /**
   * Catch a game's private action streams and active board up to canonical
   * history. A host calls this after recovery so a crash between a canonical
   * append and its publication does not wait for the next reader.
   */
  publish(gameId: string): Promise<void>;
  close(): Promise<void>;
}

export interface AppContext {
  streams: GameStreams;
  stores: Stores;
  now: () => number;
  commandService: CommandServiceDeps;
  createGameId(): string;
  defenseTimers: DefenseTimers;
  actionsStreamTimeoutMs?: number;
  activeGeneration(gameId: string): string;
  authenticateCapability(request: Request): Promise<CapabilityRow | null>;
  requireCapability(
    request: Request,
    gameId: string,
    role?: CapabilityRole,
  ): Promise<CapabilityRow | Response>;
  issueAndStore(gameId: string, playerId: string, role: CapabilityRole): Promise<string>;
}

export function buildApp(deps: AppDeps): App {
  const lifetime = new AbortController();
  const now = deps.now ?? (() => Date.now());
  const commandService: CommandServiceDeps = {
    streams: deps.streams,
    commands: deps.stores.commands,
    rng: deps.rng ?? createSeededRng(0x1215_9ee5),
    now,
    defenseTimeoutMs: deps.defenseTimeoutMs,
  };
  const defenseTimers = createDefenseTimers({
    streams: deps.streams,
    commandService,
    games: deps.stores.games,
    scheduler: deps.scheduler,
    onResolved: (gameId) => publish(gameId),
    now,
  });

  async function publish(gameId: string): Promise<void> {
    await Promise.all([
      catchUpActions(deps.streams, gameId),
      materializeBoard(deps.streams, gameId, activeGeneration(gameId)),
    ]);
  }

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

  function activeGeneration(gameId: string): string {
    const generation = deps.stores.games.get(gameId)?.generation ?? BOARD_GENERATION;
    if (deps.stores.generations.get(gameId, generation)?.reducerVersion !== BOARD_REDUCER_VERSION) {
      throw new Error(`Active board generation "${generation}" needs a rebuild`);
    }
    return generation;
  }

  const context: AppContext = {
    streams: deps.streams,
    stores: deps.stores,
    now,
    commandService,
    createGameId: deps.createGameId ?? (() => randomId("game")),
    defenseTimers,
    actionsStreamTimeoutMs: deps.actionsStreamTimeoutMs,
    activeGeneration,
    authenticateCapability: authenticate,
    requireCapability,
    issueAndStore,
  };
  const api = createRouter(createRiskRoutes(context));
  // Capture the exact storage context used by commands and the projection.
  // Mounting no other declaration makes the raw canonical and action streams unreachable.
  const boardWeb = HttpRouter.toWebHandler(
    Serve.mount(BoardStream, "/streams/games/:gameId/projections/board/:generation", {
      contract: BOARD_REDUCER_VERSION,
    }).pipe(
      HttpRouter.provideRequest(
        Layer.unwrap(deps.streams.contextEffect.pipe(Effect.map(Layer.succeedContext))),
      ),
    ),
    { disableLogger: true },
  );
  return {
    defenseTimers,
    publish,
    async close() {
      lifetime.abort();
      defenseTimers.stop();
      await boardWeb.dispose();
    },
    async fetch(incoming) {
      const request = new Request(incoming, {
        signal: AbortSignal.any([incoming.signal, lifetime.signal]),
      });
      const url = new URL(request.url);
      if (!url.pathname.startsWith("/streams/")) return api(request);

      // Public spectators may read only the currently-active board projection.
      // Canonical events, command logs, turn streams, and retired generations
      // remain unreachable through this facade.
      const match = /^\/streams\/games\/([^/]+)\/projections\/board\/([^/]+)$/.exec(url.pathname);
      if (!match) return error(404, "NOT_FOUND", "Only active board streams are exposed.");
      const decoded = decodeBoardPath(match[1]!, match[2]!);
      if (!decoded) return error(404, "NOT_FOUND", "Invalid board stream path.");
      const { gameId, generation } = decoded;
      const game = deps.stores.games.get(gameId);
      if (!game || boardStreamId(gameId, game.generation) !== boardStreamId(gameId, generation)) {
        return error(404, "NOT_FOUND", "Board stream is not active.");
      }
      if (
        deps.stores.generations.get(gameId, generation)?.reducerVersion !== BOARD_REDUCER_VERSION
      ) {
        return error(409, "INTERNAL", "Active board generation needs a rebuild.");
      }
      // A first read can repair a canonical append whose process died before
      // emitting board rows. Already-open long polls are woken by the writer.
      if (request.method === "GET" || request.method === "HEAD") {
        try {
          await materializeBoard(deps.streams, gameId, generation);
        } catch {
          return error(500, "INTERNAL", "Board projection is unavailable.");
        }
      }
      return boardWeb.handler(request);
    },
  };
}

function randomId(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  return `${prefix}_${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

function decodeBoardPath(game: string, gen: string) {
  try {
    return { gameId: decodeURIComponent(game), generation: decodeURIComponent(gen) };
  } catch {
    return null;
  }
}
