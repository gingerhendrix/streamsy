/**
 * Durable Risk command & capability API (Batch 3), as a web-standard fetch
 * handler so it can be driven directly in tests or hosted by Bun.
 *
 * Every mutation authenticates a bearer capability into `{gameId, playerId,
 * role}` before any domain validation. Commands are decided against a fold of
 * canonical Streamsy history at its exact head and CAS-appended there; the board
 * is a separate causally-watermarked projection. Raw tokens are returned once at
 * issuance and never stored or echoed thereafter.
 */

import type { StreamProtocolFactory } from "@streamsy/core";

import type { Command } from "../src/commands.ts";
import { RULESET, MAP_VERSION } from "../src/map.ts";
import { foldAggregate } from "../src/aggregate.ts";
import { buildDecisionContext } from "../src/decision.ts";
import { projectionBoardView } from "../src/projection.ts";
import type { Rng } from "../src/rng.ts";
import { createSeededRng } from "../src/rng.ts";
import {
  bearerToken,
  constantTimeEqual,
  issueCapability,
  parseToken,
  sha256Hex,
  type CapabilityRole,
} from "./capabilities.ts";
import { submitCommand, readCanonical, type SubmitResult } from "./command-service.ts";
import { createBoardRuntimeCache, materializeBoard, type BoardRuntimeCache } from "./board.ts";
import { catchUpTurns, readTurns } from "./turn-notifier.ts";
import { BOARD_REDUCER_VERSION } from "../src/materializer/board-projection.ts";
import { eventStreamId, boardStreamId, BOARD_GENERATION } from "./names.ts";
import { openApiDocument } from "./openapi.ts";
import {
  createRouter,
  error,
  json,
  readJsonBody,
  statusForCode,
  type ErrorCode,
  type Route,
} from "./http.ts";
import type { CapabilityRow, Stores } from "./stores.ts";

export interface AppDeps {
  protocol: StreamProtocolFactory;
  stores: Stores;
  /** Dice/setup randomness. Defaults to a seeded generator for reproducibility. */
  rng?: Rng;
  now?: () => number;
  boardCache?: BoardRuntimeCache;
}

export interface App {
  fetch: (request: Request) => Promise<Response>;
}

function randomId(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  let hex = "";
  for (const b of bytes) hex += b.toString(16).padStart(2, "0");
  return `${prefix}_${hex}`;
}

function ackBody(
  result: Extract<SubmitResult, { status: "accepted" | "duplicate" }>,
  turnId?: string,
): Record<string, unknown> {
  const body: Record<string, unknown> = {
    status: result.status,
    commandId: result.commandId,
    sourceStreamId: result.sourceStreamId,
    sourceOffset: result.sourceOffset,
    events: result.events,
  };
  if (turnId) body.turnId = turnId;
  return body;
}

function rejection(result: Extract<SubmitResult, { status: "rejected" }>): Response {
  const code = result.error.code as ErrorCode;
  const extra = result.error.currentTurnId ? { currentTurnId: result.error.currentTurnId } : {};
  return error(statusForCode(code), code, result.error.message, extra);
}

export function buildApp(deps: AppDeps): App {
  const rng: Rng = deps.rng ?? createSeededRng(0x1215_9ee5);
  const now = deps.now ?? (() => Date.now());
  const boardCache = deps.boardCache ?? createBoardRuntimeCache();
  const service = { protocol: deps.protocol, commands: deps.stores.commands, rng, now };

  /** The durable active board generation for a game (defaults to v1). */
  function activeGeneration(gameId: string): string {
    return deps.stores.games.get(gameId)?.generation ?? BOARD_GENERATION;
  }

  async function authenticate(request: Request): Promise<CapabilityRow | null> {
    const token = bearerToken(request.headers.get("authorization"));
    if (!token) return null;
    const parsed = parseToken(token);
    if (!parsed) return null;
    const row = deps.stores.capabilities.getByTokenId(parsed.tokenId);
    if (!row) return null;
    const hash = await sha256Hex(parsed.secret);
    if (!constantTimeEqual(hash, row.verifierHash)) return null;
    return row;
  }

  /** Authenticate and require the capability to be scoped to `gameId`. */
  async function requireCapability(
    request: Request,
    gameId: string,
    role?: CapabilityRole,
  ): Promise<CapabilityRow | Response> {
    const cap = await authenticate(request);
    if (!cap) return error(401, "UNAUTHORIZED", "Missing or invalid bearer capability.");
    if (cap.gameId !== gameId)
      return error(403, "WRONG_GAME", "Capability is scoped to another game.");
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

  // --- Route handlers -------------------------------------------------------

  async function createGame(request: Request): Promise<Response> {
    const body =
      (await readJsonBody<{ name?: string; color?: string; commandId?: string }>(request)) ?? {};
    const name = body.name ?? "Host";
    const color = body.color ?? "red";
    const gameId = randomId("game");
    const hostPlayerId = randomId("p");
    const command: Command = {
      type: "create-game",
      commandId: body.commandId ?? randomId("cmd"),
      gameId,
      hostPlayerId,
      hostName: name,
      hostColor: color,
    };
    const result = await submitCommand(service, eventStreamId(gameId), command);
    if (result.status === "rejected") return rejection(result);

    deps.stores.games.put({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      projectionStreamId: boardStreamId(gameId),
      generation: BOARD_GENERATION,
      createdAt: now(),
    });
    // Record the initial board generation as the durable active pointer. Later
    // generations are appended by the rebuild service and the active one is
    // repointed atomically on cutover.
    deps.stores.generations.put({
      gameId,
      generation: BOARD_GENERATION,
      streamId: boardStreamId(gameId, BOARD_GENERATION),
      reducerVersion: BOARD_REDUCER_VERSION,
      status: "active",
      sourceThroughOffset: null,
      createdAt: now(),
    });
    const token = await issueAndStore(gameId, hostPlayerId, "host");
    return json(
      {
        game: { id: gameId, ruleset: RULESET, mapVersion: MAP_VERSION },
        player: { id: hostPlayerId, name, color, role: "host" },
        capability: token,
        ack: ackBody(result),
      },
      201,
    );
  }

  async function joinGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    if (!deps.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const body =
      (await readJsonBody<{ name?: string; color?: string; commandId?: string }>(request)) ?? {};
    const name = body.name ?? "Player";
    const color = body.color ?? "blue";
    const playerId = randomId("p");
    const command: Command = {
      type: "join-game",
      commandId: body.commandId ?? randomId("cmd"),
      playerId,
      name,
      color,
    };
    const result = await submitCommand(service, eventStreamId(gameId), command);
    if (result.status === "rejected") return rejection(result);

    const token = await issueAndStore(gameId, playerId, "player");
    return json(
      {
        player: { id: playerId, name, color, role: "player" },
        capability: token,
        ack: ackBody(result),
      },
      201,
    );
  }

  async function startGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await requireCapability(request, gameId, "host");
    if (cap instanceof Response) return cap;
    const body = (await readJsonBody<{ commandId?: string }>(request)) ?? {};
    const command: Command = { type: "start-game", commandId: body.commandId ?? randomId("cmd") };
    const result = await submitCommand(service, eventStreamId(gameId), command);
    if (result.status === "rejected") return rejection(result);
    await catchUpTurns(deps.protocol, gameId);
    return json(ackBody(result), 200);
  }

  async function getGame(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    if (!deps.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const { events } = await readCanonical(deps.protocol, eventStreamId(gameId));
    const state = foldAggregate(events);
    return json({
      gameId,
      status: state.status,
      ruleset: RULESET,
      mapVersion: MAP_VERSION,
      round: state.round,
      activePlayerId: state.activePlayerId,
      phase: state.phase,
      winnerId: state.winnerId,
      players: state.players.map((p) => ({
        id: p.id,
        name: p.name,
        color: p.color,
        eliminated: p.eliminated,
      })),
    });
  }

  async function getBoard(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    if (!deps.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const board = await materializeBoard(
      deps.protocol,
      boardCache,
      gameId,
      activeGeneration(gameId),
    );
    const view = projectionBoardView(board.state);
    return json({
      gameId,
      sourceStreamId: board.sourceStreamId,
      sourceThroughOffset: board.sourceThroughOffset,
      generation: board.generation,
      game: board.state.game,
      players: board.state.players,
      territories: board.state.territories,
      view,
    });
  }

  async function getDecision(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const { events } = await readCanonical(deps.protocol, eventStreamId(gameId));
    const state = foldAggregate(events);
    if (!state.players.some((p) => p.id === cap.playerId)) {
      return error(404, "NOT_FOUND", "Player is not part of this game.");
    }
    const board = await materializeBoard(
      deps.protocol,
      boardCache,
      gameId,
      activeGeneration(gameId),
    );
    const context = buildDecisionContext(state, cap.playerId, {
      sourceStreamId: board.sourceStreamId,
      sourceThroughOffset: board.sourceThroughOffset,
    });
    return json(context);
  }

  async function postCommand(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const body = await readJsonBody<{
      commandId?: unknown;
      turnId?: unknown;
      action?: Record<string, unknown>;
    }>(request);
    if (
      !body ||
      typeof body.commandId !== "string" ||
      typeof body.turnId !== "string" ||
      !body.action
    ) {
      return error(400, "BAD_REQUEST", "Body must be { commandId, turnId, action }.");
    }
    const command = buildPlayCommand(body.commandId, body.turnId, cap.playerId, body.action);
    if (!command) return error(400, "BAD_REQUEST", "Unrecognized or malformed action.");

    const result = await submitCommand(service, eventStreamId(gameId), command);
    if (result.status === "rejected") return rejection(result);
    await catchUpTurns(deps.protocol, gameId);
    return json(ackBody(result, body.turnId), 200);
  }

  async function getTurns(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const url = new URL(request.url);
    const cursor = url.searchParams.get("offset") ?? url.searchParams.get("cursor") ?? undefined;
    const waitMs = Number.parseInt(url.searchParams.get("wait") ?? "0", 10) || 0;
    // Produce any wakes owed by already-committed events, then read/long-poll.
    await catchUpTurns(deps.protocol, gameId);
    const result = await readTurns(deps.protocol, gameId, cap.playerId, {
      cursor,
      waitMs,
      signal: request.signal,
    });
    return json(result);
  }

  async function getCommand(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const commandId = params.commandId!;

    const row = deps.stores.commands.get(gameId, commandId);
    if (row) {
      if (row.status === "accepted") {
        return json({
          status: "accepted",
          commandId,
          sourceStreamId: eventStreamId(gameId),
          sourceOffset: row.sourceOffset,
          events: row.events,
        });
      }
      const code = (row.error?.code ?? "ILLEGAL_ACTION") as ErrorCode;
      return error(statusForCode(code), code, row.error?.message ?? "rejected");
    }

    // Fall back to the canonical stream if the command log lost the row.
    const { byCommand } = await readCanonical(deps.protocol, eventStreamId(gameId));
    const prior = byCommand.get(commandId);
    if (!prior) return error(404, "NOT_FOUND", "Unknown command.");
    return json({
      status: "accepted",
      commandId,
      sourceStreamId: eventStreamId(gameId),
      sourceOffset: prior.lastOffset,
      events: prior.events,
    });
  }

  const routes: Route[] = [
    { method: "GET", pattern: "/", handler: () => json({ name: "risk-demo", ok: true }) },
    { method: "GET", pattern: "/openapi.json", handler: () => json(openApiDocument) },
    { method: "POST", pattern: "/v1/games", handler: createGame },
    { method: "POST", pattern: "/v1/games/:gameId/players", handler: joinGame },
    { method: "POST", pattern: "/v1/games/:gameId/start", handler: startGame },
    { method: "GET", pattern: "/v1/games/:gameId", handler: getGame },
    { method: "GET", pattern: "/v1/games/:gameId/board", handler: getBoard },
    { method: "GET", pattern: "/v1/games/:gameId/decision", handler: getDecision },
    { method: "POST", pattern: "/v1/games/:gameId/commands", handler: postCommand },
    { method: "GET", pattern: "/v1/games/:gameId/commands/:commandId", handler: getCommand },
    { method: "GET", pattern: "/v1/games/:gameId/players/me/turns", handler: getTurns },
  ];

  return { fetch: createRouter(routes) };
}

function buildPlayCommand(
  commandId: string,
  turnId: string,
  playerId: string,
  action: Record<string, unknown>,
): Command | null {
  const type = action.type;
  switch (type) {
    case "reinforce":
      if (typeof action.territoryId !== "string" || typeof action.armies !== "number") return null;
      return {
        type,
        commandId,
        turnId,
        playerId,
        territoryId: action.territoryId,
        armies: action.armies,
      };
    case "attack":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        typeof action.attackerDice !== "number"
      ) {
        return null;
      }
      return {
        type,
        commandId,
        turnId,
        playerId,
        from: action.from,
        to: action.to,
        attackerDice: action.attackerDice,
      };
    case "fortify":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        typeof action.armies !== "number"
      ) {
        return null;
      }
      return {
        type,
        commandId,
        turnId,
        playerId,
        from: action.from,
        to: action.to,
        armies: action.armies,
      };
    case "end-turn":
      return { type, commandId, turnId, playerId };
    default:
      return null;
  }
}
