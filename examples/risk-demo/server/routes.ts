import type {
  CommandAck,
  CreateGameRequest,
  CreateGameResponse,
  GameResponse,
  JoinGameRequest,
  JoinGameResponse,
  PlayCommandRequest,
} from "../src/api.ts";
import type { Command } from "../src/commands.ts";
import { foldAggregate } from "../src/aggregate.ts";
import { buildDecisionContext } from "../src/decision.ts";
import { MAP_VERSION, RULESET } from "../src/map.ts";
import { projectionBoardView } from "../src/projection.ts";
import { BOARD_REDUCER_VERSION } from "../src/materializer/board-projection.ts";
import { materializeBoard } from "./board.ts";
import {
  readCanonical,
  recoverCommand,
  submitCommand,
  type SubmitResult,
} from "./command-service.ts";
import { error, json, readJsonBody, statusForCode, type ErrorCode, type Route } from "./http.ts";
import { BOARD_GENERATION, boardStreamId, eventStreamId } from "./names.ts";
import { boardProjectionTxId } from "../src/transaction.ts";
import { openApiDocument } from "./openapi.ts";
import { catchUpTurns, readTurns } from "./turn-notifier.ts";
import type { AppContext } from "./app.ts";

function randomId(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  return `${prefix}_${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

function ackBody(
  result: Extract<SubmitResult, { status: "accepted" | "duplicate" }>,
  turnId?: string,
): CommandAck {
  return { ...result, ...(turnId ? { turnId } : {}) };
}

function rejection(result: Extract<SubmitResult, { status: "rejected" }>): Response {
  const code = result.error.code as ErrorCode;
  const extra = result.error.currentTurnId ? { currentTurnId: result.error.currentTurnId } : {};
  return error(statusForCode(code), code, result.error.message, extra);
}

export function createRiskRoutes(ctx: AppContext): Route[] {
  const syncBoard = (gameId: string) =>
    materializeBoard(ctx.protocol, ctx.boardCache, gameId, ctx.activeGeneration(gameId));

  async function createGame(request: Request): Promise<Response> {
    const body = (await readJsonBody<CreateGameRequest>(request)) ?? {};
    const name = body.name ?? "Host";
    const color = body.color ?? "#e05a47";
    const gameId = randomId("game");
    const hostPlayerId = randomId("p");
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "create-game",
      commandId: body.commandId ?? randomId("cmd"),
      gameId,
      hostPlayerId,
      hostName: name,
      hostColor: color,
    });
    if (result.status === "rejected") return rejection(result);

    ctx.stores.games.put({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      projectionStreamId: boardStreamId(gameId),
      generation: BOARD_GENERATION,
      createdAt: ctx.now(),
    });
    ctx.stores.generations.put({
      gameId,
      generation: BOARD_GENERATION,
      streamId: boardStreamId(gameId),
      reducerVersion: BOARD_REDUCER_VERSION,
      status: "active",
      sourceThroughOffset: null,
      createdAt: ctx.now(),
    });
    await syncBoard(gameId);
    const capability = await ctx.issueAndStore(gameId, hostPlayerId, "host");
    const response: CreateGameResponse = {
      game: { id: gameId, ruleset: RULESET, mapVersion: MAP_VERSION },
      player: { id: hostPlayerId, name, color, role: "host" },
      capability,
      ack: ackBody(result),
    };
    return json(response, 201);
  }

  async function joinGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    if (!ctx.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const body = (await readJsonBody<JoinGameRequest>(request)) ?? {};
    const name = body.name ?? "Player";
    const color = body.color ?? "#3b82f6";
    const playerId = randomId("p");
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "join-game",
      commandId: body.commandId ?? randomId("cmd"),
      playerId,
      name,
      color,
    });
    if (result.status === "rejected") return rejection(result);
    await syncBoard(gameId);
    const response: JoinGameResponse = {
      player: { id: playerId, name, color, role: "player" },
      capability: await ctx.issueAndStore(gameId, playerId, "player"),
      ack: ackBody(result),
    };
    return json(response, 201);
  }

  async function startGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId, "host");
    if (cap instanceof Response) return cap;
    const body = (await readJsonBody<{ commandId?: string }>(request)) ?? {};
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "start-game",
      commandId: body.commandId ?? randomId("cmd"),
    });
    if (result.status === "rejected") return rejection(result);
    await Promise.all([catchUpTurns(ctx.protocol, gameId), syncBoard(gameId)]);
    return json(ackBody(result));
  }

  async function getGame(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const game = ctx.stores.games.get(gameId);
    if (!game) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    await syncBoard(gameId);
    const { events } = await readCanonical(ctx.protocol, eventStreamId(gameId));
    const state = foldAggregate(events);
    const response: GameResponse = {
      gameId,
      status: state.status,
      ruleset: RULESET,
      mapVersion: MAP_VERSION,
      round: state.round,
      activePlayerId: state.activePlayerId,
      phase: state.phase,
      winnerId: state.winnerId,
      generation: game.generation,
      boardStreamId: boardStreamId(gameId, game.generation),
      players: state.players.map(({ id, name, color, eliminated }) => ({
        id,
        name,
        color,
        eliminated,
      })),
    };
    return json(response);
  }

  async function getBoard(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    if (!ctx.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const board = await syncBoard(gameId);
    return json({
      gameId,
      sourceStreamId: board.sourceStreamId,
      sourceThroughOffset: board.sourceThroughOffset,
      generation: board.generation,
      game: board.state.game,
      players: board.state.players,
      territories: board.state.territories,
      view: projectionBoardView(board.state),
    });
  }

  async function getDecision(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const { events } = await readCanonical(ctx.protocol, eventStreamId(gameId));
    const state = foldAggregate(events);
    if (!state.players.some((player) => player.id === cap.playerId)) {
      return error(404, "NOT_FOUND", "Player is not part of this game.");
    }
    const board = await syncBoard(gameId);
    return json(
      buildDecisionContext(state, cap.playerId, {
        sourceStreamId: board.sourceStreamId,
        sourceThroughOffset: board.sourceThroughOffset,
      }),
    );
  }

  async function postCommand(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const parsed = buildPlayCommand(await readJsonBody<unknown>(request), cap.playerId);
    if (!parsed) {
      return error(400, "BAD_REQUEST", "Body must be { commandId, turnId, action }.");
    }
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), parsed.command);
    if (result.status === "rejected") return rejection(result);
    await Promise.all([catchUpTurns(ctx.protocol, gameId), syncBoard(gameId)]);
    return json(ackBody(result, parsed.body.turnId));
  }

  async function getTurns(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const url = new URL(request.url);
    await catchUpTurns(ctx.protocol, gameId);
    return json(
      await readTurns(ctx.protocol, gameId, cap.playerId, {
        cursor: url.searchParams.get("offset") ?? url.searchParams.get("cursor") ?? undefined,
        waitMs: Number.parseInt(url.searchParams.get("wait") ?? "0", 10) || 0,
        signal: request.signal,
      }),
    );
  }

  async function getCommand(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const commandId = params.commandId!;
    const row = await recoverCommand(ctx.commandService, eventStreamId(gameId), gameId, commandId);
    if (!row) return error(404, "NOT_FOUND", "Unknown command.");
    if (row.status === "accepted") {
      if (!row.sourceOffset) return error(500, "INTERNAL", "Accepted command has no offset.");
      return json({
        status: "accepted",
        commandId,
        sourceStreamId: eventStreamId(gameId),
        sourceOffset: row.sourceOffset,
        txid: boardProjectionTxId(commandId, row.sourceOffset),
        events: row.events,
      });
    }
    const code = (row.error?.code ?? "ILLEGAL_ACTION") as ErrorCode;
    return error(statusForCode(code), code, row.error?.message ?? "rejected");
  }

  return [
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
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function buildPlayCommand(
  value: unknown,
  playerId: string,
): { body: PlayCommandRequest; command: Command } | null {
  if (!isRecord(value) || typeof value.commandId !== "string" || typeof value.turnId !== "string") {
    return null;
  }
  const action = value.action;
  if (!isRecord(action) || typeof action.type !== "string") return null;
  const common = { commandId: value.commandId, turnId: value.turnId, playerId };
  let body: PlayCommandRequest;
  switch (action.type) {
    case "reinforce":
      if (typeof action.territoryId !== "string" || typeof action.armies !== "number") return null;
      body = {
        commandId: value.commandId,
        turnId: value.turnId,
        action: { type: "reinforce", territoryId: action.territoryId, armies: action.armies },
      };
      break;
    case "attack":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        typeof action.attackerDice !== "number"
      ) {
        return null;
      }
      body = {
        commandId: value.commandId,
        turnId: value.turnId,
        action: {
          type: "attack",
          from: action.from,
          to: action.to,
          attackerDice: action.attackerDice,
        },
      };
      break;
    case "fortify":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        typeof action.armies !== "number"
      ) {
        return null;
      }
      body = {
        commandId: value.commandId,
        turnId: value.turnId,
        action: { type: "fortify", from: action.from, to: action.to, armies: action.armies },
      };
      break;
    case "end-turn":
      body = { commandId: value.commandId, turnId: value.turnId, action: { type: "end-turn" } };
      break;
    default:
      return null;
  }
  return { body, command: { ...common, ...body.action } };
}
