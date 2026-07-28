import type {
  BoardResponse,
  BoardResponseV2,
  AgentSeatRequest,
  AgentSeatResponse,
  CommandAck,
  CreateGameRequest,
  CreateGameResponse,
  GameResponse,
  JoinGameRequest,
  JoinGameResponse,
  PlayCommandRequest,
} from "../../src/application/api.ts";
import { agentPlayInstructions, agentSeatDescriptor } from "../../src/application/agent-play.ts";
import type { Command } from "../../src/domain/commands.ts";
import type { CommandV2, GameActionV2, PlayCommandV2 } from "../../src/domain/commands-v2.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import { foldAggregateV2 } from "../../src/domain/aggregate-v2.ts";
import { buildDecisionContext } from "../../src/application/decision.ts";
import { buildDecisionContextV2 } from "../../src/application/decision-v2.ts";
import { MAP_VERSION, RULESET } from "../../src/domain/map.ts";
import { MAP_VERSION_V2, RULESET_V2, generateMapSeed } from "../../src/domain/map-v2.ts";
import type { PlayerController } from "../../src/domain/events-v2.ts";
import { projectionBoardView } from "../../src/board/projection.ts";
import { projectionBoardViewV2 } from "../../src/board/projection-v2.ts";
import { BOARD_REDUCER_VERSION } from "../../src/board/board-projection.ts";
import { BOARD_REDUCER_VERSION_V2 } from "../../src/board/board-projection-v2.ts";
import { materializeBoard } from "../game/board.ts";
import { materializeBoardV2 } from "../game/board-v2.ts";
import {
  assertRulesetMatches,
  isRulesetV2,
  readCanonical,
  readCanonicalV2,
  readCanonicalV2Through,
  submitCommand,
  submitCommandV2,
  type SubmitResult,
  type SubmitResultV2,
} from "../game/command-service.ts";
import { error, json, readJsonBody, statusForCode, type ErrorCode, type Route } from "./router.ts";
import {
  BOARD_GENERATION,
  BOARD_GENERATION_V2,
  boardStreamId,
  eventStreamId,
} from "../game/names.ts";
import { openApiDocument } from "./openapi.ts";
import { catchUpActions, readActions } from "../game/action-notifier.ts";
import type { AppContext } from "./app.ts";

function randomId(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  return `${prefix}_${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

type AnyAccepted = Extract<SubmitResult | SubmitResultV2, { status: "accepted" | "duplicate" }>;
type AnyRejected = Extract<SubmitResult | SubmitResultV2, { status: "rejected" }>;

function ackBody(result: AnyAccepted, turnId?: string): CommandAck {
  return { ...result, ...(turnId ? { turnId } : {}) };
}

/**
 * The colour a seat was canonically issued. The v2 decider assigns colours
 * conflict-safely, so the response must report the recorded event's colour
 * rather than echoing whatever the request happened to ask for.
 */
function assignedSeatColor(result: AnyAccepted, fallback: string): string {
  for (const event of result.events) {
    if (event.type === "GameCreated" && "hostColor" in event) return event.hostColor;
    if (event.type === "PlayerJoined") return event.color;
  }
  return fallback;
}

const STATE_GUIDANCE = "Read your actions stream again and act on the newest message.";

function rejection(result: AnyRejected, guideToState = false): Response {
  const code = result.error.code as ErrorCode;
  const extra = result.error.currentTurnId ? { currentTurnId: result.error.currentTurnId } : {};
  const message = guideToState ? `${result.error.message} ${STATE_GUIDANCE}` : result.error.message;
  return error(statusForCode(code), code, message, extra);
}

function controllerOf(value: unknown): PlayerController {
  if (value === "agent") return "external-agent";
  if (value === "bot") return "bot";
  return "human";
}

export function createRiskRoutes(ctx: AppContext): Route[] {
  const rulesetOf = (gameId: string): string => ctx.stores.games.get(gameId)?.ruleset ?? RULESET;

  /** Catch a game's board projection up to the canonical head, per ruleset. */
  const syncBoard = (gameId: string) =>
    materializeBoard(ctx.protocol, ctx.boardCache, gameId, ctx.activeGeneration(gameId));
  const syncBoardV2 = (gameId: string) =>
    materializeBoardV2(ctx.protocol, ctx.boardCacheV2, gameId, ctx.activeGeneration(gameId));

  /** After any accepted command: derive wakes, and reconcile v2 defence timers. */
  async function afterCommand(gameId: string, ruleset: string): Promise<void> {
    if (isRulesetV2(ruleset)) {
      // Agent seats do not make a dice decision. Resolve their defence first,
      // then materialize/wake from the complete canonical result.
      await ctx.defenseTimers.ensure(gameId);
      await Promise.all([catchUpActions(ctx.protocol, gameId), syncBoardV2(gameId)]);
      return;
    }
    await syncBoard(gameId);
  }

  async function createGame(request: Request): Promise<Response> {
    const body = (await readJsonBody<CreateGameRequest>(request)) ?? {};
    const caller = await ctx.authenticateCapability(request);
    if (caller?.role === "agent") {
      return error(403, "FORBIDDEN", "Agent capabilities cannot create games.");
    }
    if (body.controller === "agent") {
      return error(
        403,
        "AGENT_SEAT_REQUIRES_HOST",
        "Create a human-hosted game, then open agent seats with POST /agent-seats.",
      );
    }
    const name = body.name ?? "Host";
    const gameId = ctx.createGameId();
    const hostPlayerId = randomId("p");
    const commandId = body.commandId ?? randomId("cmd");
    // New games are `risk-demo-v2` (design spec §11). V1 is not migrated and not
    // reinterpreted — it stays selectable by name so existing demo fixtures and
    // v1-subject tests keep exercising the v1 kernel, renderer, and projection.
    const wantsV2 = (body.ruleset ?? RULESET_V2) === RULESET_V2;
    const ruleset = wantsV2 ? RULESET_V2 : RULESET;

    const result = wantsV2
      ? await submitCommandV2(ctx.commandService, eventStreamId(gameId), {
          type: "create-game",
          commandId,
          gameId,
          hostPlayerId,
          hostName: name,
          // Optional: the v2 decider honours a free requested colour and
          // otherwise issues the first available palette colour.
          hostColor: body.color,
          hostController: controllerOf(body.controller),
          // Production lets the server mint the seed; a demo or test may pin one.
          mapSeed: body.mapSeed ?? generateMapSeed(ctx.commandService.rng),
        })
      : await submitCommand(ctx.commandService, eventStreamId(gameId), {
          type: "create-game",
          commandId,
          gameId,
          hostPlayerId,
          hostName: name,
          // The v1 kernel is not migrated: it keeps its fixed request default.
          hostColor: body.color ?? "#e05a47",
        });
    if (result.status === "rejected") return rejection(result);

    // Each ruleset gets its own generation lineage and reducer version, so a v2
    // board is always a *new* projection stream rather than a reinterpretation
    // of v1 projection history.
    const generation = wantsV2 ? BOARD_GENERATION_V2 : BOARD_GENERATION;
    ctx.stores.games.put({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      projectionStreamId: boardStreamId(gameId, generation),
      generation,
      ruleset,
      createdAt: ctx.now(),
    });
    ctx.stores.generations.put({
      gameId,
      generation,
      streamId: boardStreamId(gameId, generation),
      reducerVersion: wantsV2 ? BOARD_REDUCER_VERSION_V2 : BOARD_REDUCER_VERSION,
      status: "active",
      sourceThroughOffset: null,
      createdAt: ctx.now(),
    });
    if (wantsV2) await syncBoardV2(gameId);
    else await syncBoard(gameId);
    const capability = await ctx.issueAndStore(gameId, hostPlayerId, "host");
    const response: CreateGameResponse = {
      game: {
        id: gameId,
        ruleset,
        mapVersion: wantsV2 ? MAP_VERSION_V2 : MAP_VERSION,
      },
      player: {
        id: hostPlayerId,
        name,
        color: assignedSeatColor(result, body.color ?? "#e05a47"),
        role: "host",
      },
      capability,
      ack: ackBody(result),
    };
    return json(response, 201);
  }

  async function joinGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    if (!ctx.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const body = (await readJsonBody<JoinGameRequest>(request)) ?? {};
    const caller = await ctx.authenticateCapability(request);
    if (caller?.role === "agent") {
      return error(403, "FORBIDDEN", "Agent capabilities cannot join games.");
    }
    if (body.controller === "agent") {
      return error(
        403,
        "AGENT_SEAT_REQUIRES_HOST",
        "The host must open agent seats with POST /agent-seats.",
      );
    }
    const name = body.name ?? "Player";
    const playerId = randomId("p");
    const commandId = body.commandId ?? randomId("cmd");
    const ruleset = rulesetOf(gameId);

    const result = isRulesetV2(ruleset)
      ? await submitCommandV2(ctx.commandService, eventStreamId(gameId), {
          type: "join-game",
          commandId,
          playerId,
          name,
          // Optional: the v2 decider honours a free requested colour and
          // otherwise issues the first available palette colour.
          color: body.color,
          controller: controllerOf(body.controller),
        })
      : await submitCommand(ctx.commandService, eventStreamId(gameId), {
          type: "join-game",
          commandId,
          playerId,
          name,
          // The v1 kernel is not migrated: it keeps its fixed request default.
          color: body.color ?? "#3b82f6",
        });
    if (result.status === "rejected") return rejection(result);
    if (isRulesetV2(ruleset)) await syncBoardV2(gameId);
    else await syncBoard(gameId);
    const capability = await ctx.issueAndStore(gameId, playerId, "player");
    const response: JoinGameResponse = {
      player: {
        id: playerId,
        name,
        color: assignedSeatColor(result, body.color ?? "#3b82f6"),
        role: "player",
      },
      capability,
      ack: ackBody(result),
    };
    return json(response, 201);
  }

  async function startGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId, "host");
    if (cap instanceof Response) return cap;
    const body = (await readJsonBody<{ commandId?: string }>(request)) ?? {};
    const commandId = body.commandId ?? randomId("cmd");
    const ruleset = rulesetOf(gameId);
    // The v2 map is generated inside `decideV2` — after the command log has
    // deduped `commandId` and before the canonical append — so a start that
    // loses its CAS refolds and is rejected as already started, never regenerated.
    const result = isRulesetV2(ruleset)
      ? await submitCommandV2(ctx.commandService, eventStreamId(gameId), {
          type: "start-game",
          commandId,
        })
      : await submitCommand(ctx.commandService, eventStreamId(gameId), {
          type: "start-game",
          commandId,
        });
    if (result.status === "rejected") return rejection(result);
    await afterCommand(gameId, ruleset);
    return json(ackBody(result));
  }

  async function createAgentSeat(
    request: Request,
    params: Record<string, string>,
  ): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId, "host");
    if (cap instanceof Response) return cap;
    const game = ctx.stores.games.get(gameId);
    if (!game || !isRulesetV2(game.ruleset)) {
      return error(400, "BAD_REQUEST", "Agent seats are available for v2 games only.");
    }
    const body = (await readJsonBody<AgentSeatRequest>(request)) ?? {};
    const commandId = body.commandId ?? randomId("cmd");
    let playerId = body.playerId;
    let name = body.name ?? "Agent";
    let color = body.color ?? "";

    if (playerId) {
      const { events } = await readCanonicalV2(ctx.protocol, eventStreamId(gameId));
      const state = foldAggregateV2(events);
      const player = state.players.find((candidate) => candidate.id === playerId);
      if (!player) return error(404, "NOT_FOUND", "That seat is not part of this game.");
      const delegated = await submitCommandV2(ctx.commandService, eventStreamId(gameId), {
        type: "delegate-agent-seat",
        commandId,
        playerId,
      });
      if (delegated.status === "rejected") return rejection(delegated);
      name = player.name;
      color = player.color;
    } else {
      playerId = randomId("p");
      const joined = await submitCommandV2(ctx.commandService, eventStreamId(gameId), {
        type: "join-game",
        commandId,
        playerId,
        name,
        color: body.color,
        controller: "external-agent",
      });
      if (joined.status === "rejected") return rejection(joined);
      color = assignedSeatColor(joined, color);
    }

    await syncBoardV2(gameId);
    const token = await ctx.issueAndStore(gameId, playerId, "agent");
    const input = {
      origin: new URL(request.url).origin,
      gameId,
      playerId,
      name,
      color,
      token,
    };
    const response: AgentSeatResponse = {
      seat: agentSeatDescriptor(input),
      instructions: agentPlayInstructions(input),
    };
    return json(response, 201, {
      "cache-control": "no-store",
      "referrer-policy": "no-referrer",
    });
  }

  async function getGame(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const game = ctx.stores.games.get(gameId);
    if (!game) return error(404, "GAME_NOT_FOUND", "Unknown game.");

    if (isRulesetV2(game.ruleset)) {
      await syncBoardV2(gameId);
      const { events } = await readCanonicalV2(ctx.protocol, eventStreamId(gameId));
      const state = foldAggregateV2(events);
      assertRulesetMatches(gameId, game.ruleset, events);
      const response: GameResponse = {
        gameId,
        status: state.status,
        ruleset: RULESET_V2,
        mapVersion: MAP_VERSION_V2,
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
        ...(state.pendingInteraction ? { pendingInteraction: state.pendingInteraction } : {}),
      };
      return json(response);
    }

    await syncBoard(gameId);
    const { events } = await readCanonical(ctx.protocol, eventStreamId(gameId));
    const state = foldAggregate(events);
    assertRulesetMatches(gameId, game.ruleset, events);
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
    const game = ctx.stores.games.get(gameId);
    if (!game) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    if (isRulesetV2(game.ruleset)) {
      const board = await syncBoardV2(gameId);
      const response: BoardResponseV2 = {
        gameId,
        ruleset: RULESET_V2,
        sourceStreamId: board.sourceStreamId,
        sourceThroughOffset: board.sourceThroughOffset,
        generation: board.generation,
        boardStreamId: boardStreamId(gameId, board.generation),
        reducerVersion: BOARD_REDUCER_VERSION_V2,
        game: board.state.game,
        players: board.state.players,
        hexes: board.state.hexes,
        territories: board.state.territories,
        continents: board.state.continents,
        turn: board.state.turn,
        combat: board.state.combat,
        moves: board.state.moves,
        view: projectionBoardViewV2(board.state),
      };
      return json(response);
    }
    const board = await syncBoard(gameId);
    const response: BoardResponse = {
      gameId,
      ruleset: RULESET,
      sourceStreamId: board.sourceStreamId,
      sourceThroughOffset: board.sourceThroughOffset,
      generation: board.generation,
      game: board.state.game,
      players: board.state.players,
      territories: board.state.territories,
      view: projectionBoardView(board.state),
    };
    return json(response);
  }

  async function getMap(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const game = ctx.stores.games.get(gameId);
    if (!game) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    if (!isRulesetV2(game.ruleset)) {
      return error(
        400,
        "BAD_REQUEST",
        "The immutable map resource is available for v2 games only.",
      );
    }
    const { events } = await readCanonicalV2(ctx.protocol, eventStreamId(gameId));
    const state = foldAggregateV2(events);
    if (!state.map) return error(409, "GAME_NOT_STARTED", "The map is available after game start.");
    return json(
      {
        gameId,
        mapVersion: state.mapVersion,
        generatorVersion: state.generatorVersion,
        seed: state.mapSeed,
        territories: state.map.territories.map((territory) => ({
          id: territory.id,
          name: territory.name,
          neighbours: territory.adjacentTerritoryIds,
          continentId: territory.continentId,
        })),
        continents: state.map.continents.map((continent) => ({
          id: continent.id,
          name: continent.name,
          territoryIds: continent.territoryIds,
          reinforcementBonus: continent.reinforcementBonus,
        })),
      },
      200,
      { "cache-control": "public, max-age=31536000, immutable" },
    );
  }

  async function getDecision(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const ruleset = rulesetOf(gameId);

    if (isRulesetV2(ruleset)) {
      // Project first, then fold exactly the canonical prefix that projection has
      // incorporated: the decision is never ahead of the board snapshot it names
      // (design spec §6.1, and see `decision-v2.ts` for the full stance).
      const board = await syncBoardV2(gameId);
      const { events } = await readCanonicalV2Through(
        ctx.protocol,
        eventStreamId(gameId),
        board.sourceThroughOffset,
      );
      assertRulesetMatches(gameId, ruleset, events);
      const state = foldAggregateV2(events);
      if (!state.players.some((player) => player.id === cap.playerId)) {
        return error(404, "NOT_FOUND", "Player is not part of this game.");
      }
      return json(
        buildDecisionContextV2(state, cap.playerId, {
          sourceStreamId: board.sourceStreamId,
          sourceThroughOffset: board.sourceThroughOffset,
          generation: board.generation,
          boardStreamId: boardStreamId(gameId, board.generation),
        }),
      );
    }

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
    const ruleset = rulesetOf(gameId);
    const raw = await readJsonBody<unknown>(request);

    if (isRulesetV2(ruleset)) {
      const parsed = buildPlayCommandV2(raw, cap.playerId);
      if (!parsed.ok)
        return error(400, "INVALID_ACTION", `Command validation failed. ${STATE_GUIDANCE}`, {
          details: parsed.details,
        });
      const result = await submitCommandV2(
        ctx.commandService,
        eventStreamId(gameId),
        parsed.value.command,
      );
      if (result.status === "rejected") return rejection(result, true);
      await afterCommand(gameId, ruleset);
      return json(ackBody(result, parsed.value.body.turnId), 200, {
        "cache-control": "no-store",
        "referrer-policy": "no-referrer",
      });
    }

    const parsed = buildPlayCommand(raw, cap.playerId);
    if (!parsed) return error(400, "BAD_REQUEST", "Body must be { commandId, turnId, action }.");
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), parsed.command);
    if (result.status === "rejected") return rejection(result, true);
    await afterCommand(gameId, ruleset);
    return json(ackBody(result, parsed.body.turnId));
  }

  async function getActions(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const url = new URL(request.url);
    if (url.searchParams.has("cursor")) {
      return error(400, "BAD_REQUEST", "Use the offset query parameter.");
    }
    await catchUpActions(ctx.protocol, gameId);
    const requested = Number.parseInt(url.searchParams.get("wait") ?? "0", 10);
    const waitMs = Number.isFinite(requested) ? Math.max(0, Math.min(requested, 30_000)) : 0;
    return json(
      await readActions(ctx.protocol, gameId, cap.playerId, {
        cursor: url.searchParams.get("offset") ?? undefined,
        waitMs,
        signal: request.signal,
      }),
      200,
      { "cache-control": "no-store", "referrer-policy": "no-referrer" },
    );
  }

  return [
    {
      method: "GET",
      pattern: "/",
      handler: () => json({ name: "risk-demo", ok: true }),
    },
    {
      method: "GET",
      pattern: "/openapi.json",
      handler: () => json(openApiDocument),
    },
    { method: "POST", pattern: "/v1/games", handler: createGame },
    { method: "POST", pattern: "/v1/games/:gameId/players", handler: joinGame },
    { method: "POST", pattern: "/v1/games/:gameId/agent-seats", handler: createAgentSeat },
    { method: "POST", pattern: "/v1/games/:gameId/start", handler: startGame },
    { method: "GET", pattern: "/v1/games/:gameId", handler: getGame },
    { method: "GET", pattern: "/v1/games/:gameId/board", handler: getBoard },
    { method: "GET", pattern: "/v1/games/:gameId/map", handler: getMap },
    {
      method: "GET",
      pattern: "/v1/games/:gameId/decision",
      handler: getDecision,
    },
    {
      method: "POST",
      pattern: "/v1/games/:gameId/commands",
      handler: postCommand,
    },
    {
      method: "GET",
      pattern: "/v1/games/:gameId/players/me/actions",
      handler: getActions,
    },
  ];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

interface Envelope {
  commandId: string;
  turnId: string;
  action: Record<string, unknown>;
}

function readEnvelope(value: unknown): Envelope | null {
  if (!isRecord(value) || typeof value.commandId !== "string" || typeof value.turnId !== "string") {
    return null;
  }
  const action = value.action;
  if (!isRecord(action) || typeof action.type !== "string") return null;
  return { commandId: value.commandId, turnId: value.turnId, action };
}

function buildPlayCommand(
  value: unknown,
  playerId: string,
): { body: PlayCommandRequest; command: Command } | null {
  const envelope = readEnvelope(value);
  if (!envelope) return null;
  const { commandId, turnId, action } = envelope;
  const common = { commandId, turnId, playerId };
  let body: PlayCommandRequest;
  switch (action.type) {
    case "reinforce":
      if (typeof action.territoryId !== "string" || typeof action.armies !== "number") return null;
      body = {
        commandId,
        turnId,
        action: {
          type: "reinforce",
          territoryId: action.territoryId,
          armies: action.armies,
        },
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
        commandId,
        turnId,
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
        commandId,
        turnId,
        action: {
          type: "fortify",
          from: action.from,
          to: action.to,
          armies: action.armies,
        },
      };
      break;
    case "end-turn":
      body = { commandId, turnId, action: { type: "end-turn" } };
      break;
    default:
      return null;
  }
  return { body, command: { ...common, ...body.action } };
}

/**
 * Parse a v2 play command. `resolve-defense-timeout` is deliberately absent: it
 * is authorized by the game service, not by a player capability, so there is no
 * transport path by which a player could submit one.
 */
type ValidationDetail = { path: string; expected: string; received: unknown };
type ParsedPlayCommandV2 =
  | { ok: true; value: { body: PlayCommandV2; command: CommandV2 } }
  | { ok: false; details: ValidationDetail[] };

/** One malformed field, in the shape `INVALID_ACTION` publishes to the agent. */
function invalid(path: string, expected: string, received: unknown): ParsedPlayCommandV2 {
  return { ok: false, details: [{ path, expected, received }] };
}

function buildPlayCommandV2(value: unknown, playerId: string): ParsedPlayCommandV2 {
  const envelope = readEnvelope(value);
  if (!envelope) {
    return {
      ok: false,
      details: [
        {
          path: "body",
          expected: "{ commandId: string, turnId: string, action: object }",
          received: value,
        },
      ],
    };
  }
  const { commandId, turnId, action } = envelope;

  let parsed: GameActionV2;
  switch (action.type) {
    case "reinforce": {
      if (!Array.isArray(action.placements))
        return invalid("action.placements", "array", action.placements);
      const placements = action.placements.map((placement) => {
        if (
          !isRecord(placement) ||
          typeof placement.territoryId !== "string" ||
          typeof placement.armies !== "number"
        ) {
          return null;
        }
        return { territoryId: placement.territoryId, armies: placement.armies };
      });
      const invalidIndex = placements.findIndex((placement) => placement === null);
      if (invalidIndex >= 0) {
        return invalid(
          `action.placements[${invalidIndex}]`,
          "{ territoryId: string, armies: integer >= 1 }",
          action.placements[invalidIndex],
        );
      }
      const invalidArmies = placements.findIndex(
        (placement) =>
          placement !== null && (!Number.isInteger(placement.armies) || placement.armies < 1),
      );
      if (invalidArmies >= 0) {
        return invalid(
          `action.placements[${invalidArmies}].armies`,
          "integer >= 1",
          (action.placements[invalidArmies] as Record<string, unknown>).armies,
        );
      }
      parsed = {
        type: "reinforce",
        placements: placements as Array<{ territoryId: string; armies: number }>,
      };
      break;
    }
    case "declare-attack":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        !Number.isInteger(action.attackerDice) ||
        (action.attackerDice as number) < 1
      ) {
        return invalid(
          "action",
          "{ type: declare-attack, from: string, to: string, attackerDice: integer }",
          action,
        );
      }
      parsed = {
        type: "declare-attack",
        from: action.from,
        to: action.to,
        attackerDice: action.attackerDice as number,
      };
      break;
    case "roll-defense":
      if (typeof action.attackId !== "string")
        return invalid("action.attackId", "string", action.attackId);
      parsed = { type: "roll-defense", attackId: action.attackId };
      break;
    case "occupy-territory":
      if (typeof action.attackId !== "string")
        return invalid("action.attackId", "string", action.attackId);
      if (!Number.isInteger(action.armies) || (action.armies as number) < 1)
        return invalid("action.armies", "integer >= 1", action.armies);
      parsed = {
        type: "occupy-territory",
        attackId: action.attackId,
        armies: action.armies as number,
      };
      break;
    case "fortify":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        !Number.isInteger(action.armies) ||
        (action.armies as number) < 1
      ) {
        return invalid(
          "action",
          "{ type: fortify, from: string, to: string, armies: integer }",
          action,
        );
      }
      parsed = {
        type: "fortify",
        from: action.from,
        to: action.to,
        armies: action.armies as number,
      };
      break;
    case "skip-fortifications":
      parsed = { type: "skip-fortifications" };
      break;
    default:
      return invalid(
        "action.type",
        "reinforce | declare-attack | roll-defense | occupy-territory | fortify | skip-fortifications",
        action.type,
      );
  }

  const body: PlayCommandV2 = { commandId, turnId, action: parsed };
  // Nothing about *who* resolved a combat is taken from the transport: the kernel
  // derives human/bot/agent attribution from the defending seat's canonical
  // controller, so a client cannot mislabel its own roll.
  const command: CommandV2 = { commandId, turnId, playerId, ...parsed };
  return { ok: true, value: { body, command } };
}
