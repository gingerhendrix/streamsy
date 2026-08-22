/* oxlint-disable effecttsgo/async-function -- Web-standard fetch handlers are Promise-native framework adapters; they delegate game and projection work to the existing application services and runtime. */
import type {
  BoardResponse,
  AgentSeatResponse,
  CommandAck,
  CreateGameResponse,
  GameResponse,
  JoinGameResponse,
  LeaveGameResponse,
  RenamePlayerResponse,
} from "../../src/application/api.ts";
import { agentPlayInstructions, agentSeatDescriptor } from "../../src/application/agent-play.ts";
import type { Command, GameAction, PlayCommand } from "../../src/domain/commands.ts";
import { foldAggregate } from "../../src/domain/aggregate.ts";
import { buildDecisionContext } from "../../src/application/decision.ts";
import { MAP_VERSION, generateMapSeed } from "../../src/domain/map.ts";
import type { GameEvent, PlayerController } from "../../src/domain/events.ts";
import { normalizePlayerName } from "../../src/domain/decide.ts";
import { projectionBoardView } from "../../src/board/projection.ts";
import { BOARD_REDUCER_VERSION } from "../../src/board/board-projection.ts";
import { materializeBoard } from "../game/board.ts";
import {
  readCanonical,
  readCanonicalThrough,
  submitCommand,
  type SubmitResult,
} from "../game/command-service.ts";
import { error, json, readJsonBody, statusForCode, type ErrorCode, type Route } from "./router.ts";
import { BOARD_GENERATION, boardStreamId, eventStreamId } from "../game/names.ts";
import { openApiDocument } from "./openapi.ts";
import { catchUpActions, readActions } from "../game/action-notifier.ts";
import { actionsStreamResponse } from "./actions-sse.ts";
import { prefersJsonOverEventStream } from "./accept.ts";
import type { AppContext } from "./app.ts";

function randomId(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(6));
  return `${prefix}_${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

type AnyAccepted = Extract<SubmitResult, { status: "accepted" | "duplicate" }>;
type AnyRejected = Extract<SubmitResult, { status: "rejected" }>;

/**
 * The ack says only that the command was recorded, and where.
 * It is deliberately *not* the outcome: dice, captures and phase changes reach a
 * player on their actions stream, and a browser recomputes the board-projection
 * transaction id it waits on from `commandId` + `eventOffset`. An ack that
 * carried a partial `events` array invited clients to treat one command's slice
 * of canonical history as the whole result of the move.
 */
function ackBody(result: AnyAccepted, turnId?: string): CommandAck {
  return {
    status: result.status,
    commandId: result.commandId,
    ...(turnId ? { turnId } : {}),
    eventOffset: result.sourceOffset,
  };
}

/**
 * The colour a seat was canonically issued. The current decider assigns colours
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

/**
 * Agent seats are minted in exactly one place. The public vocabulary for one is
 * `agent`, but the canonical event vocabulary is `external-agent` — so an
 * unauthenticated create/join must refuse *both* spellings rather than letting
 * the internal one fall through `controllerOf`'s default to a human seat.
 */
function requestsAgentSeat(value: unknown): boolean {
  return value === "agent" || value === "external-agent";
}

export function createRiskRoutes(ctx: AppContext): Route[] {
  /** Catch a game's board projection up to the canonical head. */
  const syncBoard = (gameId: string) =>
    materializeBoard(ctx.protocol, ctx.boardCache, gameId, ctx.activeGeneration(gameId));

  /** After any accepted command: reconcile defence timers and update derived views. */
  async function afterCommand(gameId: string): Promise<void> {
    // Agent seats do not make a dice decision. Resolve their defence first,
    // then materialize/wake from the complete canonical result.
    await ctx.defenseTimers.ensure(gameId);
    await Promise.all([catchUpActions(ctx.protocol, gameId), syncBoard(gameId)]);
  }

  async function createGame(request: Request): Promise<Response> {
    const body = (await readJsonBody(request)) ?? {};
    const caller = await ctx.authenticateCapability(request);
    if (caller?.role === "agent") {
      return error(403, "FORBIDDEN", "Agent capabilities cannot create games.");
    }
    if (requestsAgentSeat(body.controller)) {
      return error(
        403,
        "AGENT_SEAT_REQUIRES_HOST",
        "Create a human-hosted game, then open agent seats with POST /agent-seats.",
      );
    }
    // The landing page asks for no name at all — the creator names themselves in
    // the lobby — so the provisional default is load-bearing, not a fallback for
    // a field somebody left blank.
    const requestedName = typeof body.name === "string" ? body.name : "";
    const requestedColor = typeof body.color === "string" ? body.color : undefined;
    const name = normalizePlayerName(requestedName) || "Host";
    const gameId = ctx.createGameId();
    const hostPlayerId = randomId("p");
    const commandId = typeof body.commandId === "string" ? body.commandId : randomId("cmd");
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "create-game",
      commandId,
      gameId,
      hostPlayerId,
      hostName: name,
      hostColor: requestedColor,
      hostController: controllerOf(body.controller),
      mapSeed:
        typeof body.mapSeed === "string" ? body.mapSeed : generateMapSeed(ctx.commandService.rng),
    });
    if (result.status === "rejected") return rejection(result);

    const generation = BOARD_GENERATION;
    ctx.stores.games.put({
      gameId,
      sourceStreamId: eventStreamId(gameId),
      projectionStreamId: boardStreamId(gameId, generation),
      generation,
      createdAt: ctx.now(),
    });
    ctx.stores.generations.put({
      gameId,
      generation,
      streamId: boardStreamId(gameId, generation),
      reducerVersion: BOARD_REDUCER_VERSION,
      status: "active",
      sourceThroughOffset: null,
      createdAt: ctx.now(),
    });
    await syncBoard(gameId);
    const capability = await ctx.issueAndStore(gameId, hostPlayerId, "host");
    const response: CreateGameResponse = {
      game: {
        id: gameId,
        mapVersion: MAP_VERSION,
      },
      player: {
        id: hostPlayerId,
        name,
        color: assignedSeatColor(result, requestedColor ?? "#e05a47"),
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
    const body = (await readJsonBody(request)) ?? {};
    const caller = await ctx.authenticateCapability(request);
    if (caller?.role === "agent") {
      return error(403, "FORBIDDEN", "Agent capabilities cannot join games.");
    }
    if (requestsAgentSeat(body.controller)) {
      return error(
        403,
        "AGENT_SEAT_REQUIRES_HOST",
        "The host must open agent seats with POST /agent-seats.",
      );
    }
    const requestedName = typeof body.name === "string" ? body.name : "";
    const requestedColor = typeof body.color === "string" ? body.color : undefined;
    const name = normalizePlayerName(requestedName) || "Player";
    const playerId = randomId("p");
    const commandId = typeof body.commandId === "string" ? body.commandId : randomId("cmd");
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "join-game",
      commandId,
      playerId,
      name,
      color: requestedColor,
      controller: controllerOf(body.controller),
    });
    if (result.status === "rejected") return rejection(result);
    await syncBoard(gameId);
    const capability = await ctx.issueAndStore(gameId, playerId, "player");
    const response: JoinGameResponse = {
      player: {
        id: playerId,
        name,
        color: assignedSeatColor(result, requestedColor ?? "#3b82f6"),
        role: "player",
      },
      capability,
      ack: ackBody(result),
    };
    return json(response, 201);
  }

  /** The seat roster as canonical history currently has it. */
  async function seatsOf(gameId: string) {
    const { events } = await readCanonical(ctx.protocol, eventStreamId(gameId));
    return foldAggregate(events).players;
  }

  /**
   * Rename a seat.
   *
   * Two callers are legitimate and no others: a player renaming their own seat,
   * and the host renaming an agent seat — the host is the only party that can open
   * one, so it is the only party with a name to give it. A host explicitly cannot
   * rename another *person's* seat, for the same reason it cannot delegate one.
   */
  async function renamePlayer(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const playerId = params.playerId!;
    if (!ctx.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    if (cap.role === "agent") {
      return error(403, "FORBIDDEN", "Agent capabilities cannot rename seats.");
    }
    const body = (await readJsonBody(request)) ?? {};
    if (typeof body.name !== "string") {
      return error(400, "BAD_REQUEST", "A rename must carry a name.");
    }

    if (cap.playerId !== playerId) {
      const target = (await seatsOf(gameId)).find((seat) => seat.id === playerId);
      if (!target) return error(404, "NOT_FOUND", "That seat is not part of this game.");
      if (cap.role !== "host" || target.controller !== "external-agent") {
        return error(403, "FORBIDDEN", "Only the seat itself, or its host for an agent seat.");
      }
    }

    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "rename-player",
      commandId: typeof body.commandId === "string" ? body.commandId : randomId("cmd"),
      playerId,
      name: body.name,
    });
    if (result.status === "rejected") return rejection(result);
    await syncBoard(gameId);
    // The recorded name, not the requested one: `decide` trims and bounds it.
    const named = result.events.find(
      (event): event is Extract<GameEvent, { type: "PlayerRenamed" }> =>
        event.type === "PlayerRenamed",
    );
    const response: RenamePlayerResponse = {
      player: { id: playerId, name: named?.name ?? normalizePlayerName(body.name) },
      ack: ackBody(result),
    };
    return json(response);
  }

  /**
   * Give up this capability's own seat.
   *
   * Scoped to `me` rather than an arbitrary seat id, so there is no shape of this
   * request that removes somebody else. A creator that leaves keeps its host
   * capability — hosting is not a seat — and simply watches the lobby it opened.
   */
  async function leaveGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    if (!ctx.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    if (cap.role === "agent") {
      return error(403, "FORBIDDEN", "An agent seat is played to the end, not given up.");
    }
    const body = (await readJsonBody(request)) ?? {};
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "leave-game",
      commandId: typeof body.commandId === "string" ? body.commandId : randomId("cmd"),
      playerId: cap.playerId,
    });
    if (result.status === "rejected") return rejection(result);
    await syncBoard(gameId);
    const response: LeaveGameResponse = { playerId: cap.playerId, ack: ackBody(result) };
    return json(response, 200, { "cache-control": "no-store" });
  }

  async function startGame(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId, "host");
    if (cap instanceof Response) return cap;
    const body = (await readJsonBody(request)) ?? {};
    const commandId = typeof body.commandId === "string" ? body.commandId : randomId("cmd");
    // The map is generated inside `decide` — after the command log has
    // deduped `commandId` and before the canonical append — so a start that
    // loses its CAS refolds and is rejected as already started, never regenerated.
    const result = await submitCommand(ctx.commandService, eventStreamId(gameId), {
      type: "start-game",
      commandId,
    });
    if (result.status === "rejected") return rejection(result);
    await afterCommand(gameId);
    return json(ackBody(result));
  }

  async function createAgentSeat(
    request: Request,
    params: Record<string, string>,
  ): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId, "host");
    if (cap instanceof Response) return cap;
    if (!ctx.stores.games.get(gameId)) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const body = (await readJsonBody(request)) ?? {};
    const commandId = typeof body.commandId === "string" ? body.commandId : randomId("cmd");
    let playerId = typeof body.playerId === "string" ? body.playerId : undefined;
    let name = normalizePlayerName(typeof body.name === "string" ? body.name : "") || "Agent";
    let color = typeof body.color === "string" ? body.color : "";

    if (playerId) {
      // Delegation converts an *existing* seat into an agent seat, and the only
      // seat a host is entitled to hand over is its own (plan A3). Allowing any
      // player id would let the host mint a playing capability for someone
      // else's seat and take over their game.
      if (playerId !== cap.playerId) {
        return error(
          403,
          "FORBIDDEN",
          "A host may delegate only its own seat. Omit playerId to open a new agent seat.",
        );
      }
      const { events } = await readCanonical(ctx.protocol, eventStreamId(gameId));
      const state = foldAggregate(events);
      const player = state.players.find((candidate) => candidate.id === playerId);
      if (!player) return error(404, "NOT_FOUND", "That seat is not part of this game.");
      const delegated = await submitCommand(ctx.commandService, eventStreamId(gameId), {
        type: "delegate-agent-seat",
        commandId,
        playerId,
      });
      if (delegated.status === "rejected") return rejection(delegated);
      name = player.name;
      color = player.color;
    } else {
      playerId = randomId("p");
      const joined = await submitCommand(ctx.commandService, eventStreamId(gameId), {
        type: "join-game",
        commandId,
        playerId,
        name,
        color: typeof body.color === "string" ? body.color : undefined,
        controller: "external-agent",
      });
      if (joined.status === "rejected") return rejection(joined);
      color = assignedSeatColor(joined, color);
    }

    await syncBoard(gameId);
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

    await syncBoard(gameId);
    const { events } = await readCanonical(ctx.protocol, eventStreamId(gameId));
    const state = foldAggregate(events);
    const response: GameResponse = {
      gameId,
      status: state.status,
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
      ...(state.pendingInteraction ? { pendingInteraction: state.pendingInteraction } : {}),
    };
    return json(response);
  }

  async function getBoard(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const game = ctx.stores.games.get(gameId);
    if (!game) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const board = await syncBoard(gameId);
    const response: BoardResponse = {
      gameId,
      sourceStreamId: board.sourceStreamId,
      sourceThroughOffset: board.sourceThroughOffset,
      generation: board.generation,
      boardStreamId: boardStreamId(gameId, board.generation),
      reducerVersion: BOARD_REDUCER_VERSION,
      game: board.state.game,
      players: board.state.players,
      hexes: board.state.hexes,
      territories: board.state.territories,
      continents: board.state.continents,
      turn: board.state.turn,
      combat: board.state.combat,
      moves: board.state.moves,
      view: projectionBoardView(board.state),
    };
    // `no-store` is a contract, not a nicety. The UI reads this endpoint once per
    // screen-open to learn the offset canonical history stood at, and compares
    // every throw against it (`openedThroughWatermark`). A cached answer would
    // reintroduce the stale-trace defect this watermark exists to close, because
    // core's stream catch-up reads *are* cacheable (`public, max-age=60`) and a
    // reload can hydrate a minute-old projection. Nothing else may weaken this.
    return json(response, 200, { "cache-control": "no-store" });
  }

  async function getMap(_request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const game = ctx.stores.games.get(gameId);
    if (!game) return error(404, "GAME_NOT_FOUND", "Unknown game.");
    const { events } = await readCanonical(ctx.protocol, eventStreamId(gameId));
    const state = foldAggregate(events);
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
    // Seat-scoped and bearer-authenticated: never cached and never referred out.
    const seatScoped = { "cache-control": "no-store", "referrer-policy": "no-referrer" };

    // Fold exactly the canonical prefix that projection has incorporated: the
    // decision is never ahead of the board snapshot it names.
    const board = await syncBoard(gameId);
    const { events } = await readCanonicalThrough(
      ctx.protocol,
      eventStreamId(gameId),
      board.sourceThroughOffset,
    );
    const state = foldAggregate(events);
    if (!state.players.some((player) => player.id === cap.playerId)) {
      return error(404, "NOT_FOUND", "Player is not part of this game.");
    }
    return json(
      buildDecisionContext(state, cap.playerId, {
        sourceStreamId: board.sourceStreamId,
        sourceThroughOffset: board.sourceThroughOffset,
        generation: board.generation,
        boardStreamId: boardStreamId(gameId, board.generation),
      }),
      200,
      seatScoped,
    );
  }

  async function postCommand(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const raw = await readJsonBody(request);

    const parsed = buildPlayCommand(raw, cap.playerId);
    if (!parsed.ok)
      return error(400, "INVALID_ACTION", `Command validation failed. ${STATE_GUIDANCE}`, {
        details: parsed.details,
      });
    const result = await submitCommand(
      ctx.commandService,
      eventStreamId(gameId),
      parsed.value.command,
    );
    if (result.status === "rejected") return rejection(result, true);
    await afterCommand(gameId);
    return json(ackBody(result, parsed.value.body.turnId), 200, {
      "cache-control": "no-store",
      "referrer-policy": "no-referrer",
    });
  }

  /**
   * Follow a seat's action-required stream.
   *
   * The published contract is Server-Sent Events: the connection carries the
   * backlog immediately, then holds open until an action lands, and closes on
   * its own after `ACTIONS_STREAM_TIMEOUT_MS` so a client reconnects from the
   * `nextOffset` it last saw. The former `wait` long poll is gone, and is
   * refused by name rather than ignored — a client still sending it is asking
   * for a semantic that no longer exists.
   *
   * `Accept: application/json` still answers one immediate, non-blocking page.
   * That representation exists for bootstrap, recovery and the repository's own
   * scripted consumers; it is explicitly negotiated, never the default, and it
   * never blocks, so nothing can mistake it for the old long poll.
   */
  async function getActions(request: Request, params: Record<string, string>): Promise<Response> {
    const gameId = params.gameId!;
    const cap = await ctx.requireCapability(request, gameId);
    if (cap instanceof Response) return cap;
    const url = new URL(request.url);
    const unknownQueryParameters = [...url.searchParams.keys()].filter((key) => key !== "offset");
    if (unknownQueryParameters.includes("wait")) {
      return error(
        400,
        "BAD_REQUEST",
        "The actions resource is a Server-Sent Events stream and no longer long-polls; drop `wait` and reconnect with ?offset=<last nextOffset>.",
      );
    }
    if (unknownQueryParameters.length > 0) {
      return error(400, "BAD_REQUEST", `Unknown query parameter: ${unknownQueryParameters[0]}.`);
    }
    await catchUpActions(ctx.protocol, gameId);
    const offset = url.searchParams.get("offset") ?? undefined;
    // Seat-scoped and bearer-authenticated: never cached and never referred out.
    const seatScoped = { "cache-control": "no-store", "referrer-policy": "no-referrer" };
    const read = (cursor: string | undefined, waitMs: number, signal: AbortSignal) =>
      readActions(ctx.protocol, gameId, cap.playerId, { cursor, waitMs, signal });

    if (prefersJsonOverEventStream(request)) {
      return json(await read(offset, 0, request.signal), 200, seatScoped);
    }
    return actionsStreamResponse({
      offset,
      read,
      signal: request.signal,
      timeoutMs: ctx.actionsStreamTimeoutMs,
    });
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
    { method: "DELETE", pattern: "/v1/games/:gameId/players/me", handler: leaveGame },
    { method: "PATCH", pattern: "/v1/games/:gameId/players/:playerId", handler: renamePlayer },
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

/**
 * Parse a play command. `resolve-defense-timeout` is deliberately absent: it
 * is authorized by the game service, not by a player capability, so there is no
 * transport path by which a player could submit one.
 */
type ValidationDetail = { path: string; expected: string; received: unknown };
type ParsedPlayCommand =
  | { ok: true; value: { body: PlayCommand; command: Command } }
  | { ok: false; details: ValidationDetail[] };

/** One malformed field, in the shape `INVALID_ACTION` publishes to the agent. */
function invalid(path: string, expected: string, received: unknown): ParsedPlayCommand {
  return { ok: false, details: [{ path, expected, received }] };
}

function buildPlayCommand(value: unknown, playerId: string): ParsedPlayCommand {
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

  let parsed: GameAction;
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
        const invalidPlacement = action.placements[invalidArmies];
        return invalid(
          `action.placements[${invalidArmies}].armies`,
          "integer >= 1",
          isRecord(invalidPlacement) ? invalidPlacement.armies : invalidPlacement,
        );
      }
      parsed = {
        type: "reinforce",
        placements: placements.filter(
          (placement): placement is { territoryId: string; armies: number } => placement !== null,
        ),
      };
      break;
    }
    case "declare-attack":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        typeof action.attackerDice !== "number" ||
        !Number.isInteger(action.attackerDice) ||
        action.attackerDice < 1
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
        attackerDice: action.attackerDice,
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
      if (
        typeof action.armies !== "number" ||
        !Number.isInteger(action.armies) ||
        action.armies < 1
      )
        return invalid("action.armies", "integer >= 1", action.armies);
      parsed = {
        type: "occupy-territory",
        attackId: action.attackId,
        armies: action.armies,
      };
      break;
    case "fortify":
      if (
        typeof action.from !== "string" ||
        typeof action.to !== "string" ||
        typeof action.armies !== "number" ||
        !Number.isInteger(action.armies) ||
        action.armies < 1
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
        armies: action.armies,
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

  const body: PlayCommand = { commandId, turnId, action: parsed };
  // Nothing about *who* resolved a combat is taken from the transport: the kernel
  // derives human/bot/agent attribution from the defending seat's canonical
  // controller, so a client cannot mislabel its own roll.
  const command: Command = { commandId, turnId, playerId, ...parsed };
  return { ok: true, value: { body, command } };
}
