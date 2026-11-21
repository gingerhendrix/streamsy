/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/global-timers, effecttsgo/new-promise, effecttsgo/global-date -- Black-box workerd tests own native HTTP and wall-clock timing. */
/** HTTP helpers that drive the local Worker from outside, like a browser or agent. */
import { expect } from "vitest";
import { createActionsReader, type ActionsReader } from "../../src/application/actions-stream.ts";

export interface Reply {
  status: number;
  body: any;
}

export async function api(
  base: string,
  method: string,
  path: string,
  options: { token?: string; body?: unknown; headers?: Record<string, string> } = {},
): Promise<Reply> {
  const headers = new Headers({
    "content-type": "application/json",
    accept: "application/json",
    ...options.headers,
  });
  if (options.token) headers.set("authorization", `Bearer ${options.token}`);
  const response = await fetch(`${base}${path}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const text = await response.text();
  let body: any = null;
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }
  return { status: response.status, body };
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export interface TwoPlayerGame {
  gameId: string;
  host: string;
  guest: string;
  tokens: Record<string, string>;
}

/** Create, join, and start a human-versus-human game through the edge. */
export async function startTwoPlayerGame(base: string, mapSeed: string): Promise<TwoPlayerGame> {
  const created = await api(base, "POST", "/v1/games", {
    body: { name: "Host", color: "red", mapSeed },
  });
  expect(created.status).toBe(201);
  const gameId: string = created.body.game.id;
  const host: string = created.body.player.id;
  const joined = await api(base, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "Guest", color: "blue" },
  });
  expect(joined.status).toBe(201);
  const guest: string = joined.body.player.id;
  const tokens = { [host]: created.body.capability, [guest]: joined.body.capability };
  const started = await api(base, "POST", `/v1/games/${gameId}/start`, {
    token: tokens[host],
    body: { commandId: `${mapSeed}:start` },
  });
  expect(started.status).toBe(200);
  return { gameId, host, guest, tokens };
}

export async function activePlayer(base: string, game: TwoPlayerGame): Promise<string> {
  const meta = await api(base, "GET", `/v1/games/${game.gameId}`);
  expect(meta.status).toBe(200);
  return meta.body.activePlayerId;
}

export async function decision(base: string, game: TwoPlayerGame, playerId: string) {
  const reply = await api(base, "GET", `/v1/games/${game.gameId}/decision`, {
    token: game.tokens[playerId],
  });
  expect(reply.status).toBe(200);
  return reply.body;
}

export async function board(base: string, game: TwoPlayerGame) {
  const reply = await api(base, "GET", `/v1/games/${game.gameId}/board`);
  expect(reply.status).toBe(200);
  return reply.body;
}

export function command(
  base: string,
  game: TwoPlayerGame,
  playerId: string,
  body: { commandId: string; turnId: string; action: object },
) {
  return api(base, "POST", `/v1/games/${game.gameId}/commands`, {
    token: game.tokens[playerId],
    body,
  });
}

/** Place the active player's pool on a border territory. Returns the ack. */
export async function reinforceBorder(base: string, game: TwoPlayerGame) {
  const attacker = await activePlayer(base, game);
  const current = await decision(base, game, attacker);
  const map = await board(base, game);
  const ownerOf = (id: string): string | undefined =>
    current.board.territories.find((t: any) => t.id === id)?.ownerId;
  const reinforce = current.legalMoves.find((move: any) => move.type === "reinforce");
  const border = map.territories.find(
    (t: any) =>
      ownerOf(t.id) === attacker &&
      t.adjacentTerritoryIds.some((adjacent: string) => ownerOf(adjacent) !== attacker),
  );
  const body = {
    commandId: `reinforce:${current.turn.id}`,
    turnId: current.turn.id,
    action: { type: "reinforce", placements: [{ territoryId: border.id, armies: reinforce.pool }] },
  };
  const placed = await command(base, game, attacker, body);
  expect(placed.status).toBe(200);
  return { attacker, body, ack: placed.body };
}

export interface PendingAttack {
  attackId: string;
  turnId: string;
  attacker: string;
  defender: string;
  deadlineAt: number;
}

/** The attack awaiting defence, read from the projected board. */
export async function pendingAttack(base: string, game: TwoPlayerGame): Promise<PendingAttack> {
  const { combat } = await board(base, game);
  expect(combat?.status).toBe("awaiting-defense");
  return {
    attackId: combat.attackId,
    turnId: combat.turnId,
    attacker: combat.attackerId,
    defender: combat.defenderId,
    deadlineAt: combat.defenseDeadlineAt,
  };
}

/** Reinforce and declare one attack, leaving a pending human defence. */
export async function declareAttack(base: string, game: TwoPlayerGame): Promise<PendingAttack> {
  const { attacker } = await reinforceBorder(base, game);
  const current = await decision(base, game, attacker);
  const choice = current.legalMoves.find((move: any) => move.type === "declare-attack").choices[0];
  const declared = await command(base, game, attacker, {
    commandId: `declare:${current.turn.id}`,
    turnId: current.turn.id,
    action: {
      type: "declare-attack",
      from: choice.from,
      to: choice.to,
      attackerDice: choice.maxAttackerDice,
    },
  });
  expect(declared.status).toBe(200);
  return pendingAttack(base, game);
}

/** Every projected resolution of one attack. Exactly-once means length 1. */
export async function resolutionsOf(base: string, game: TwoPlayerGame, attackId: string) {
  const map = await board(base, game);
  return map.moves.filter(
    (move: any) => move.kind === "AttackResolved" && move.attackId === attackId,
  );
}

export async function openActions(
  base: string,
  gameId: string,
  token: string | undefined,
  offset?: string,
): Promise<{ response: Response; reader: ActionsReader }> {
  const query = offset ? `?offset=${encodeURIComponent(offset)}` : "";
  const connection = new AbortController();
  const headers = new Headers({ accept: "text/event-stream" });
  if (token) headers.set("authorization", `Bearer ${token}`);
  const response = await fetch(`${base}/v1/games/${gameId}/players/me/actions${query}`, {
    headers,
    signal: connection.signal,
  });
  return { response, reader: createActionsReader(response, connection) };
}
