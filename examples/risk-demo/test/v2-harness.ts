/**
 * HTTP-level fixtures for `risk-demo-v2` integration tests.
 *
 * Three things are injected that production leaves to the environment, and each
 * exists so a protocol property can be asserted rather than waited for:
 *
 *  - a **manual scheduler**, so a defence timeout fires exactly when the test
 *    says so instead of 15 real seconds later;
 *  - a **mutable clock**, so the canonical deadline can be crossed without
 *    sleeping — and so a "restart" can happen at a chosen point in that window;
 *  - a **riggable Rng**, so a throw's outcome is chosen rather than seed-hunted.
 *    Map generation runs on its own seeded substreams, so every draw from this
 *    one during play is a die.
 */

import { expect } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";

import { buildApp, type App } from "../server/http/app.ts";
import { createInMemoryStores, type Stores } from "../server/persistence/stores.ts";
import { createManualScheduler, type ManualScheduler } from "../server/game/defense-timer.ts";
import { RULESET_V2 } from "../src/domain/map-v2.ts";
import type { PlayerController } from "../src/domain/events-v2.ts";
import type { Rng } from "../src/domain/rng.ts";
import { createSeededRng } from "../src/domain/rng.ts";
import type { HttpCall } from "../server/demo/agent.ts";

export const BASE = "http://risk.test";
export const DEFENSE_MS = 15_000;

export interface V2Harness {
  app: App;
  stores: Stores;
  protocol: StreamProtocolFactory;
  scheduler: ManualScheduler;
  clock: { now: number };
  /** Force the next dice to specific faces (1..6), then fall back to the seed. */
  rig(faces: readonly number[]): void;
}

export function riggableRng(seed: number): Rng & { rig(faces: readonly number[]): void } {
  let queue: number[] = [];
  const fallback = createSeededRng(seed);
  return {
    nextInt: (bound) => (queue.length > 0 ? (queue.shift()! - 1) % bound : fallback.nextInt(bound)),
    rig: (faces) => {
      queue = faces.slice();
    },
  };
}

export function v2Harness(seed = 11): V2Harness {
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  const stores = createInMemoryStores();
  const scheduler = createManualScheduler();
  const rng = riggableRng(seed);
  const clock = { now: 1_700_000_000_000 };
  const app = buildApp({
    protocol,
    stores,
    rng,
    now: () => clock.now,
    scheduler,
    defenseTimeoutMs: DEFENSE_MS,
  });
  return { app, stores, protocol, scheduler, clock, rig: rng.rig };
}

/** Rebuild the app over the SAME storage and stores — a process restart. */
export function restartV2(previous: V2Harness, seed = 11): V2Harness {
  const scheduler = createManualScheduler();
  const rng = riggableRng(seed);
  const app = buildApp({
    protocol: previous.protocol,
    stores: previous.stores,
    rng,
    now: () => previous.clock.now,
    scheduler,
    defenseTimeoutMs: DEFENSE_MS,
  });
  return { ...previous, app, scheduler, rig: rng.rig };
}

export async function call(
  app: App,
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<{ status: number; body: any }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
  const res = await app.fetch(
    new Request(`${BASE}${path}`, {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    }),
  );
  return { status: res.status, body: await res.json() };
}

/** The `HttpCall` shape the agent harness consumes. */
export function httpFor(app: App): HttpCall {
  return (method, path, opts = {}) => call(app, method, path, opts);
}

export interface V2Game {
  gameId: string;
  players: string[];
  tokenByPlayer: Record<string, string>;
}

export async function createV2Game(
  app: App,
  options: { players?: number; controllers?: PlayerController[]; mapSeed?: string } = {},
): Promise<V2Game> {
  const count = options.players ?? 2;
  const created = await call(app, "POST", "/v1/games", {
    body: {
      ruleset: RULESET_V2,
      name: "Alice",
      color: "red",
      controller: options.controllers?.[0] ?? "human",
      mapSeed: options.mapSeed ?? "integration-seed",
    },
  });
  expect(created.status).toBe(201);
  expect(created.body.game.ruleset).toBe(RULESET_V2);
  const gameId: string = created.body.game.id;
  const hostId: string = created.body.player.id;
  const players = [hostId];
  const tokenByPlayer: Record<string, string> = { [hostId]: created.body.capability };

  for (let i = 1; i < count; i += 1) {
    const joined = await call(app, "POST", `/v1/games/${gameId}/players`, {
      body: {
        name: `Player ${i + 1}`,
        color: ["blue", "green", "yellow"][i - 1],
        controller: options.controllers?.[i] ?? "human",
      },
    });
    expect(joined.status).toBe(201);
    players.push(joined.body.player.id);
    tokenByPlayer[joined.body.player.id] = joined.body.capability;
  }

  const started = await call(app, "POST", `/v1/games/${gameId}/start`, {
    token: tokenByPlayer[hostId],
    body: {},
  });
  expect(started.status).toBe(200);
  return { gameId, players, tokenByPlayer };
}

export async function decisionFor(app: App, game: V2Game, playerId: string) {
  const res = await call(app, "GET", `/v1/games/${game.gameId}/decision`, {
    token: game.tokenByPlayer[playerId]!,
  });
  expect(res.status).toBe(200);
  return res.body;
}

export async function gameMeta(app: App, game: V2Game) {
  return (await call(app, "GET", `/v1/games/${game.gameId}`)).body;
}

export async function post(
  app: App,
  game: V2Game,
  playerId: string,
  body: { commandId: string; turnId: string; action: Record<string, unknown> },
) {
  return call(app, "POST", `/v1/games/${game.gameId}/commands`, {
    token: game.tokenByPlayer[playerId]!,
    body,
  });
}

export interface PendingAttack {
  attackId: string;
  turnId: string;
  attacker: string;
  defender: string;
  from: string;
  to: string;
}

/**
 * Place the whole pool on a border country and declare one attack, leaving the
 * game in the pending-defence interrupt.
 */
export async function declareAttack(
  h: V2Harness,
  game: V2Game,
  attackerFaces: readonly number[] = [3, 3, 3],
): Promise<PendingAttack> {
  const attacker = (await gameMeta(h.app, game)).activePlayerId as string;
  let decision = await decisionFor(h.app, game, attacker);

  const ownerOf = (id: string): string | undefined =>
    decision.board.territories.find((x: any) => x.id === id)?.ownerId;
  const reinforce = decision.legalActions.find((a: any) => a.type === "reinforce");
  const border = decision.board.map.territories.find(
    (t: any) =>
      ownerOf(t.id) === attacker &&
      t.adjacentTerritoryIds.some((adj: string) => ownerOf(adj) !== attacker),
  );
  const placed = await post(h.app, game, attacker, {
    commandId: `reinf:${decision.turn.id}`,
    turnId: decision.turn.id,
    action: { type: "reinforce", territoryId: border.id, armies: reinforce.maxArmies },
  });
  expect(placed.status).toBe(200);

  decision = await decisionFor(h.app, game, attacker);
  const choice = decision.legalActions
    .find((a: any) => a.type === "declare-attack")
    .choices.find((c: any) => c.from === border.id);
  const attackId = `atk:${decision.turn.id}:${choice.to}`;

  h.rig(attackerFaces.slice(0, choice.maxAttackerDice));
  const declared = await post(h.app, game, attacker, {
    commandId: attackId,
    turnId: decision.turn.id,
    action: {
      type: "declare-attack",
      from: choice.from,
      to: choice.to,
      attackerDice: choice.maxAttackerDice,
    },
  });
  expect(declared.status).toBe(200);

  return {
    attackId,
    turnId: decision.turn.id,
    attacker,
    defender: decision.board.territories.find((t: any) => t.id === choice.to)!.ownerId,
    from: choice.from,
    to: choice.to,
  };
}
