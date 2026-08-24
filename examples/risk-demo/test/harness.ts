/* oxlint-disable effecttsgo/async-function -- This test harness intentionally exposes Promise helpers to Vitest while delegating application work to the existing runtime-owned APIs. */
/**
 * HTTP-level fixtures for `Hex Domination` integration tests.
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
import type { Rng } from "../src/domain/rng.ts";
import { createSeededRng } from "../src/domain/rng.ts";
import type { HttpCall, OpenActionsStream } from "../server/demo/bot.ts";
import { Schema } from "effect";

export const BASE = "http://risk.test";
export const DEFENSE_MS = 15_000;

type CheckedJsonValue = Schema.Schema.Type<typeof Schema.Json> | undefined;
const CheckedRecordSchema = Schema.Record(Schema.String, Schema.Json);
type CheckedRecord = Schema.Schema.Type<typeof CheckedRecordSchema>;

export function checkedRecord(value: CheckedJsonValue, label: string): CheckedRecord {
  try {
    return Schema.decodeUnknownSync(CheckedRecordSchema)(value);
  } catch {
    throw new Error(`${label} must be an object`);
  }
}

export function checkedString(value: CheckedJsonValue, label: string): string {
  try {
    return Schema.decodeUnknownSync(Schema.String)(value);
  } catch {
    throw new Error(`${label} must be a string`);
  }
}

export function checkedNumber(value: CheckedJsonValue, label: string): number {
  try {
    return Schema.decodeUnknownSync(Schema.Number)(value);
  } catch {
    throw new Error(`${label} must be a number`);
  }
}

export function checkedArray(value: CheckedJsonValue, label: string): CheckedJsonValue[] {
  try {
    return Schema.decodeUnknownSync(Schema.mutable(Schema.Array(Schema.Json)))(value);
  } catch {
    throw new Error(`${label} must be an array`);
  }
}

export interface Harness {
  app: App;
  stores: Stores;
  protocol: StreamProtocolFactory;
  scheduler: ManualScheduler;
  clock: { now: number };
  /** Force the next dice to specific faces (1..6), then fall back to the seed. */
  rig: (faces: readonly number[]) => void;
}

export function riggableRng(seed: number): Rng & { rig: (faces: readonly number[]) => void } {
  let queue: number[] = [];
  const fallback = createSeededRng(seed);
  return {
    nextInt: (bound) => (queue.length > 0 ? (queue.shift()! - 1) % bound : fallback.nextInt(bound)),
    rig: (faces) => {
      queue = faces.slice();
    },
  };
}

export function riskHarness(seed = 11, options: { actionsStreamTimeoutMs?: number } = {}): Harness {
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
    actionsStreamTimeoutMs: options.actionsStreamTimeoutMs,
  });
  return { app, stores, protocol, scheduler, clock, rig: rng.rig };
}

/** Rebuild the app over the SAME storage and stores — a process restart. */
export function restart(previous: Harness, seed = 11): Harness {
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
  options: { token?: string; body?: unknown; accept?: string } = {},
): Promise<{ status: number; body: any }> {
  // The actions resource streams unless a caller asks for the immediate page, so
  // every JSON-reading helper says so explicitly.
  const headers = new Headers({
    "content-type": "application/json",
    accept: options.accept ?? "application/json",
  });
  if (options.token) headers.set("authorization", `Bearer ${options.token}`);
  const res = await app.fetch(
    new Request(`${BASE}${path}`, {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    }),
  );
  return { status: res.status, body: await res.json() };
}

/** The `HttpCall` shape the scripted bot consumes. */
export function httpFor(app: App): HttpCall {
  return (method, path, opts = {}) => call(app, method, path, opts);
}

/** Open an SSE actions stream against an in-process app. */
export function streamFor(app: App): OpenActionsStream {
  return (path, opts) =>
    app.fetch(
      new Request(`${BASE}${path}`, {
        headers: { accept: "text/event-stream", authorization: `Bearer ${opts.token}` },
        signal: opts.signal,
      }),
    );
}

export interface Game {
  gameId: string;
  players: string[];
  tokenByPlayer: Record<string, string>;
}

export async function createGame(
  app: App,
  options: {
    players?: number;
    controllers?: Array<"human" | "bot" | "agent">;
    mapSeed?: string;
  } = {},
): Promise<Game> {
  const count = options.players ?? 2;
  const created = await call(app, "POST", "/v1/games", {
    body: {
      name: "Alice",
      color: "red",
      controller:
        (options.controllers?.[0] ?? "human") === "agent" ? "human" : options.controllers?.[0],
      mapSeed: options.mapSeed ?? "integration-seed",
    },
  });
  expect(created.status).toBe(201);
  const gameId: string = created.body.game.id;
  const hostId: string = created.body.player.id;
  const players = [hostId];
  const tokenByPlayer: Game["tokenByPlayer"] = { [hostId]: created.body.capability };
  const hostCapability = checkedString(created.body.capability, "host capability");

  if (options.controllers?.[0] === "agent") {
    const delegated = await call(app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostCapability,
      body: { playerId: hostId },
    });
    expect(delegated.status).toBe(201);
    tokenByPlayer[hostId] = delegated.body.seat.token;
  }

  for (let i = 1; i < count; i += 1) {
    const controller = options.controllers?.[i] ?? "human";
    const joined =
      controller === "agent"
        ? await call(app, "POST", `/v1/games/${gameId}/agent-seats`, {
            token: hostCapability,
            body: { name: `Player ${i + 1}`, color: ["blue", "green", "yellow"][i - 1] },
          })
        : await call(app, "POST", `/v1/games/${gameId}/players`, {
            body: {
              name: `Player ${i + 1}`,
              color: ["blue", "green", "yellow"][i - 1],
              controller,
            },
          });
    expect(joined.status).toBe(201);
    const player = controller === "agent" ? joined.body.seat : joined.body.player;
    players.push(player.playerId ?? player.id);
    tokenByPlayer[player.playerId ?? player.id] =
      controller === "agent" ? player.token : joined.body.capability;
  }

  const started = await call(app, "POST", `/v1/games/${gameId}/start`, {
    token: hostCapability,
    body: {},
  });
  expect(started.status).toBe(200);
  return { gameId, players, tokenByPlayer };
}

export async function decisionFor(app: App, game: Game, playerId: string) {
  const res = await call(app, "GET", `/v1/games/${game.gameId}/decision`, {
    token: game.tokenByPlayer[playerId]!,
  });
  expect(res.status).toBe(200);
  return res.body;
}

export async function gameMeta(app: App, game: Game) {
  return (await call(app, "GET", `/v1/games/${game.gameId}`)).body;
}

/** The projected current board — including the static map rows `/decision` omits. */
export async function boardFor(app: App, game: Game) {
  const res = await call(app, "GET", `/v1/games/${game.gameId}/board`);
  expect(res.status).toBe(200);
  return res.body;
}

export async function post(
  app: App,
  game: Game,
  playerId: string,
  body: { commandId: string; turnId: string; action: {} },
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
  h: Harness,
  game: Game,
  attackerFaces: readonly number[] = [3, 3, 3],
): Promise<PendingAttack> {
  const attacker = checkedString((await gameMeta(h.app, game)).activePlayerId, "attacker id");
  let decision = await decisionFor(h.app, game, attacker);
  const board = await boardFor(h.app, game);

  const ownerOf = (id: string): string | undefined =>
    decision.board.territories.find((x: any) => x.id === id)?.ownerId;
  const reinforce = decision.legalMoves.find((a: any) => a.type === "reinforce");
  const border = board.territories.find(
    (t: any) =>
      ownerOf(t.id) === attacker &&
      t.adjacentTerritoryIds.some((adj: string) => ownerOf(adj) !== attacker),
  );
  const placed = await post(h.app, game, attacker, {
    commandId: `reinf:${decision.turn.id}`,
    turnId: decision.turn.id,
    action: {
      type: "reinforce",
      placements: [{ territoryId: border.id, armies: reinforce.pool }],
    },
  });
  expect(placed.status).toBe(200);

  decision = await decisionFor(h.app, game, attacker);
  const choice = decision.legalMoves
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

/**
 * Resolve the pending throw and, if the target holds, declare another one against
 * it — until the country falls. Returns the attack that captured it, which is the
 * one now awaiting occupation.
 */
export async function throwUntilCapture(
  h: Harness,
  game: Game,
  attack: PendingAttack,
): Promise<PendingAttack> {
  let current = attack;
  for (let round = 0; round < 24; round += 1) {
    h.rig([1, 1]);
    const rolled = await post(h.app, game, current.defender, {
      commandId: `def:${current.attackId}`,
      turnId: current.turnId,
      action: { type: "roll-defense", attackId: current.attackId },
    });
    expect(rolled.status).toBe(200);
    // The ack is a receipt, not the outcome: a capture is observed as the
    // occupation the canonical board now demands.
    const after = await gameMeta(h.app, game);
    if (after.pendingInteraction?.type === "occupation") return current;

    const decision = await decisionFor(h.app, game, current.attacker);
    const choice = decision.legalMoves
      .find((a: any) => a.type === "declare-attack")
      ?.choices.find((c: any) => c.from === current.from && c.to === current.to);
    if (!choice) throw new Error(`${current.to} can no longer be attacked from ${current.from}`);

    const attackId = `atk:${current.turnId}:${current.to}:${round}`;
    h.rig([6, 6, 6].slice(0, choice.maxAttackerDice));
    const declared = await post(h.app, game, current.attacker, {
      commandId: attackId,
      turnId: current.turnId,
      action: {
        type: "declare-attack",
        from: choice.from,
        to: choice.to,
        attackerDice: choice.maxAttackerDice,
      },
    });
    expect(declared.status).toBe(200);
    current = { ...current, attackId };
  }
  throw new Error(`${attack.to} never fell`);
}
