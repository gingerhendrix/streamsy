/**
 * Scripted-bot harness: plays Risk using only the published HTTP resources and
 * its per-player action stream — never the kernel directly.
 *
 * Loop: follow the action stream from a persisted cursor → on wake fetch fresh
 * `/decision` → choose from structured `legalActions` with a deterministic
 * strategy → POST a command with a stable `commandId` → repeat while the bot
 * still has something to do. Wakes are hints; correctness comes from fresh
 * decision fetches and canonical command validation.
 *
 * Restart-safe idempotency: a command's `commandId` is derived deterministically
 * from the observed board state (`playerId:turnId:<state fingerprint>`). After a
 * crash + resume from the saved cursor, re-deriving at the same board yields the
 * same id (idempotent retry); once a command commits the board changes, so the
 * next id differs. Only the action-stream cursor needs to be persisted.
 *
 * The harness speaks both rulesets. `risk-demo-v2` adds one shape it must handle
 * that v1 does not have: an out-of-turn `roll-defense`. That command uses the
 * stable id `bot-defense:<attackId>` rather than a board fingerprint, because
 * the board has not changed and the point is that a duplicate wake, a retry, and
 * a race with the canonical timeout must all collapse to one roll. Duplicate and
 * stale outcomes are treated as success — if the bot is offline entirely, the
 * canonical timeout resolves the combat without it.
 */

import type { PlayerActionNotification } from "../game/turn-notifier.ts";
import {
  chooseAttack,
  chooseFortify,
  chooseOccupy,
  chooseReinforce,
  strategyContext,
  type StrategyMap,
} from "./strategy-v2.ts";

export type HttpCall = (
  method: string,
  path: string,
  opts?: { token?: string; body?: unknown },
) => Promise<{ status: number; body: any }>;

/** Persisted bot state — just the last consumed action-stream cursor. */
export interface BotState {
  cursor?: string;
}

export interface CreateBotOptions {
  call: HttpCall;
  gameId: string;
  playerId: string;
  token: string;
  /** Mutated in place; snapshot `{ cursor }` to simulate a restart. */
  state?: BotState;
  /** Optional observer hook after each successful command; used to pace the live demo. */
  onCommandCommitted?: (action: Record<string, unknown>) => void | Promise<void>;
}

export interface Bot {
  readonly state: BotState;
  /** Poll the action stream once; advance the cursor; return the latest wake seen. */
  awaitTurn(waitMs?: number): Promise<PlayerActionNotification | null>;
  /** Take one action if this bot has a legal one right now. */
  step(): Promise<Record<string, unknown> | null>;
  /** Play until this bot has nothing legal left (turn passed, or waiting). */
  playTurn(maxSteps?: number): Promise<void>;
  /** Resolve a pending defence if one is waiting on this bot (§9.1). */
  defend(): Promise<boolean>;
}

/** Collision-resistant 53-bit string hash → base36 (deterministic, sync). */
function fingerprint(input: string): string {
  let h1 = 0xdeadbeef ^ input.length;
  let h2 = 0x41c6ce57 ^ input.length;
  for (let i = 0; i < input.length; i += 1) {
    const c = input.charCodeAt(i);
    h1 = Math.imul(h1 ^ c, 2654435761);
    h2 = Math.imul(h2 ^ c, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36);
}

interface TerritoryView {
  id: string;
  ownerId?: string;
  armies: number;
  adjacentTerritoryIds?: string[];
}

/**
 * Static geometry, read once from `GET /board`.
 *
 * `/decision` deliberately carries only what changes — ownership, armies, legal
 * actions — so the bot fetches the map from the board surface a single time and
 * caches it. It is immutable after `GameStarted`, so there is nothing to refresh.
 */
type MapView = StrategyMap;

interface Decision {
  ruleset?: string;
  mode?: string;
  turn: {
    id: string;
    activePlayerId?: string;
    phase: string;
    reinforcement?: { remaining: number };
  };
  pendingInteraction?: { type: string; attackId: string };
  board: {
    map?: { mapVersion?: string; boardStreamId?: string };
    territories: TerritoryView[];
    players: Array<{ id: string; remainingArmies?: number; eliminated: boolean }>;
  };
  legalActions: any[];
}

function isV2(decision: Decision): boolean {
  return decision.ruleset === "risk-demo-v2";
}

function boardFingerprint(playerId: string, decision: Decision): string {
  const terr = decision.board.territories
    .map((t) => `${t.id}:${t.ownerId ?? "-"}:${t.armies}`)
    .join(",");
  const remaining =
    decision.turn.reinforcement?.remaining ??
    decision.board.players.find((p) => p.id === playerId)?.remainingArmies ??
    0;
  return fingerprint(`${decision.turn.phase}|${remaining}|${terr}`);
}

// ---------------------------------------------------------------------------
// risk-demo-v1 strategy
// ---------------------------------------------------------------------------

function chooseActionV1(playerId: string, decision: Decision): Record<string, unknown> | null {
  const reinforce = decision.legalActions.find((a) => a.type === "reinforce");
  if (reinforce) {
    const owned = decision.board.territories.filter((t) => t.ownerId === playerId);
    const frontier =
      owned.find((t) =>
        (t.adjacentTerritoryIds ?? []).some(
          (adj) => decision.board.territories.find((x) => x.id === adj)?.ownerId !== playerId,
        ),
      ) ?? owned[0];
    if (!frontier) return { type: "end-turn" };
    return { type: "reinforce", territoryId: frontier.id, armies: reinforce.maxArmies };
  }

  const attack = decision.legalActions.find((a) => a.type === "attack");
  if (attack && attack.choices.length > 0) {
    const c = attack.choices[0];
    return { type: "attack", from: c.from, to: c.to, attackerDice: c.maxAttackerDice };
  }

  if (decision.legalActions.some((a) => a.type === "end-turn")) return { type: "end-turn" };
  return null;
}

// ---------------------------------------------------------------------------
// risk-demo-v2 strategy (design spec §9.2, implemented in `strategy-v2.ts`)
// ---------------------------------------------------------------------------

async function chooseActionV2(
  playerId: string,
  decision: Decision,
  loadMap: () => Promise<MapView | null>,
): Promise<Record<string, unknown> | null> {
  // Defence first: it is the only out-of-turn action, the deadline is ticking,
  // and it needs no map at all.
  const defense = decision.legalActions.find((a) => a.type === "roll-defense");
  if (defense) return { type: "roll-defense", attackId: defense.attackId };

  const map = await loadMap();
  if (!map) return null;
  const ctx = strategyContext(playerId, decision.board.territories, map);

  const occupy = decision.legalActions.find((a) => a.type === "occupy-territory");
  if (occupy) return chooseOccupy(ctx, occupy);

  const reinforce = decision.legalActions.find((a) => a.type === "reinforce");
  if (reinforce) return chooseReinforce(ctx, reinforce);

  const attack = decision.legalActions.find((a) => a.type === "declare-attack");
  if (attack) {
    const chosen = chooseAttack(ctx, attack);
    if (chosen) return chosen;
  }

  // No favourable attack anywhere: move idle armies toward one rather than
  // passing the turn, which is what a turtling opponent relies on (D4).
  const fortify = decision.legalActions.find((a) => a.type === "fortify");
  if (fortify) {
    const chosen = chooseFortify(ctx, fortify);
    if (chosen) return chosen;
  }

  if (decision.legalActions.some((a) => a.type === "end-turn")) return { type: "end-turn" };
  return null;
}

export function createBot(options: CreateBotOptions): Bot {
  const { call, gameId, playerId, token } = options;
  const state: BotState = options.state ?? {};

  async function awaitTurn(waitMs = 0): Promise<PlayerActionNotification | null> {
    const query = new URLSearchParams();
    if (state.cursor) query.set("offset", state.cursor);
    if (waitMs > 0) query.set("wait", String(waitMs));
    const res = await call("GET", `/v1/games/${gameId}/players/me/turns?${query.toString()}`, {
      token,
    });
    if (res.status !== 200) return null;
    state.cursor = res.body.cursor;
    const notes: PlayerActionNotification[] = res.body.notifications ?? [];
    return notes.length > 0 ? notes[notes.length - 1]! : null;
  }

  async function fetchDecision(): Promise<Decision | null> {
    const res = await call("GET", `/v1/games/${gameId}/decision`, { token });
    return res.status === 200 ? (res.body as Decision) : null;
  }

  /** Fetch the static map from the board surface once and keep it. */
  let cachedMap: MapView | null = null;
  async function loadMap(): Promise<MapView | null> {
    if (cachedMap) return cachedMap;
    const res = await call("GET", `/v1/games/${gameId}/board`);
    if (res.status !== 200) return null;
    const territories = (res.body.territories ?? []) as Array<{
      id: string;
      continentId: string;
      adjacentTerritoryIds: string[];
    }>;
    // Before `GameStarted` there is no map yet; do not cache an empty one.
    if (territories.length === 0) return null;
    cachedMap = {
      territories: territories.map((t) => ({
        id: t.id,
        continentId: t.continentId,
        adjacentTerritoryIds: t.adjacentTerritoryIds,
      })),
      continents: (res.body.continents ?? []) as MapView["continents"],
    };
    return cachedMap;
  }

  /**
   * A defence roll is idempotent by construction: the id names the attack, not
   * the board. A duplicate wake, a retry, and a lost race with the canonical
   * timeout therefore all end in the same place — one recorded roll.
   */
  function commandIdFor(action: Record<string, unknown>, decision: Decision): string {
    if (action.type === "roll-defense") return `bot-defense:${String(action.attackId)}`;
    return `${playerId}:${decision.turn.id}:${boardFingerprint(playerId, decision)}`;
  }

  async function step(): Promise<Record<string, unknown> | null> {
    const decision = await fetchDecision();
    if (!decision || decision.legalActions.length === 0) return null;

    const action = isV2(decision)
      ? await chooseActionV2(playerId, decision, loadMap)
      : decision.turn.activePlayerId === playerId
        ? chooseActionV1(playerId, decision)
        : null;
    if (!action) return null;

    const submitted = await call("POST", `/v1/games/${gameId}/commands`, {
      token,
      body: { commandId: commandIdFor(action, decision), turnId: decision.turn.id, action },
    });
    if (submitted.status !== 200) {
      // Any rejection means the canonical board moved on — a resolved attack, a
      // passed turn, a closed deadline. All of these are ordinary outcomes for a
      // client acting on a wake, not errors to retry blindly.
      return null;
    }
    await options.onCommandCommitted?.(action);
    return action;
  }

  async function defend(): Promise<boolean> {
    const decision = await fetchDecision();
    if (!decision) return false;
    if (!decision.legalActions.some((a) => a.type === "roll-defense")) return false;
    return (await step()) !== null;
  }

  async function playTurn(maxSteps = 300): Promise<void> {
    for (let taken = 0; taken < maxSteps; taken += 1) {
      const action = await step();
      if (!action || action.type === "end-turn") return;
    }
    throw new Error(`bot ${playerId} exceeded ${maxSteps} steps in one turn`);
  }

  return { state, awaitTurn, step, playTurn, defend };
}
