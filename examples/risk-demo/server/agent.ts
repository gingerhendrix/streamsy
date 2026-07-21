/**
 * Coding-agent harness: plays Risk using ONLY the published HTTP resources and
 * its per-player turn stream — never the kernel directly.
 *
 * Loop: follow the turn stream from a persisted cursor → on wake fetch fresh
 * `/decision` → choose from structured `legalActions` with a deterministic
 * strategy → POST a command with a stable `commandId` → repeat while the same
 * turn is active. Wakes are hints; correctness comes from fresh decision fetches
 * and canonical command validation.
 *
 * Restart-safe idempotency: a command's `commandId` is derived deterministically
 * from the observed board state (`playerId:turnId:<state fingerprint>`). After a
 * crash + resume from the saved cursor, re-deriving at the same board yields the
 * same id (idempotent retry); once a command commits the board changes, so the
 * next id differs. Only the turn-stream cursor needs to be persisted.
 */

import type { TurnNotification } from "./turn-notifier.ts";

export type HttpCall = (
  method: string,
  path: string,
  opts?: { token?: string; body?: unknown },
) => Promise<{ status: number; body: any }>;

/** Persisted agent state — just the last consumed turn-stream cursor. */
export interface AgentState {
  cursor?: string;
}

export interface CreateAgentOptions {
  call: HttpCall;
  gameId: string;
  playerId: string;
  token: string;
  /** Mutated in place; snapshot `{ cursor }` to simulate a restart. */
  state?: AgentState;
}

export interface Agent {
  readonly state: AgentState;
  /** Poll the turn stream once; advance the cursor; return the latest wake seen. */
  awaitTurn(waitMs?: number): Promise<TurnNotification | null>;
  /** Play the active turn to completion (until control passes / game ends). */
  playTurn(maxSteps?: number): Promise<void>;
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

interface DecisionBoard {
  territories: Array<{
    id: string;
    ownerId?: string;
    armies: number;
    adjacentTerritoryIds: string[];
  }>;
  players: Array<{ id: string; remainingArmies: number; eliminated: boolean }>;
}

interface Decision {
  turn: { id: string; activePlayerId?: string; phase: string };
  board: DecisionBoard;
  legalActions: any[];
}

function boardFingerprint(playerId: string, decision: Decision): string {
  const terr = decision.board.territories
    .map((t) => `${t.id}:${t.ownerId ?? "-"}:${t.armies}`)
    .join(",");
  const me = decision.board.players.find((p) => p.id === playerId)?.remainingArmies ?? 0;
  return fingerprint(`${decision.turn.phase}|${me}|${terr}`);
}

/** The deterministic strategy: reinforce a frontier, attack forward, then end. */
function chooseAction(playerId: string, decision: Decision): Record<string, unknown> | null {
  const reinforce = decision.legalActions.find((a) => a.type === "reinforce");
  if (reinforce) {
    const owned = decision.board.territories.filter((t) => t.ownerId === playerId);
    const frontier =
      owned.find((t) =>
        t.adjacentTerritoryIds.some(
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

export function createAgent(options: CreateAgentOptions): Agent {
  const { call, gameId, playerId, token } = options;
  const state: AgentState = options.state ?? {};

  async function awaitTurn(waitMs = 0): Promise<TurnNotification | null> {
    const query = new URLSearchParams();
    if (state.cursor) query.set("offset", state.cursor);
    if (waitMs > 0) query.set("wait", String(waitMs));
    const res = await call("GET", `/v1/games/${gameId}/players/me/turns?${query.toString()}`, {
      token,
    });
    if (res.status !== 200) return null;
    state.cursor = res.body.cursor;
    const notes: TurnNotification[] = res.body.notifications ?? [];
    return notes.length > 0 ? notes[notes.length - 1]! : null;
  }

  async function playTurn(maxSteps = 300): Promise<void> {
    for (let step = 0; step < maxSteps; step += 1) {
      const res = await call("GET", `/v1/games/${gameId}/decision`, { token });
      if (res.status !== 200) return;
      const decision = res.body as Decision;
      if (decision.turn.activePlayerId !== playerId || decision.legalActions.length === 0) return;

      const action = chooseAction(playerId, decision);
      if (!action) return;

      const commandId = `${playerId}:${decision.turn.id}:${boardFingerprint(playerId, decision)}`;
      const submitted = await call("POST", `/v1/games/${gameId}/commands`, {
        token,
        body: { commandId, turnId: decision.turn.id, action },
      });
      // Any rejection (stale turn, game finished, illegal) means control passed on.
      if (submitted.status !== 200) return;
      if (action.type === "end-turn") return;
    }
    throw new Error(`agent ${playerId} exceeded ${maxSteps} steps in one turn`);
  }

  return { state, awaitTurn, playTurn };
}
