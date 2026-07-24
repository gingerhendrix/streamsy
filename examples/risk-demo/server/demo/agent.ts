/**
 * Coding-agent harness: plays Risk using ONLY the published HTTP resources and
 * its per-player action stream — never the kernel directly.
 *
 * Loop: follow the action stream from a persisted cursor → on wake fetch fresh
 * `/decision` → choose from structured `legalActions` with a deterministic
 * strategy → POST a command with a stable `commandId` → repeat while the agent
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
 * stable id `agent-defense:<attackId>` rather than a board fingerprint, because
 * the board has not changed and the point is that a duplicate wake, a retry, and
 * a race with the canonical timeout must all collapse to one roll. Duplicate and
 * stale outcomes are treated as success — if the agent is offline entirely, the
 * canonical timeout resolves the combat without it.
 */

import type { PlayerActionNotification } from "../game/turn-notifier.ts";

export type HttpCall = (
  method: string,
  path: string,
  opts?: { token?: string; body?: unknown },
) => Promise<{ status: number; body: any }>;

/** Persisted agent state — just the last consumed action-stream cursor. */
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
  /** Optional observer hook after each successful command; used to pace the live demo. */
  onCommandCommitted?: (action: Record<string, unknown>) => void | Promise<void>;
}

export interface Agent {
  readonly state: AgentState;
  /** Poll the action stream once; advance the cursor; return the latest wake seen. */
  awaitTurn(waitMs?: number): Promise<PlayerActionNotification | null>;
  /** Take one action if this agent has a legal one right now. */
  step(): Promise<Record<string, unknown> | null>;
  /** Play until this agent has nothing legal left (turn passed, or waiting). */
  playTurn(maxSteps?: number): Promise<void>;
  /** Resolve a pending defence if one is waiting on this agent (§9.1). */
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

interface MapView {
  territories: Array<{ id: string; continentId: string; adjacentTerritoryIds: string[] }>;
  continents: Array<{ id: string; territoryIds: string[]; reinforcementBonus: number }>;
}

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
    map?: MapView;
    territories: TerritoryView[];
    players: Array<{ id: string; remainingArmies?: number; eliminated: boolean }>;
  };
  legalActions: any[];
}

function isV2(decision: Decision): boolean {
  return decision.ruleset === "risk-demo-v2" || decision.board.map !== undefined;
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
// risk-demo-v2 strategy (design spec §9.2)
// ---------------------------------------------------------------------------

interface V2Context {
  playerId: string;
  decision: Decision;
  map: MapView;
  armiesOf: (id: string) => number;
  ownerOf: (id: string) => string | undefined;
  neighbours: (id: string) => string[];
}

function v2Context(playerId: string, decision: Decision): V2Context | null {
  const map = decision.board.map;
  if (!map) return null;
  const byId = new Map(decision.board.territories.map((t) => [t.id, t]));
  const adjacency = new Map(map.territories.map((t) => [t.id, t.adjacentTerritoryIds]));
  return {
    playerId,
    decision,
    map,
    armiesOf: (id) => byId.get(id)?.armies ?? 0,
    ownerOf: (id) => byId.get(id)?.ownerId,
    neighbours: (id) => adjacency.get(id) ?? [],
  };
}

/** Enemy countries bordering `id`. The agent's whole notion of "exposed". */
function enemyNeighbours(ctx: V2Context, id: string): string[] {
  return ctx.neighbours(id).filter((adj) => ctx.ownerOf(adj) !== ctx.playerId);
}

/**
 * How much the agent cares about a continent: full control is worth defending,
 * and being one country away is worth pushing for.
 */
function continentPressure(ctx: V2Context, territoryId: string): number {
  const territory = ctx.map.territories.find((t) => t.id === territoryId);
  if (!territory) return 0;
  const continent = ctx.map.continents.find((c) => c.id === territory.continentId);
  if (!continent) return 0;
  const missing = continent.territoryIds.filter((id) => ctx.ownerOf(id) !== ctx.playerId).length;
  if (missing === 0) return continent.reinforcementBonus;
  if (missing === 1) return Math.max(1, Math.floor(continent.reinforcementBonus / 2));
  return 0;
}

/** Reinforce a border country, favouring continents held or nearly held. */
function chooseReinforceV2(ctx: V2Context, action: any): Record<string, unknown> {
  const scored = (action.territoryIds as string[])
    .map((id) => ({
      id,
      exposure: enemyNeighbours(ctx, id).length,
      pressure: continentPressure(ctx, id),
      armies: ctx.armiesOf(id),
    }))
    .filter((t) => t.exposure > 0);
  const pool =
    scored.length > 0
      ? scored
      : [{ id: action.territoryIds[0], exposure: 0, pressure: 0, armies: 0 }];
  const best = pool.toSorted(
    (a, b) =>
      b.pressure - a.pressure ||
      b.exposure - a.exposure ||
      a.armies - b.armies ||
      (a.id < b.id ? -1 : 1),
  )[0]!;
  // One placement per turn keeps the command count low and the ledger readable.
  return { type: "reinforce", territoryId: best.id, armies: action.maxArmies };
}

/** Attack where the army difference is favourable; break ties on continent value. */
function chooseAttackV2(ctx: V2Context, action: any): Record<string, unknown> | null {
  const scored = (action.choices as Array<{ from: string; to: string; maxAttackerDice: number }>)
    .map((choice) => ({
      choice,
      advantage: ctx.armiesOf(choice.from) - 1 - ctx.armiesOf(choice.to),
      pressure: continentPressure(ctx, choice.to),
    }))
    .filter((c) => c.advantage >= 1)
    .toSorted(
      (a, b) =>
        b.pressure - a.pressure ||
        b.advantage - a.advantage ||
        (a.choice.to < b.choice.to ? -1 : 1),
    );
  const best = scored[0];
  if (!best) return null;
  return {
    type: "declare-attack",
    from: best.choice.from,
    to: best.choice.to,
    attackerDice: best.choice.maxAttackerDice,
  };
}

/**
 * Occupy with the minimum, unless the captured country still borders enemies —
 * then push a bounded share of the available garrison forward instead of leaving
 * a token holding to be retaken next turn.
 */
function chooseOccupyV2(ctx: V2Context, action: any): Record<string, unknown> {
  const exposed = enemyNeighbours(ctx, action.to).length;
  const armies =
    exposed > 0
      ? Math.min(
          action.maxArmies,
          Math.max(action.minArmies, Math.ceil((action.minArmies + action.maxArmies) / 2)),
        )
      : action.minArmies;
  return { type: "occupy-territory", attackId: action.attackId, armies };
}

/** Fortify from an interior country toward its weakest reachable border. */
function chooseFortifyV2(ctx: V2Context, action: any): Record<string, unknown> | null {
  type Choice = { from: string; reachable: Array<{ to: string; maxArmies: number }> };
  for (const choice of (action.choices as Choice[]).toSorted(
    (a, b) => ctx.armiesOf(b.from) - ctx.armiesOf(a.from) || (a.from < b.from ? -1 : 1),
  )) {
    if (enemyNeighbours(ctx, choice.from).length > 0) continue; // already a border
    const borders = choice.reachable
      .filter((r) => enemyNeighbours(ctx, r.to).length > 0)
      .toSorted((a, b) => ctx.armiesOf(a.to) - ctx.armiesOf(b.to) || (a.to < b.to ? -1 : 1));
    const target = borders[0];
    if (!target) continue;
    const armies = Math.min(target.maxArmies, ctx.armiesOf(choice.from) - 1);
    if (armies < 1) continue;
    return { type: "fortify", from: choice.from, to: target.to, armies };
  }
  return null;
}

function chooseActionV2(playerId: string, decision: Decision): Record<string, unknown> | null {
  // Defence first: it is the only out-of-turn action, and the deadline is ticking.
  const defense = decision.legalActions.find((a) => a.type === "roll-defense");
  if (defense) return { type: "roll-defense", attackId: defense.attackId };

  const ctx = v2Context(playerId, decision);
  if (!ctx) return null;

  const occupy = decision.legalActions.find((a) => a.type === "occupy-territory");
  if (occupy) return chooseOccupyV2(ctx, occupy);

  const reinforce = decision.legalActions.find((a) => a.type === "reinforce");
  if (reinforce) return chooseReinforceV2(ctx, reinforce);

  const attack = decision.legalActions.find((a) => a.type === "declare-attack");
  if (attack) {
    const chosen = chooseAttackV2(ctx, attack);
    if (chosen) return chosen;
  }

  const fortify = decision.legalActions.find((a) => a.type === "fortify");
  if (fortify) {
    const chosen = chooseFortifyV2(ctx, fortify);
    if (chosen) return chosen;
  }

  if (decision.legalActions.some((a) => a.type === "end-turn")) return { type: "end-turn" };
  return null;
}

export function createAgent(options: CreateAgentOptions): Agent {
  const { call, gameId, playerId, token } = options;
  const state: AgentState = options.state ?? {};

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

  /**
   * A defence roll is idempotent by construction: the id names the attack, not
   * the board. A duplicate wake, a retry, and a lost race with the canonical
   * timeout therefore all end in the same place — one recorded roll.
   */
  function commandIdFor(action: Record<string, unknown>, decision: Decision): string {
    if (action.type === "roll-defense") return `agent-defense:${String(action.attackId)}`;
    return `${playerId}:${decision.turn.id}:${boardFingerprint(playerId, decision)}`;
  }

  async function step(): Promise<Record<string, unknown> | null> {
    const decision = await fetchDecision();
    if (!decision || decision.legalActions.length === 0) return null;

    const action = isV2(decision)
      ? chooseActionV2(playerId, decision)
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
    throw new Error(`agent ${playerId} exceeded ${maxSteps} steps in one turn`);
  }

  return { state, awaitTurn, step, playTurn, defend };
}
