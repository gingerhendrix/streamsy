/**
 * Pure authoritative aggregate fold.
 *
 * `foldAggregate` reduces an ordered list of canonical events into the decision
 * model the command service validates against. It is total and deterministic:
 * the same events always produce the same state, and it never consults an `Rng`
 * (dice outcomes are read from `AttackResolved` facts).
 *
 * Derived (event-free) transitions kept here so the event log stays minimal:
 *  - reinforce -> attack once the reinforcement pool is fully placed;
 *  - attack -> fortify after the single fortify move;
 *  - reinforcement pool is recomputed whenever a turn begins.
 */

import type { GameEvent } from "./events.ts";
import { MAP_VERSION, RULES, RULESET, reinforcementPool } from "./map.ts";

export type GamePhase = "reinforce" | "attack" | "fortify";
export type GameStatus = "lobby" | "playing" | "finished";

export interface PlayerState {
  id: string;
  name: string;
  color: string;
  eliminated: boolean;
}

export interface TerritoryState {
  id: string;
  ownerId?: string;
  armies: number;
}

export interface CommandRecord {
  /** Events appended for this command, in order. */
  events: GameEvent[];
  /** Positional index of the command's last event in the log. */
  lastOffset: number;
}

export interface AggregateState {
  gameId?: string;
  ruleset: typeof RULESET;
  mapVersion: typeof MAP_VERSION;
  status: GameStatus;
  players: PlayerState[];
  turnOrder: string[];
  activePlayerId?: string;
  phase?: GamePhase;
  round: number;
  territories: Record<string, TerritoryState>;
  /** Reinforcements the active player still has to place (0 outside reinforce). */
  reinforcementsRemaining: number;
  winnerId?: string;
  /** commandId -> the outcome it produced, for idempotent replay/dedup. */
  commandIndex: Record<string, CommandRecord>;
  /** Number of events folded so far; equals the next event's offset. */
  eventCount: number;
}

export function initialState(): AggregateState {
  return {
    ruleset: RULESET,
    mapVersion: MAP_VERSION,
    status: "lobby",
    players: [],
    turnOrder: [],
    round: 0,
    territories: {},
    reinforcementsRemaining: 0,
    commandIndex: {},
    eventCount: 0,
  };
}

/** Turn identity a play command must observe: `round-<n>:<playerId>`. */
export function buildTurnId(round: number, playerId: string): string {
  return `round-${round}:${playerId}`;
}

export function currentTurnId(state: AggregateState): string | undefined {
  if (state.status !== "playing" || !state.activePlayerId) return undefined;
  return buildTurnId(state.round, state.activePlayerId);
}

export function ownedBy(state: AggregateState, playerId: string): string[] {
  return Object.values(state.territories)
    .filter((t) => t.ownerId === playerId)
    .map((t) => t.id);
}

export function player(state: AggregateState, playerId: string): PlayerState | undefined {
  return state.players.find((p) => p.id === playerId);
}

/** Non-eliminated players in `turnOrder`, preserving order. */
function activePlayers(state: AggregateState): string[] {
  return state.turnOrder.filter((id) => !player(state, id)?.eliminated);
}

/** Grant the active player's reinforcement pool at the start of their turn. */
function beginTurn(state: AggregateState, playerId: string): void {
  state.activePlayerId = playerId;
  state.phase = "reinforce";
  state.reinforcementsRemaining = reinforcementPool(ownedBy(state, playerId).length);
}

function applyEvent(state: AggregateState, event: GameEvent): void {
  switch (event.type) {
    case "GameCreated": {
      state.gameId = event.gameId;
      state.players.push({
        id: event.hostPlayerId,
        name: event.hostName,
        color: event.hostColor,
        eliminated: false,
      });
      break;
    }
    case "PlayerJoined": {
      state.players.push({
        id: event.playerId,
        name: event.name,
        color: event.color,
        eliminated: false,
      });
      break;
    }
    case "GameStarted": {
      state.status = "playing";
      state.turnOrder = event.turnOrder.slice();
      state.round = event.round;
      state.territories = {};
      for (const t of event.initialTerritories) {
        state.territories[t.territoryId] = {
          id: t.territoryId,
          ownerId: t.ownerId,
          armies: t.armies,
        };
      }
      beginTurn(state, event.turnOrder[0]!);
      break;
    }
    case "ArmiesReinforced": {
      const territory = state.territories[event.territoryId];
      if (territory) territory.armies += event.armies;
      state.reinforcementsRemaining -= event.armies;
      if (state.reinforcementsRemaining <= 0) state.phase = "attack";
      break;
    }
    case "AttackResolved": {
      const from = state.territories[event.from];
      const to = state.territories[event.to];
      if (from) from.armies -= event.attackerLosses;
      if (to) to.armies -= event.defenderLosses;
      if (event.territoryCaptured && from && to) {
        const occupying = event.occupyingArmies ?? 0;
        from.armies -= occupying;
        to.ownerId = event.playerId;
        to.armies = occupying;
      }
      break;
    }
    case "ArmiesFortified": {
      const from = state.territories[event.from];
      const to = state.territories[event.to];
      if (from) from.armies -= event.armies;
      if (to) to.armies += event.armies;
      state.phase = "fortify";
      break;
    }
    case "PlayerEliminated": {
      const eliminated = player(state, event.playerId);
      if (eliminated) eliminated.eliminated = true;
      break;
    }
    case "TurnEnded": {
      state.round = event.round;
      beginTurn(state, event.nextPlayerId);
      break;
    }
    case "GameWon": {
      state.status = "finished";
      state.winnerId = event.playerId;
      state.activePlayerId = undefined;
      state.phase = undefined;
      state.reinforcementsRemaining = 0;
      break;
    }
  }
}

export function foldAggregate(events: readonly GameEvent[]): AggregateState {
  const state = initialState();
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index]!;
    applyEvent(state, event);
    state.eventCount = index + 1;
    const record = state.commandIndex[event.commandId];
    if (record) {
      record.events.push(event);
      record.lastOffset = index;
    } else {
      state.commandIndex[event.commandId] = { events: [event], lastOffset: index };
    }
  }
  return state;
}

/**
 * Determine the next active player after `TurnEnded` and whether the round wraps.
 * Exposed for the decision layer so `TurnEnded` records the same values the fold
 * will re-derive.
 */
export function nextTurn(
  state: AggregateState,
  finishingPlayerId: string,
): { nextPlayerId: string; round: number } {
  const order = activePlayers(state);
  const fromIndex = order.indexOf(finishingPlayerId);
  const nextIndex = (fromIndex + 1) % order.length;
  const nextPlayerId = order[nextIndex]!;
  const wrapped = nextIndex <= fromIndex;
  return { nextPlayerId, round: wrapped ? state.round + 1 : state.round };
}

export { RULES };
