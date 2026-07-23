/**
 * Pure board projection reducer — the query-shaped read model.
 *
 * This is deliberately a *second, independent* reduction over the same canonical
 * events (not a re-export of the aggregate). Keeping the two reducers separate is
 * what makes the equivalence check meaningful: if either drifts, `boardsEqual`
 * catches it. In later batches this same shape is what the materializer writes to
 * a Durable State stream alongside `sourceThroughOffset`.
 */

import type { GameEvent, GameEventType } from "./events.ts";
import { reinforcementPool } from "./map.ts";
import type { AggregateState, GamePhase, GameStatus } from "./aggregate.ts";

export interface ProjectedGame {
  id?: string;
  hostPlayerId?: string;
  status: GameStatus;
  phase?: GamePhase;
  activePlayerId?: string;
  round: number;
  winnerId?: string;
}

export interface ProjectedPlayer {
  id: string;
  name: string;
  color: string;
  remainingArmies: number;
  eliminated: boolean;
}

export interface ProjectedTerritory {
  id: string;
  ownerId?: string;
  armies: number;
}

export interface ProjectedMove {
  id: string;
  commandId: string;
  kind: GameEventType;
  playerId?: string;
  sourceOffset: string;
  territoryId?: string;
  from?: string;
  to?: string;
  armies?: number;
  attackerRolls?: number[];
  defenderRolls?: number[];
  attackerLosses?: number;
  defenderLosses?: number;
  territoryCaptured?: boolean;
  nextPlayerId?: string;
}

/** A compact demo feed: bounded so projection checkpoints stay O(1) in game length. */
export const MOVE_FEED_LIMIT = 30;

export interface ProjectionState {
  game: ProjectedGame;
  players: ProjectedPlayer[];
  territories: ProjectedTerritory[];
  moves: ProjectedMove[];
  /** Source offset (as a string) through which this projection is valid, or null. */
  sourceThroughOffset: string | null;
}

export function initialProjection(): ProjectionState {
  return {
    game: { status: "lobby", round: 0 },
    players: [],
    territories: [],
    moves: [],
    sourceThroughOffset: null,
  };
}

function findPlayer(state: ProjectionState, id: string): ProjectedPlayer | undefined {
  return state.players.find((p) => p.id === id);
}

function findTerritory(state: ProjectionState, id: string): ProjectedTerritory | undefined {
  return state.territories.find((t) => t.id === id);
}

function ownedCount(state: ProjectionState, ownerId: string): number {
  return state.territories.filter((t) => t.ownerId === ownerId).length;
}

function beginTurn(state: ProjectionState, playerId: string): void {
  state.game.activePlayerId = playerId;
  state.game.phase = "reinforce";
  const active = findPlayer(state, playerId);
  const pool = reinforcementPool(ownedCount(state, playerId));
  for (const p of state.players) p.remainingArmies = 0;
  if (active) active.remainingArmies = pool;
}

function movePlayerId(event: GameEvent): string | undefined {
  switch (event.type) {
    case "GameCreated":
      return event.hostPlayerId;
    case "GameStarted":
      return undefined;
    case "GameWon":
    case "PlayerJoined":
    case "PlayerEliminated":
    case "ArmiesReinforced":
    case "AttackResolved":
    case "ArmiesFortified":
    case "TurnEnded":
      return "playerId" in event ? event.playerId : undefined;
  }
}

/**
 * Pure projection step: apply one event observed at `sourceOffset` and return a
 * new projection state (the previous state is not mutated).
 *
 * `sourceOffset` is the canonical source offset the event sits at. In the pure
 * kernel it is the event's positional index (see {@link projectEvents}); when the
 * projection is materialized off a Streamsy stream (Batch 2) it is the real
 * stream offset, so the embedded `sourceThroughOffset` is a genuine watermark.
 */
export function projectEvent(
  previous: ProjectionState,
  event: GameEvent,
  sourceOffset: string,
): ProjectionState {
  const state = structuredClone(previous);

  switch (event.type) {
    case "GameCreated": {
      state.game.id = event.gameId;
      state.game.hostPlayerId = event.hostPlayerId;
      state.players.push({
        id: event.hostPlayerId,
        name: event.hostName,
        color: event.hostColor,
        remainingArmies: 0,
        eliminated: false,
      });
      break;
    }
    case "PlayerJoined": {
      state.players.push({
        id: event.playerId,
        name: event.name,
        color: event.color,
        remainingArmies: 0,
        eliminated: false,
      });
      break;
    }
    case "GameStarted": {
      state.game.status = "playing";
      state.game.round = event.round;
      state.territories = event.initialTerritories.map((t) => ({
        id: t.territoryId,
        ownerId: t.ownerId,
        armies: t.armies,
      }));
      beginTurn(state, event.turnOrder[0]!);
      break;
    }
    case "ArmiesReinforced": {
      const territory = findTerritory(state, event.territoryId);
      if (territory) territory.armies += event.armies;
      const active = findPlayer(state, event.playerId);
      if (active) active.remainingArmies -= event.armies;
      if ((active?.remainingArmies ?? 0) <= 0) state.game.phase = "attack";
      break;
    }
    case "AttackResolved": {
      const from = findTerritory(state, event.from);
      const to = findTerritory(state, event.to);
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
      const from = findTerritory(state, event.from);
      const to = findTerritory(state, event.to);
      if (from) from.armies -= event.armies;
      if (to) to.armies += event.armies;
      state.game.phase = "fortify";
      break;
    }
    case "PlayerEliminated": {
      const eliminated = findPlayer(state, event.playerId);
      if (eliminated) eliminated.eliminated = true;
      break;
    }
    case "TurnEnded": {
      state.game.round = event.round;
      beginTurn(state, event.nextPlayerId);
      break;
    }
    case "GameWon": {
      state.game.status = "finished";
      state.game.winnerId = event.playerId;
      state.game.activePlayerId = undefined;
      state.game.phase = undefined;
      for (const p of state.players) p.remainingArmies = 0;
      break;
    }
  }

  const detail = (() => {
    switch (event.type) {
      case "ArmiesReinforced":
        return { territoryId: event.territoryId, armies: event.armies };
      case "AttackResolved":
        return {
          from: event.from,
          to: event.to,
          attackerRolls: event.attackerRolls,
          defenderRolls: event.defenderRolls,
          attackerLosses: event.attackerLosses,
          defenderLosses: event.defenderLosses,
          territoryCaptured: event.territoryCaptured,
        };
      case "ArmiesFortified":
        return { from: event.from, to: event.to, armies: event.armies };
      case "TurnEnded":
        return { nextPlayerId: event.nextPlayerId };
      default:
        return {};
    }
  })();
  state.moves.push({
    id: sourceOffset,
    commandId: event.commandId,
    kind: event.type,
    playerId: movePlayerId(event),
    sourceOffset,
    ...detail,
  });
  if (state.moves.length > MOVE_FEED_LIMIT) {
    state.moves.splice(0, state.moves.length - MOVE_FEED_LIMIT);
  }
  state.sourceThroughOffset = sourceOffset;
  return state;
}

export function projectEvents(events: readonly GameEvent[]): ProjectionState {
  let state = initialProjection();
  for (let index = 0; index < events.length; index += 1) {
    state = projectEvent(state, events[index]!, String(index));
  }
  return state;
}

/** Normalised, order-independent view of the logical board both reducers agree on. */
export interface BoardView {
  status: GameStatus;
  phase?: GamePhase;
  activePlayerId?: string;
  round: number;
  winnerId?: string;
  players: Array<{ id: string; remainingArmies: number; eliminated: boolean }>;
  territories: Array<{ id: string; ownerId?: string; armies: number }>;
}

const byId = (a: { id: string }, b: { id: string }): number => a.id.localeCompare(b.id);

export function aggregateBoardView(state: AggregateState): BoardView {
  return {
    status: state.status,
    phase: state.phase,
    activePlayerId: state.activePlayerId,
    round: state.round,
    winnerId: state.winnerId,
    players: state.players
      .map((p) => ({
        id: p.id,
        remainingArmies: p.id === state.activePlayerId ? state.reinforcementsRemaining : 0,
        eliminated: p.eliminated,
      }))
      .toSorted(byId),
    territories: Object.values(state.territories)
      .map((t) => ({ id: t.id, ownerId: t.ownerId, armies: t.armies }))
      .toSorted(byId),
  };
}

export function projectionBoardView(state: ProjectionState): BoardView {
  return {
    status: state.game.status,
    phase: state.game.phase,
    activePlayerId: state.game.activePlayerId,
    round: state.game.round,
    winnerId: state.game.winnerId,
    players: state.players
      .map((p) => ({ id: p.id, remainingArmies: p.remainingArmies, eliminated: p.eliminated }))
      .toSorted(byId),
    territories: state.territories
      .map((t) => ({ id: t.id, ownerId: t.ownerId, armies: t.armies }))
      .toSorted(byId),
  };
}

/** True when the aggregate fold and the board projection agree on the logical board. */
export function boardsEqual(a: BoardView, b: BoardView): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
