/**
 * Agent-oriented decision context (`GET /v1/games/{id}/decision`).
 *
 * Built purely from the authoritative aggregate fold plus the board projection's
 * causal watermark, so an agent has everything needed for one move — fresh turn,
 * board, and structured legal actions — without scraping a UI or trusting a
 * possibly-lagging projection for legality.
 */

import type { AggregateState, GamePhase } from "../domain/aggregate.ts";
import { buildTurnId } from "../domain/aggregate.ts";
import { adjacentTo } from "../domain/map.ts";
import { legalActions, type LegalAction } from "./legal-actions.ts";

export interface DecisionContext {
  gameId: string;
  player: { id: string; name: string; color: string };
  turn: {
    id: string;
    round: number;
    activePlayerId?: string;
    phase: GamePhase | "setup";
  };
  board: {
    sourceStreamId: string;
    sourceThroughOffset: string | null;
    territories: Array<{
      id: string;
      ownerId?: string;
      armies: number;
      adjacentTerritoryIds: string[];
    }>;
    players: Array<{ id: string; remainingArmies: number; eliminated: boolean }>;
  };
  legalMoves: LegalAction[];
}

export interface BoardWatermark {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
}

export function buildDecisionContext(
  state: AggregateState,
  playerId: string,
  watermark: BoardWatermark,
): DecisionContext {
  const self = state.players.find((p) => p.id === playerId);
  if (!self) throw new Error(`unknown player ${playerId}`);

  const activePlayerId = state.activePlayerId;
  const turnId =
    state.status === "playing" && activePlayerId ? buildTurnId(state.round, activePlayerId) : "";

  return {
    gameId: state.gameId ?? "",
    player: { id: self.id, name: self.name, color: self.color },
    turn: {
      id: turnId,
      round: state.round,
      activePlayerId,
      phase: state.phase ?? "setup",
    },
    board: {
      sourceStreamId: watermark.sourceStreamId,
      sourceThroughOffset: watermark.sourceThroughOffset,
      territories: Object.values(state.territories)
        .map((t) => ({
          id: t.id,
          ownerId: t.ownerId,
          armies: t.armies,
          adjacentTerritoryIds: [...adjacentTo(t.id)],
        }))
        .toSorted((a, b) => a.id.localeCompare(b.id)),
      players: state.players
        .map((p) => ({
          id: p.id,
          remainingArmies: p.id === activePlayerId ? state.reinforcementsRemaining : 0,
          eliminated: p.eliminated,
        }))
        .toSorted((a, b) => a.id.localeCompare(b.id)),
    },
    legalMoves: legalActions(state, playerId),
  };
}
