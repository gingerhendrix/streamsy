/**
 * Player-relative `risk-demo-v2` decision context (design spec §6.1).
 *
 * Built purely from the authoritative aggregate fold, so an agent has everything
 * needed for one move — fresh turn, board, pending interrupt, and structured
 * legal actions — without scraping a UI or trusting a possibly-lagging projection
 * for legality.
 *
 * Unlike v1 this resource is not active-player-only: an out-of-turn defender gets
 * a `roll-defense` action here, which is what makes an agent's defence loop a
 * normal decision fetch rather than a special channel.
 *
 * Scope note: the full decision/OpenAPI polish and the v2 board projection land
 * with the next slice. Until then the watermark reported below is the canonical
 * head rather than a projection watermark — named honestly in
 * {@link DecisionBoardV2.sourceThroughOffset}'s producer, not disguised.
 */

import type {
  AggregateStateV2,
  GamePhaseV2,
  PendingInteraction,
  ReinforcementState,
} from "../domain/aggregate-v2.ts";
import { buildTurnIdV2 } from "../domain/aggregate-v2.ts";
import type { PlayerController } from "../domain/events-v2.ts";
import type { GeneratedMap } from "../domain/map-v2.ts";
import { decisionModeV2, legalActionsV2, type DecisionModeV2 } from "./legal-actions-v2.ts";
import type { LegalActionV2 } from "./legal-actions-v2.ts";

export interface DecisionBoardV2 {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  /** The canonical map snapshot; clients never import the generator. */
  map?: GeneratedMap;
  territories: Array<{ id: string; ownerId?: string; armies: number }>;
  players: Array<{ id: string; controller: PlayerController; eliminated: boolean }>;
}

export interface DecisionContextV2 {
  gameId: string;
  ruleset: string;
  player: { id: string; name: string; color: string; controller: PlayerController };
  mode: DecisionModeV2;
  turn: {
    id: string;
    round: number;
    activePlayerId?: string;
    phase: GamePhaseV2 | "setup";
    reinforcement: ReinforcementState;
  };
  pendingInteraction?: PendingInteraction;
  board: DecisionBoardV2;
  legalActions: LegalActionV2[];
}

export interface BoardWatermarkV2 {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
}

export function buildDecisionContextV2(
  state: AggregateStateV2,
  playerId: string,
  watermark: BoardWatermarkV2,
): DecisionContextV2 {
  const self = state.players.find((p) => p.id === playerId);
  if (!self) throw new Error(`unknown player ${playerId}`);

  const activePlayerId = state.activePlayerId;
  const turnId =
    state.status === "playing" && activePlayerId ? buildTurnIdV2(state.round, activePlayerId) : "";

  return {
    gameId: state.gameId ?? "",
    ruleset: state.ruleset ?? "",
    player: { id: self.id, name: self.name, color: self.color, controller: self.controller },
    mode: decisionModeV2(state, playerId),
    turn: {
      id: turnId,
      round: state.round,
      activePlayerId,
      phase: state.phase ?? "setup",
      reinforcement: state.reinforcement,
    },
    ...(state.pendingInteraction ? { pendingInteraction: state.pendingInteraction } : {}),
    board: {
      sourceStreamId: watermark.sourceStreamId,
      sourceThroughOffset: watermark.sourceThroughOffset,
      map: state.map,
      territories: Object.values(state.territories)
        .map((t) => ({ id: t.id, ownerId: t.ownerId, armies: t.armies }))
        .toSorted((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)),
      players: state.players
        .map((p) => ({ id: p.id, controller: p.controller, eliminated: p.eliminated }))
        .toSorted((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)),
    },
    legalActions: legalActionsV2(state, playerId),
  };
}
