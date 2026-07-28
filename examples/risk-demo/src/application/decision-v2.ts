/**
 * Player-relative `risk-demo-v2` decision context.
 *
 * Built purely from the authoritative aggregate fold, so an agent has everything
 * needed for one move — fresh turn, live board, pending interrupt, and structured
 * legal actions — without scraping a UI or trusting a possibly-lagging projection
 * for legality.
 *
 * Unlike v1 this resource is not active-player-only: an out-of-turn human defender gets
 * a `roll-defense` action here. External-agent defences are server-resolved, so dice remain a
 * human-facing interaction rather than work delegated to the coding agent.
 *
 * ## Watermark stance
 *
 * The service catches the board projection up *first*, then folds exactly the
 * canonical prefix that projection has incorporated, and reports that
 * projection's `sourceThroughOffset`. So the contract is precise: **this decision
 * is derived from canonical history through `board.sourceThroughOffset`, and the
 * board generation named beside it has materialized at least that far.** The
 * decision is therefore never ahead of the board snapshot it names (spec §6.1),
 * and a client can compare the two offsets against its own StreamDB position
 * without ever comparing across streams. A command appended after the watermark
 * is simply not reflected yet — harmless, because every command is revalidated
 * against a fresh canonical fold at submission.
 *
 * ## Payload
 *
 * The full `GeneratedMap` is deliberately *not* shipped on every fetch. Static
 * geometry — hexes, names, adjacency, continents, label anchors — is board
 * surface: it is materialized once into the projection and fetched once from
 * `GET /board` (or followed live on `boardStreamId`). What changes every move —
 * ownership, armies, eliminations — stays here. {@link DecisionMapRef} carries
 * enough for a client to know *which* map it should have and where to get it.
 */

import type {
  AggregateStateV2,
  GamePhaseV2,
  PendingInteraction,
  ReinforcementState,
} from "../domain/aggregate-v2.ts";
import { buildTurnIdV2 } from "../domain/aggregate-v2.ts";
import type { PlayerController } from "../domain/events-v2.ts";
import { decisionModeV2, legalActionsV2, type DecisionModeV2 } from "./legal-actions-v2.ts";
import type { LegalActionV2 } from "./legal-actions-v2.ts";

/** Which map this decision was folded against, and where the snapshot lives. */
export interface DecisionMapRef {
  mapVersion?: string;
  generatorVersion?: string;
  seed?: string;
  /** Durable State stream carrying the projected map/board rows. */
  boardStreamId: string;
  territoryCount: number;
  continentCount: number;
}

export interface DecisionBoardV2 {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  /** Active board-projection generation the watermark belongs to. */
  generation: string;
  map: DecisionMapRef;
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
  legalMoves: LegalActionV2[];
}

export interface BoardWatermarkV2 {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
  boardStreamId: string;
}

const byId = (a: { id: string }, b: { id: string }): number => a.id.localeCompare(b.id);

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
      generation: watermark.generation,
      map: {
        mapVersion: state.mapVersion,
        generatorVersion: state.generatorVersion,
        seed: state.mapSeed,
        boardStreamId: watermark.boardStreamId,
        territoryCount: state.map?.territories.length ?? 0,
        continentCount: state.map?.continents.length ?? 0,
      },
      territories: Object.values(state.territories)
        .map((t) => ({ id: t.id, ownerId: t.ownerId, armies: t.armies }))
        .toSorted(byId),
      players: state.players
        .map((p) => ({ id: p.id, controller: p.controller, eliminated: p.eliminated }))
        .toSorted(byId),
    },
    legalMoves: legalActionsV2(state, playerId),
  };
}
