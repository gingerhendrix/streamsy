/**
 * Player-relative `Hex Domination` decision context.
 *
 * Built purely from the authoritative aggregate fold, so an agent has everything
 * needed for one move — fresh turn, live board, pending interrupt, and structured
 * legal actions — without scraping a UI or trusting a possibly-lagging projection
 * for legality.
 *
 * This resource is player-relative: an out-of-turn human defender gets
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

import type { AggregateState } from "../domain/aggregate.ts";
import { buildTurnId } from "../domain/aggregate.ts";
import { PlayerControllerSchema, type PlayerController } from "../domain/events.ts";
import { decisionMode, DecisionMode, legalActions, LegalAction } from "./legal-actions.ts";
import { Schema } from "effect";

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

export interface DecisionBoard {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  /** Active board-projection generation the watermark belongs to. */
  generation: string;
  map: DecisionMapRef;
  territories: Array<{ id: string; ownerId?: string; armies: number }>;
  players: Array<{ id: string; controller: PlayerController; eliminated: boolean }>;
}

const MutableArray = <S extends Schema.Top>(schema: S) => Schema.mutable(Schema.Array(schema));
const ReinforcementStateSchema = Schema.Struct({
  base: Schema.Int,
  continents: MutableArray(Schema.Struct({ continentId: Schema.String, bonus: Schema.Int })),
  total: Schema.Int,
  remaining: Schema.Int,
});
export const PendingInteractionSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("defense"),
    attackId: Schema.String,
    turnId: Schema.String,
    attackerId: Schema.String,
    defenderId: Schema.String,
    from: Schema.String,
    to: Schema.String,
    attackerDice: Schema.Int,
    attackerRolls: MutableArray(Schema.Int),
    defenderDice: Schema.Int,
    declaredAt: Schema.Finite,
    defenseDeadlineAt: Schema.Finite,
  }),
  Schema.Struct({
    type: Schema.Literal("occupation"),
    attackId: Schema.String,
    turnId: Schema.String,
    playerId: Schema.String,
    from: Schema.String,
    to: Schema.String,
    minArmies: Schema.Int,
    maxArmies: Schema.Int,
  }),
]);
export const DecisionContext = Schema.Struct({
  gameId: Schema.String,
  player: Schema.Struct({
    id: Schema.String,
    name: Schema.String,
    color: Schema.String,
    controller: PlayerControllerSchema,
  }),
  mode: DecisionMode,
  turn: Schema.Struct({
    id: Schema.String,
    round: Schema.Int,
    activePlayerId: Schema.optionalKey(Schema.String),
    phase: Schema.Literals(["reinforce", "attack", "fortify", "setup"]),
    reinforcement: ReinforcementStateSchema,
  }),
  pendingInteraction: Schema.optionalKey(PendingInteractionSchema),
  board: Schema.Struct({
    sourceStreamId: Schema.String,
    sourceThroughOffset: Schema.NullOr(Schema.String),
    generation: Schema.String,
    map: Schema.Struct({
      mapVersion: Schema.optionalKey(Schema.String),
      generatorVersion: Schema.optionalKey(Schema.String),
      seed: Schema.optionalKey(Schema.String),
      boardStreamId: Schema.String,
      territoryCount: Schema.Int,
      continentCount: Schema.Int,
    }),
    territories: MutableArray(
      Schema.Struct({
        id: Schema.String,
        ownerId: Schema.optionalKey(Schema.String),
        armies: Schema.Int,
      }),
    ),
    players: MutableArray(
      Schema.Struct({
        id: Schema.String,
        controller: PlayerControllerSchema,
        eliminated: Schema.Boolean,
      }),
    ),
  }),
  legalMoves: MutableArray(LegalAction),
});
export type DecisionContext = typeof DecisionContext.Type;

export interface BoardWatermark {
  sourceStreamId: string;
  sourceThroughOffset: string | null;
  generation: string;
  boardStreamId: string;
}

const byId = (a: { id: string }, b: { id: string }): number => a.id.localeCompare(b.id);

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
    player: { id: self.id, name: self.name, color: self.color, controller: self.controller },
    mode: decisionMode(state, playerId),
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
    legalMoves: legalActions(state, playerId),
  };
}
