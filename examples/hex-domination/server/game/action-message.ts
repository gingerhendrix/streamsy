import { Schema } from "effect";
import { GameEvent } from "../../src/domain/events.ts";
import { LegalAction } from "../../src/application/legal-actions.ts";
import { PendingInteractionSchema } from "../../src/application/decision.ts";

const ReinforcementStateSchema = Schema.Struct({
  base: Schema.Int,
  continents: Schema.Array(Schema.Struct({ continentId: Schema.String, bonus: Schema.Int })),
  total: Schema.Int,
  remaining: Schema.Int,
});
const SinceSchema = Schema.Struct({
  fromEventOffset: Schema.NullOr(Schema.String),
  events: Schema.Array(GameEvent),
});
export const AgentMessageSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("ActionRequired"),
    messageId: Schema.String,
    seq: Schema.Int,
    gameId: Schema.String,
    playerId: Schema.String,
    reason: Schema.Literals([
      "turn-started",
      "phase-changed",
      "reinforcement-remaining",
      "attack-resolved",
      "occupation-required",
      "defense-required",
    ]),
    turn: Schema.Struct({
      id: Schema.String,
      round: Schema.Int,
      phase: Schema.Literals(["reinforce", "attack", "fortify"]),
      activePlayerId: Schema.String,
      reinforcement: ReinforcementStateSchema,
    }),
    mode: Schema.Literals(["active-turn", "defense"]),
    pendingInteraction: Schema.NullOr(PendingInteractionSchema),
    legalMoves: Schema.Array(LegalAction),
    board: Schema.Struct({
      territories: Schema.Array(
        Schema.Struct({
          id: Schema.String,
          ownerId: Schema.NullOr(Schema.String),
          armies: Schema.Int,
        }),
      ),
      players: Schema.Array(Schema.Struct({ id: Schema.String, eliminated: Schema.Boolean })),
    }),
    since: SinceSchema,
    eventOffset: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("GameOver"),
    messageId: Schema.String,
    seq: Schema.Int,
    gameId: Schema.String,
    playerId: Schema.String,
    winner: Schema.Struct({ id: Schema.String, name: Schema.String }),
    since: SinceSchema,
    eventOffset: Schema.String,
  }),
]);
export type AgentMessage = typeof AgentMessageSchema.Type;
export type ActionRequired = Extract<AgentMessage, { readonly type: "ActionRequired" }>;
export type GameOver = Extract<AgentMessage, { readonly type: "GameOver" }>;
