import { MemoryStorage, Streamsy } from "@streamsy/core";
import { CheckpointStore } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { Layer, ManagedRuntime, Schema } from "effect";
import { GameEvent } from "../../src/domain/events.ts";
import {
  ProjectedCombatSchema,
  ProjectedContinentSchema,
  ProjectedGameSchema,
  ProjectedHexSchema,
  ProjectedMoveSchema,
  ProjectedPlayerSchema,
  ProjectedTerritorySchema,
  ProjectedTurnSchema,
  ProjectionMetaRowSchema,
} from "../../src/board/schemas.ts";
import { AgentMessageSchema } from "./action-message.ts";

/** A command is one canonical item. Its events never straddle projection pages. */
export const CommandBatch = Schema.Struct({
  payloadHash: Schema.optionalKey(Schema.String),
  events: Schema.NonEmptyArray(GameEvent),
});

export class GameEvents extends Streamsy.json<GameEvents>()(
  "HexDomination/GameEvents",
  "games/:gameId/events",
  { params: { gameId: Schema.String }, schema: CommandBatch },
) {}

export class PlayerActions extends Streamsy.json<PlayerActions>()(
  "HexDomination/PlayerActions",
  "games/:gameId/players/:playerId/actions",
  { params: { gameId: Schema.String, playerId: Schema.String }, schema: AgentMessageSchema },
) {}

export class BoardStream extends Streamsy.state<BoardStream>()(
  "HexDomination/Board",
  "games/:gameId/projections/board/:generation",
  {
    params: { gameId: Schema.String, generation: Schema.String },
    collections: {
      game: { schema: ProjectedGameSchema, key: "id" },
      player: { schema: ProjectedPlayerSchema, key: "id" },
      hex: { schema: ProjectedHexSchema, key: "id" },
      territory: { schema: ProjectedTerritorySchema, key: "id" },
      continent: { schema: ProjectedContinentSchema, key: "id" },
      turn: { schema: ProjectedTurnSchema, key: "id" },
      combat: { schema: ProjectedCombatSchema, key: "id" },
      move: { schema: ProjectedMoveSchema, key: "id" },
      projectionMeta: { schema: ProjectionMetaRowSchema, key: "id" },
    },
  },
) {}

const streams = Streamsy.group(GameEvents, PlayerActions, BoardStream);
const memoryLayer = () =>
  CheckpointStore.layer.pipe(Layer.provideMerge(MemoryStorage.layer(streams)));
const bunLayer = (filename: string) =>
  CheckpointStore.layer.pipe(
    Layer.provideMerge(BunStorage.layer(streams, { client: { filename } })),
  );

export type GameStreams = ManagedRuntime.ManagedRuntime<
  Layer.Success<ReturnType<typeof memoryLayer>>,
  Layer.Error<ReturnType<typeof bunLayer>>
>;

export const memoryStreams = (): GameStreams => ManagedRuntime.make(memoryLayer());
export const sqliteStreams = (filename: string): GameStreams =>
  ManagedRuntime.make(bunLayer(filename));
