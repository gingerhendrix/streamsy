/**
 * Host-neutral stream declarations for Hex Domination.
 *
 * Bun (`../persistence/bun.ts`) and the Cloudflare game object
 * (`../cloudflare/game-object.ts`) bind these same declarations to their own
 * storage. This module imports no platform storage.
 */
import { MemoryStorage, Streamsy, type StorageFault } from "@streamsy/core";
import { CheckpointStore } from "@streamsy/projection";
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

/** Every stream one game owns. Each host binds this group to its storage. */
export const gameStreams = Streamsy.group(GameEvents, PlayerActions, BoardStream);

/** The services the game, projection, and Serve code need from any host. */
export type GameStreamServices = CheckpointStore | Streamsy.Host<typeof gameStreams>;

/** A host-owned runtime over one storage. Both SQLite hosts fail with `StorageFault`. */
export type GameStreams = ManagedRuntime.ManagedRuntime<GameStreamServices, StorageFault>;

/** Add the checkpoint store to a host storage layer that serves {@link gameStreams}. */
export const withCheckpoints = <A, E>(storage: Layer.Layer<A, E>) =>
  CheckpointStore.layer.pipe(Layer.provideMerge(storage));

export const memoryStreams = (): GameStreams =>
  ManagedRuntime.make(withCheckpoints(MemoryStorage.layer(gameStreams)));
