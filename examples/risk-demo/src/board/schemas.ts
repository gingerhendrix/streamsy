import { Schema } from "effect";
import { PlayerControllerSchema, DefenseResolutionSourceSchema } from "../domain/events.ts";
import { AxialSchema, ContinentPaletteSchema, TerrainSchema } from "../domain/map.ts";

const Int = Schema.Int;
const Text = Schema.String;
const OptionalText = Schema.optionalKey(Text);
const ArrayOf = <S extends Schema.Top>(schema: S) => Schema.mutable(Schema.Array(schema));
const Reinforcement = Schema.Struct({
  base: Int,
  continents: ArrayOf(Schema.Struct({ continentId: Text, bonus: Int })),
  total: Int,
  remaining: Int,
});

export const ProjectedGameSchema = Schema.Struct({
  id: Text,
  hostPlayerId: OptionalText,
  status: Schema.Literals(["lobby", "playing", "finished"]),
  mapVersion: OptionalText,
  generatorVersion: OptionalText,
  mapSeed: OptionalText,
  round: Int,
  activePlayerId: OptionalText,
  phase: Schema.optionalKey(Schema.Literals(["reinforce", "attack", "fortify"])),
  winnerId: OptionalText,
});
export const ProjectedPlayerSchema = Schema.Struct({
  id: Text,
  name: Text,
  color: Text,
  controller: PlayerControllerSchema,
  eliminated: Schema.Boolean,
  territoryCount: Int,
  armyCount: Int,
});
export const ProjectedHexSchema = Schema.Struct({
  id: Text,
  q: Int,
  r: Int,
  territoryId: Text,
  terrain: TerrainSchema,
});
export const ProjectedTerritorySchema = Schema.Struct({
  id: Text,
  name: Text,
  continentId: Text,
  ownerId: OptionalText,
  armies: Int,
  hexIds: ArrayOf(Text),
  adjacentTerritoryIds: ArrayOf(Text),
  labelAnchor: AxialSchema,
});
export const ProjectedContinentSchema = Schema.Struct({
  id: Text,
  name: Text,
  territoryIds: ArrayOf(Text),
  reinforcementBonus: Int,
  controllerId: OptionalText,
  palette: ContinentPaletteSchema,
});
const ProjectedDice = Schema.Struct({
  attackId: Text,
  from: Text,
  to: Text,
  attackerRolls: ArrayOf(Int),
  defenderRolls: ArrayOf(Int),
  attackerLosses: Int,
  defenderLosses: Int,
  territoryCaptured: Schema.Boolean,
  resolutionSource: DefenseResolutionSourceSchema,
});
export const ProjectedTurnSchema = Schema.Struct({
  id: Text,
  turnId: Text,
  round: Int,
  playerId: Text,
  phase: Schema.optionalKey(Schema.Literals(["reinforce", "attack", "fortify"])),
  reinforcement: Reinforcement,
  reinforcementsPlaced: Int,
  attacksDeclared: Int,
  throwsResolved: Int,
  captures: Int,
  eliminations: Int,
  latestDice: Schema.optionalKey(ProjectedDice),
});
export const ProjectedCombatSchema = Schema.Struct({
  id: Text,
  attackId: Text,
  turnId: Text,
  status: Schema.Literals(["awaiting-defense", "awaiting-occupation"]),
  attackerId: Text,
  defenderId: Text,
  from: Text,
  to: Text,
  attackerDice: Int,
  attackerRolls: ArrayOf(Int),
  defenderDice: Int,
  declaredAt: Schema.Finite,
  defenseDeadlineAt: Schema.Finite,
  defenderRolls: Schema.optionalKey(ArrayOf(Int)),
  attackerLosses: Schema.optionalKey(Int),
  defenderLosses: Schema.optionalKey(Int),
  territoryCaptured: Schema.optionalKey(Schema.Boolean),
  resolutionSource: Schema.optionalKey(DefenseResolutionSourceSchema),
  minArmies: Schema.optionalKey(Int),
  maxArmies: Schema.optionalKey(Int),
});
const GameEventType = Schema.Literals([
  "GameCreated",
  "PlayerJoined",
  "PlayerControllerChanged",
  "PlayerRenamed",
  "PlayerLeft",
  "GameStarted",
  "ArmiesReinforced",
  "AttackDeclared",
  "AttackResolved",
  "TerritoryOccupied",
  "ArmiesFortified",
  "PlayerEliminated",
  "TurnEnded",
  "GameWon",
]);
export const ProjectedMoveSchema = Schema.Struct({
  id: Text,
  commandId: Text,
  kind: GameEventType,
  playerId: OptionalText,
  name: OptionalText,
  sourceOffset: Text,
  turnId: OptionalText,
  attackId: OptionalText,
  territoryId: OptionalText,
  from: OptionalText,
  to: OptionalText,
  armies: Schema.optionalKey(Int),
  attackerRolls: Schema.optionalKey(ArrayOf(Int)),
  defenderRolls: Schema.optionalKey(ArrayOf(Int)),
  attackerLosses: Schema.optionalKey(Int),
  defenderLosses: Schema.optionalKey(Int),
  territoryCaptured: Schema.optionalKey(Schema.Boolean),
  resolutionSource: Schema.optionalKey(DefenseResolutionSourceSchema),
  nextPlayerId: OptionalText,
});
export const ProjectionStateSchema = Schema.Struct({
  game: ProjectedGameSchema,
  players: ArrayOf(ProjectedPlayerSchema),
  hexes: ArrayOf(ProjectedHexSchema),
  territories: ArrayOf(ProjectedTerritorySchema),
  continents: ArrayOf(ProjectedContinentSchema),
  turn: Schema.NullOr(ProjectedTurnSchema),
  combat: Schema.NullOr(ProjectedCombatSchema),
  moves: ArrayOf(ProjectedMoveSchema),
  sourceThroughOffset: Schema.NullOr(Text),
});
export const BoardProjectionMetaSchema = Schema.Struct({
  sourceStreamId: Text,
  sourceThroughOffset: Text,
  sourceSeq: Int,
  generation: Text,
  reducerVersion: Text,
  snapshot: ProjectionStateSchema,
});
export const ProjectionMetaRowSchema = Schema.Struct({
  id: Text,
  ...BoardProjectionMetaSchema.fields,
});

const StateHeaders = Schema.Struct({
  operation: Schema.Literals(["insert", "update", "upsert", "delete"]),
  offset: Text,
  txid: Text,
});
export const DurableBoardFactSchema = Schema.Struct({
  type: Text,
  key: Text,
  value: Schema.Json,
  old_value: Schema.optionalKey(Schema.Json),
  headers: StateHeaders,
});
export const BoardMetaFactSchema = Schema.Struct({
  type: Schema.Literal("projectionMeta"),
  key: Schema.Literal("board"),
  value: BoardProjectionMetaSchema,
  headers: Schema.optionalKey(Schema.Unknown),
});
