import { Schema } from "effect";

export interface InitialTerritory {
  readonly territoryId: string;
  readonly ownerId: string;
  readonly armies: number;
}

export const InitialTerritorySchema = Schema.Struct({
  territoryId: Schema.String,
  ownerId: Schema.String,
  armies: Schema.Int,
});
