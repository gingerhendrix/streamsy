import { Effect, Schema } from "effect";

export class CatalogWorkspaceMismatch extends Schema.TaggedError<CatalogWorkspaceMismatch>()(
  "CatalogWorkspaceMismatch",
  { message: Schema.String },
) {}

export const checkCatalogWorkspace = (
  addressWorkspace: string,
  valueWorkspace: string,
  collection: string,
  key: string,
) =>
  addressWorkspace === valueWorkspace
    ? Effect.void
    : Effect.fail(
        new CatalogWorkspaceMismatch({
          message: `Catalog ${collection}/${key} names workspace ${valueWorkspace}, but address workspace is ${addressWorkspace}`,
        }),
      );
