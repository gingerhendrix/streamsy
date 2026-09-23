import { Effect, Schema } from "effect";

export class CatalogWorkspaceMismatch extends Schema.TaggedError<CatalogWorkspaceMismatch>()(
  "CatalogWorkspaceMismatch",
  { message: Schema.String },
) {}

export const checkCatalogWorkspace = (
  memberWorkspace: string,
  valueWorkspace: string,
  collection: string,
  key: string,
) =>
  memberWorkspace === valueWorkspace
    ? Effect.void
    : Effect.fail(
        new CatalogWorkspaceMismatch({
          message: `Catalog ${collection}/${key} names workspace ${valueWorkspace}, but member workspace is ${memberWorkspace}`,
        }),
      );
