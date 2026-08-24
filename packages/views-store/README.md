# `@streamsy/views-store`

Durable maintained-view state shared by the memory and Bun SQLite backends.

`ViewStore.commit` is the transaction boundary. Row mutations, opaque operator values and indexes, reducer working state, one ordered change batch, and the after-exclusive source cursor either advance together or do not advance. A caller supplies the expected prior cursor; stale maintainers receive `ViewCursorConflict`.

History positions are store-owned `{ epoch, sequence }` values. Retention removes complete batches and persists a floor; reads below it fail with `ViewHistoryExpired`. Transport adapters may map that result to snapshot fallback, but must not expose the position as a Durable Streams offset or token.

Checkpoint generations contain the plan hash, source identity and cursor, reducer identity and version, and a counted set of entries. SQLite creates and activates a generation in one transaction. `recover` loads the latest compatible generation and asks its source dependency for the strictly-after suffix.

The SQLite tables use the `streamsy_view_` prefix and the independent `streamsy_view_schema_version` migration namespace. This package does not modify `@streamsy/storage-sqlite` protocol tables. `importLegacyIssueStore` can copy the accepted Slice 1 `view_rows`, `reducer_state`, and `view_progress` rows once while leaving the old tables intact.

The operator seam is deliberately opaque. A runtime supplies namespace IDs, canonical JSON keys, private JSON values, and exact/ordered index mutations. This package never branches on an IVM node kind.
