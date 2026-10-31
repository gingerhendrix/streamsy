## @streamsy/outbox@0.8.0

### The Cloudflare object host logs requests

`StreamsyObject.make` and `objectHandlers` built their web handler with `disableLogger: true`. A handler defect answered 500 and logged nothing, so the only sign of the failure was the status code.

- The object host now logs each request at INFO level when it ends, with method, URL and status.
- A failed request logs its full cause, stack included.
- Each request adds one line to Workers logs.

### New @streamsy/outbox package replaces action delivery in serve

**Breaking change:** `@streamsy/serve/action` and its sub-paths `/action/errors`, `/action/outbox`, `/action/runtime` and `/action/sql` are removed. Use `@streamsy/outbox`.

`@streamsy/outbox` is a transactional outbox for Effect. `Outbox.enqueue` writes entries in the caller's transaction, and `Outbox.drain` delivers them at least once in bounded passes. The package has no host dependencies.

- `@streamsy/outbox/memory` exports `layerMemory`. `@streamsy/outbox/sql` exports `layerSql` and `migrateOutbox` for SQLite, Postgres and MySQL. `layerSql` builds the store from the `SqlClient` in context, so `enqueue` joins the application's active transaction.
- A drain pass delivers at most `limit` entries (default 100) and returns `{ name, attempted, delivered, retried, deadLettered, more }`. `more` is true when the pass stopped at its limit.
- `OutboxBacking` is the adapter protocol behind `OutboxStore`. Its `settle(id, Settlement)` takes a `Settlement` with `Delivered`, `Retry` and `Dead` cases.
- `@streamsy/serve/cloudflare` and `@streamsy/serve/alchemy` export `OutboxWakeup.make(definition, deliver, options)`, which drains an outbox scope from a host wakeup.

SQL stores no longer create their table. Run `migrateOutbox(sql)` once at startup on every dialect, outside any transaction. Without it, store operations fail with `OutboxUnavailable`.

The new table is `streamsy_outbox`. This release does not migrate the old `streamsy_effect_outbox` table or change it. Entries still pending in that table are not delivered after the upgrade. Drain them with the previous release before you upgrade, or move them yourself.

### Create rejects invalid options and JSON with typed errors

`CreateError` gains `InvalidCreateRequest`, a minor change to the public error union. Code that exhaustively matches `CreateError` must handle the new member. The error carries `id` and `message`.

- A remote plain create HTTP 400 response now fails with `InvalidCreateRequest` and the response message instead of `InvalidForkRequest`. Responses with `stream-not-supported` still fail with `NotSupported`, and fork HTTP 400 responses still fail with `InvalidForkRequest`.
- Local and remote creates reject `ttlSeconds` combined with `expiresAt`, and reject an `expiresAt` value that `DateTime.make` cannot parse. Both fail with `InvalidCreateRequest` before storage access or an HTTP request. Local creates previously used the TTL when both were set and could create a stream that never expired for an invalid date. HTTP continues to return 400 with the same messages.
- Invalid JSON `initialData` on a plain create, including typed toolkit create, now fails with `InvalidCreateRequest` and `Invalid JSON` instead of dying with `SyntaxError`. Fork initial data fails with `InvalidForkRequest` and the same message. Append framing and its invalid JSON behavior are unchanged.
