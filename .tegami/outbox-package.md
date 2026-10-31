---
packages:
  "group:streamsy": minor
---

## New @streamsy/outbox package replaces action delivery in serve

**Breaking change:** `@streamsy/serve/action` and its sub-paths `/action/errors`, `/action/outbox`, `/action/runtime` and `/action/sql` are removed. Use `@streamsy/outbox`.

`@streamsy/outbox` is a transactional outbox for Effect. `Outbox.enqueue` writes entries in the caller's transaction, and `Outbox.drain` delivers them at least once in bounded passes. The package has no host dependencies.

- `@streamsy/outbox/memory` exports `layerMemory`. `@streamsy/outbox/sql` exports `layerSql` and `migrateOutbox` for SQLite, Postgres and MySQL. `layerSql` builds the store from the `SqlClient` in context, so `enqueue` joins the application's active transaction.
- A drain pass delivers at most `limit` entries (default 100) and returns `{ name, attempted, delivered, retried, deadLettered, more }`. `more` is true when the pass stopped at its limit.
- `OutboxBacking` is the adapter protocol behind `OutboxStore`. Its `settle(id, Settlement)` takes a `Settlement` with `Delivered`, `Retry` and `Dead` cases.
- `@streamsy/serve/cloudflare` and `@streamsy/serve/alchemy` export `OutboxWakeup.make(definition, deliver, options)`, which drains an outbox scope from a host wakeup.

SQL stores no longer create their table. Run `migrateOutbox(sql)` once at startup on every dialect, outside any transaction. Without it, store operations fail with `OutboxUnavailable`.

The new table is `streamsy_outbox`. This release does not migrate the old `streamsy_effect_outbox` table or change it. Entries still pending in that table are not delivered after the upgrade. Drain them with the previous release before you upgrade, or move them yourself.
