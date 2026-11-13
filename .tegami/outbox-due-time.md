---
packages:
  "@streamsy/outbox": minor
---

## Schedule and maintain outbox work

Add `notBefore` for delayed first attempts, filtered and paginated `list` reads,
`count` for monitoring, bounded `Outbox.prune` for settled rows, and
`Outbox.requeue` to retry dead entries with a fresh attempt budget. Duplicate
enqueue keeps the first due time; ordered drains block behind future entries.
Pruned keys can be enqueued again. Requeue preserves the original identity.

Settlement now fails with `OutboxUnavailable` for missing or already-settled
entries on every store, stopping the drain without overwriting a recorded result.

**Breaking for third-party backings:** `OutboxBacking` now requires `count`,
`prune` and `requeue`. Implementations must also honor `OutboxDraft.notBeforeMs`,
the extended list filters and pending-only settlement. The built-in stores need no database schema migration.
