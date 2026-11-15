---
packages:
  "@streamsy/serve": minor
---

## Run external deliveries as projections

**Breaking change:** `@streamsy/outbox` is no longer released with Streamsy and
gets no further versions. `OutboxWakeup` is removed from
`@streamsy/serve/cloudflare` and `@streamsy/serve/alchemy`.

Use a projection with `Commit.atLeastOnce` and `unit.key` as the idempotency key,
the declaration's `retry` policy for a retry budget and `onExhausted`, and
`Projection.wakeAt` for a deferred retry or a snooze. On Cloudflare, attach it with
`ProjectionWakeup.attach`. See [Effects](https://streamsy.dev/docs/projections/effects).
