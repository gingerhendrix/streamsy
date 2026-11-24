---
packages:
  "@streamsy/projection": patch
---

## Forgetting a binding no longer overlaps another host's first unit

With `Commit.transactionWithCleanup`, a `Projection.forget` that found no checkpoint could run its cleanup after another host committed the binding's first unit. Both calls reported success. The checkpoint stayed, and rows that the cleanup deletes could be gone. Projection 0.13.0 on Postgres has this fault.

`Commit.transaction` and `Commit.transactionWithCleanup` now check for a checkpoint again after cleanup. If one exists, the forget rolls back and fails with a retryable `token-conflict`. Stop the other host's runners and forget again. The check covers routed and unrouted bindings and MySQL at `REPEATABLE READ`. The `REPEATABLE READ` check needs `@streamsy/storage` at the same version. [Forget](https://streamsy.dev/docs/projections/checkpoints#forget)

Stop and drain a binding's runners before you forget it, as before. To clear a binding that an earlier forget left with a checkpoint and no rows, stop its runners and forget it again, or raise `generation`.

One limit stays. If two hosts race to commit a binding's first unit on Postgres or MySQL, one checkpoint and one output result. The slower host can get a non-retryable `invalid-output`, or a `ViewDataFault` from a Views circuit with routed outputs. That stops the binding until you call `Registry.retry` or start the runner again. Run one host for each binding. [One writer](https://streamsy.dev/docs/projections/checkpoints#one-writer)
