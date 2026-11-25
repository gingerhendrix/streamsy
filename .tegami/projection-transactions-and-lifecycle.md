---
packages:
  "@streamsy/projection": minor
---

## Unified transaction commits on SQL and memory hosts

`Commit.transaction` and `Commit.transactionWithCleanup` now use one unified transaction model across SQL and `MemoryStorage` hosts: compose `Transaction.layer` and `CheckpointStore.layer` over the same storage owner so each unit, its outputs, and its checkpoint (or lifecycle cleanup on `forget`/`retire`) commit together.

**Breaking change:**

- `Transaction` no longer exposes a `sql` field. Read `SqlClient` from Effect context where SQL is needed, and use `Layer.provideMerge` when composing the host so the storage owner's `SqlClient` remains in the runner's context (including for `StateTable` and `FoldByKey` runners).
- The `Commit.transactionWithCleanup` callback now takes only the checkpoint key.
- `StateTable` and `FoldByKey` fail with `unsupported-composition` if `SqlClient` has no open transaction. Wrap direct `table.sync()` calls in `Transaction.withTransaction`.
- The transaction now verifies checkpoint ownership on every unit, including units without routed outputs. Use the `load` function provided by `CheckpointStore.layer` directly—wrapping `load` drops owner metadata and fails with `unsupported-composition`.

See [`Commit.transaction`](https://streamsy.dev/docs/reference/projection/commit#committransaction).

## Retire forgotten routed streams and undecodable checkpoints

- `Projection.forget(bound)` now marks routed output records as forgotten rather than deleting them, so `Projection.retire` can still locate and clean up those streams. Pass the binding from `declaration.at(params)` with its routed outputs. Replay checks that each forgotten stream starts empty and open.
- `Projection.retire` can now write a tombstone over an undecodable checkpoint and close or remove its streams in the same call.
- **Breaking change:** `Retired.streams` replaces `Retired.outputs` and `Retired.routed`, and `Projection.RetiredOutput` is renamed to `Projection.RetiredStream` with an `output` field. The array is sorted by output name and stream id, and includes shared wrapped outputs as well as the wrapped accepted stream under `"@streamsy/accepted"`.
- **Custom stores:** Custom `RouteRecords` implementations must now provide `state` and `forget` (`fromStore` from `@streamsy/projection/checkpoint` supplies both). See [`RouteRecords`](https://streamsy.dev/docs/reference/projection/checkpoint-store#routerecords).

**Upgrade notes:**

- Bindings forgotten under 0.13.0 have no route records left, so `retire` cannot discover their routed streams.
- Keep 0.13.0 runners stopped after upgrading: a 0.13.0 runner trusts forgotten route records and can append replayed data into old streams.
- Routed `forget` (for an existing checkpoint) and routed `retire` fail with `unsupported-composition` when the owner store cannot list the binding's records (such as a MySQL range containing a legacy hashed key).

See [Forget](https://streamsy.dev/docs/projections/checkpoints#forget) and [Retire a generation](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation).

## Prevent `Projection.forget` cleanup from overlapping another host's first unit

In 0.13.0 on Postgres, a `Projection.forget` under `Commit.transactionWithCleanup` that saw no checkpoint could run its cleanup after another host committed the binding's first unit, leaving the checkpoint behind without its cleaned-up rows while reporting success.

- `Commit.transaction` and `Commit.transactionWithCleanup` now re-check the checkpoint after running cleanup. If a checkpoint exists, the forget rolls back and fails with a retryable `token-conflict`. This covers routed and unrouted bindings, including MySQL at `REPEATABLE READ` (requires `@streamsy/storage` at the same version).
- A cleanup callback that catches a SQL error now fails `forget` instead of reporting success on a rolled-back MySQL transaction.

Always stop and drain a binding's runners before calling `forget`. To clean up a binding left with a checkpoint and missing rows by an earlier release, stop its runners and forget it again, or raise `generation`. One limit remains: if two hosts race to commit a binding's first unit on Postgres or MySQL, one wins while the slower host can hit a non-retryable `invalid-output` (or `ViewDataFault` on a routed Views circuit), stopping the binding until `Registry.retry` or restart—run one host per binding. See [One writer](https://streamsy.dev/docs/projections/checkpoints#one-writer).
