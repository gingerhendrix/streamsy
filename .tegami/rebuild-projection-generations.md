---
packages:
  "@streamsy/projection": minor
  "@streamsy/core": minor
  "@streamsy/storage": minor
---

## Rebuild a projection into a new generation

Raise a declaration's `generation` and attach it, and the runner now replays the existing bindings into the new generation in the background. On start, `Projection.attach` and `ProjectionWakeup.attach` list the live checkpoints for the projection id. They queue each binding that has no live checkpoint at the declared generation, create its generation-addressed outputs, and mark it for replay. A projection with no params also queues its `{}` binding. [Rebuild a projection](https://streamsy.dev/docs/projections/rebuild)

Pass `bindings` to either attach form to choose the replayed bindings yourself. It is an Effect that returns decoded params, such as `Projection.bindings(oldDeclaration)` or `Effect.succeed([{ user: "u1" }])`. Use it when you run a new projection id beside the old one, on MySQL, or when bindings that never committed a unit must replay. [Choose the binding inventory](https://streamsy.dev/docs/projections/rebuild#choose-the-binding-inventory)

Call `Projection.bindings(declaration, { generation })` to list the decoded params of the bindings with live checkpoints for a projection id. Without `generation` it lists every generation. It fails with `InvalidBinding` when stored params no longer decode, and with `unsupported-composition` on MySQL and on checkpoint stores that cannot list.

Capture `Projection.tails(binding)` once, then wait with `Projection.caughtUp(binding, tails)` until the binding's checkpoint reaches those input offsets and closes. Add your own timeout. `Projection.lag(binding, tails)` returns the committed and target offset of each input and a `caughtUp` flag. `Tails` is a Schema, so you can store the captured vector and check it later. [Check replay progress](https://streamsy.dev/docs/projections/rebuild#check-replay-progress)

When startup cannot list bindings, or the `bindings` Effect fails, attach logs the cause and skips startup replay. Live commits and triggers still run.

Trigger cursors are now kept for each generation, so a new generation reads its trigger streams from the start. After you upgrade, the first pass of each attached projection with `triggers` also reads them from the start, because the stored cursors have no generation. Expect that pass to schedule every member named in the retained trigger history once.

`CheckpointStoreApi` and `EncodedStore` gain an optional `list` member. `CheckpointStoreApi.list(id)` returns a `ListedBinding` (`generation` and encoded `params`) for each live checkpoint of the id. `CheckpointStore.layer` and `CheckpointStore.layerMemory` implement it. A custom checkpoint store without `list` keeps working, but attach skips implicit startup replay and `Projection.bindings` fails. To support them, implement `EncodedStore.list(prefix)` for a store built with `fromStore`, or `CheckpointStoreApi.list` for a store written from scratch. [`CheckpointStoreApi`](https://streamsy.dev/docs/reference/projection/checkpoint-store#checkpointstoreapi)

**Breaking change:** the `OwnerRecords` service from `@streamsy/core/internal` now requires `list(prefix)`. It returns the owner's `[key, encoded]` entries whose keys start with the prefix. The memory storage and the SQLite and PostgreSQL storage in `@streamsy/storage` implement it. MySQL storage fails it with a `StorageFault` whose `operation` is `OWNER_RECORDS_LIST_UNSUPPORTED`, because MySQL may have shortened long keys. If you provide `OwnerRecords` from your own storage host, add `list`, or fail with that operation when your store cannot list. [Memory storage internal services](https://streamsy.dev/docs/reference/core/memory-storage#types-with-no-exported-name)
