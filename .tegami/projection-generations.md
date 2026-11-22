---
packages:
  "@streamsy/projection": minor
---

## Rebuild a projection into a new generation

You can now change a projection's handler and replay its inputs into fresh output streams, while readers stay on the old streams until you move them. The steps are:

1. Wrap the projection's outputs with `Projection.perGeneration`, so each generation writes to its own streams.
2. Raise `generation` and attach. The runner replays the existing bindings in the background.
3. Wait with `Projection.caughtUp`, then move readers to the new streams.
4. Call `Projection.retire` to close or remove the old generation.

The next four sections describe each step. [Rebuild a projection](https://streamsy.dev/docs/projections/rebuild) is the full guide, with an in-place procedure and a side-by-side procedure.

## Put the generation in an output's address

Wrap a projection output, or the wake class in `accepted`, with `Projection.perGeneration(route, param)`. The named route parameter takes the declaration's `generation`. The binding's params fill the other parameters.

```ts
class Doubled extends Streamsy.json<Doubled>()("Derived/Doubled", "doubled/:user/g/:rev", {
  params: { user: Schema.String, rev: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

const doubled = Projection.emit({
  id: "doubled",
  generation: 2,
  params: { user: Schema.String },
  inputs: { input: Input },
  outputs: { doubled: Projection.perGeneration(Doubled, "rev") },
  commit: Commit.transaction,
  emit: ({ item }) => ({ doubled: [item * 2] }),
});

// The projection writes to doubled/u1/g/2. A reader pins an address the same way.
const pinned = Doubled.at({ user: "u1", rev: 2 });
```

`Commit.transaction` and `Commit.producer` create a wrapped stream on its first write and check that it starts empty. A failed check fails the unit with the non-retryable reason `invalid-output`. Pass `{ shared: true }` as the third argument to skip the check for a stream that several writers share. Call `Projection.createOutputs(bound)` when readers need the streams before the first unit commits.

`Projection.isPerGeneration` recognizes a wrapped output, and `Projection.generationBinding` reads its route, generation parameter, `shared` flag and `routeBy` names. [Generation-addressed outputs](https://streamsy.dev/docs/projections/outputs#generation-addressed-outputs)

## Replay existing bindings when you attach a new generation

When you raise `generation` and attach the declaration, `Projection.attach` and `ProjectionWakeup.attach` list the live checkpoints for the projection id. Each binding with no checkpoint at the declared generation gets its generation-addressed outputs created and is queued for replay. A projection with no params queues its `{}` binding.

Pass `bindings` to either attach form to choose the replayed bindings yourself. It is an Effect that returns decoded params. Use it when you run a new projection id beside the old one, on MySQL, or when bindings that never committed a unit must replay.

```ts
Projection.attach(totalsV2, { durable: true, bindings: Projection.bindings(totalsV1) });
```

`Projection.bindings(declaration, { generation })` lists the decoded params of the bindings with live checkpoints for a projection id. Without `generation` it lists every generation. It fails with `InvalidBinding` when stored params no longer decode, and with `unsupported-composition` on MySQL and on checkpoint stores that cannot list.

When startup cannot list bindings, or the `bindings` Effect fails, attach logs the cause and skips startup replay. Live commits and triggers still run. [Choose the binding inventory](https://streamsy.dev/docs/projections/rebuild#choose-the-binding-inventory)

## Check replay progress

Capture `Projection.tails(binding)` once, then wait with `Projection.caughtUp(binding, tails)` until the binding's checkpoint reaches those input offsets and closes. Producers can keep writing while you wait. `caughtUp` has no timeout, so add your own.

`Projection.lag(binding, tails)` returns the committed and target offset of each input and a `caughtUp` flag. `Tails` is a Schema, so you can store the captured value and check it later. [Check replay progress](https://streamsy.dev/docs/projections/rebuild#check-replay-progress)

## Retire the old generation

Call `Projection.retire(declaration, params, { generation })` to fence the generation's checkpoint and remove its exclusively owned generation-addressed outputs. Pass `streams: "close"` to keep their data and let readers reach the end. The returned `Retired` value reports a `RetiredOutput` for each output. A later run of the retired generation fails with the non-retryable reason `retired-generation`. [Retire a generation](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation)

Two more calls remove what a retired binding leaves in SQL:

- `Commit.transactionWithCleanup(cleanup)` runs your cleanup in the same transaction as `Projection.forget` or `Projection.retire`. Run the cleanup SQL with the supplied client, so a failure rolls back the lifecycle call and the cleanup together. [Commit reference](https://streamsy.dev/docs/reference/projection/commit#committransactionwithcleanup)
- `StateTable.clear(table)` deletes a retired stream's cached rows and applied offset. [Clear a retired stream's rows](https://streamsy.dev/docs/projections/state-table-operations#clear-a-retired-streams-rows)

## Route one binding's output to many streams

Pass `routeBy` to `Projection.perGeneration(route, generationParam, { routeBy: ["workspace", "date"] })` when one binding writes to several stream addresses. Each name in `routeBy` must be a distinct route parameter. The handler supplies those values, the generation fills the generation parameter, and the binding's params fill the rest. A routed output cannot be `shared`. [`Projection.perGeneration`](https://streamsy.dev/docs/reference/projection/constructors#projectionpergeneration)

Declare routed outputs in `Projection.process` with `Commit.transaction` or `Commit.transactionWithCleanup`. In `process`, `outputs[name]` is a `RoutedHandle`. Its `open(routeParams)` returns the stream address and creates the stream on first use, which must find the stream empty and open. The stream creation, the appends and the checkpoint commit in one transaction.

`Projection.retire` closes or removes the streams that a routed output opened and reports them in `Retired.routed`, an array of `RetiredOutput` for each output name. `Projection.forget` clears the binding's record of those streams. On MySQL, which cannot list owner records, routed retirement fails with `unsupported-composition`.

## Check these changes when you upgrade

- **Triggers rescan once.** Trigger cursors are now kept for each generation, so a new generation reads its trigger streams from the start. The stored cursors have no generation, so the first pass of each attached projection with `triggers` after the upgrade also reads them from the start. Expect that pass to schedule every member named in the retained trigger history once.
- **New fault reason.** `ProjectionFault.reason` gains `retired-generation`. Add it to exhaustive switches over `reason`.
- **Custom checkpoint stores.** `CheckpointStoreApi` gains two optional members, `list` and `routes`. `CheckpointStore.layer` and `CheckpointStore.layerMemory` implement both. A custom store without them keeps working, but attach skips startup replay, `Projection.bindings` fails, and opening a routed output fails with `unsupported-composition`. For a store built with `CheckpointStore.fromStore`, implement `EncodedStore.list(prefix)` to get both. [`CheckpointStoreApi`](https://streamsy.dev/docs/reference/projection/checkpoint-store#checkpointstoreapi)
