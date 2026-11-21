---
packages:
  "@streamsy/projection": minor
---

## Route one binding's output to many streams

Pass `routeBy` to `Projection.perGeneration(route, generationParam, { routeBy: ["workspace", "date"] })` when one binding writes to several stream addresses. Each name in `routeBy` must be a distinct route parameter. The handler supplies those values, the generation fills the generation parameter, and the binding's params fill the rest. A routed output cannot be `shared`. [`Projection.perGeneration`](https://streamsy.dev/docs/reference/projection/constructors#projectionpergeneration)

Declare routed outputs in `Projection.process` with `Commit.transaction` or `Commit.transactionWithCleanup`. In `process`, `outputs[name]` is a `RoutedHandle`. Its `open(routeParams)` returns the stream address and creates the stream on first use, which must find the stream empty and open. The stream creation, its route record, the appends and the checkpoint commit in one transaction on the checkpoint owner. `OutputsContext.outputs` holds the address of each output with a fixed address and a `RoutedHandle` for each routed output. Open handles only in `process`, because `prepare` runs before the transaction.

`CheckpointStoreApi` gains an optional `routes` member of type `RouteRecords`, which records the streams that each binding's generation opened. `CheckpointStore.fromStore` implements it with `EncodedStore.list`. A checkpoint store without `routes` fails a routed open with `unsupported-composition`. `RouteRecords` and `RoutedStreamRecord` are exported from `@streamsy/projection` and `@streamsy/projection/checkpoint`. [`RouteRecords`](https://streamsy.dev/docs/reference/projection/checkpoint-store#routerecords)

`Projection.retire` finds routed streams through these records and reports them in the new optional `Retired.routed` field, as an array of results per output name. `Projection.RetiredOutput` is now exported. With `streams: "close"`, retirement keeps the route records so a later `"remove"` can delete the closed streams. `Projection.forget` clears the route records of a live binding in its checkpoint transaction. On MySQL, which cannot list owner records, routed retirement fails with `unsupported-composition`. [Retire a generation](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation)

`isPerGeneration` recognizes routed declarations, and `generationBinding` returns `routeBy` when it is set. `PerGeneration` takes an optional third type parameter for the route names.
