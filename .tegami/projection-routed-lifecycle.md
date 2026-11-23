---
packages:
  "@streamsy/projection": minor
---

## Retire routed streams after forgetting a binding

`Projection.forget(bound)` now keeps route records and marks them forgotten. The runner does the same after the final unit for a gone input. `Projection.retire` can still find the streams, and replay checks that each forgotten stream is empty and open on its first `open`. Pass the binding from `declaration.at(params)` to `forget`. A bare key leaves the route records live and allows a replay to append to old data.

`Projection.retire` can now write a tombstone over an undecodable checkpoint and clean up its streams in the same call. [Checkpoints and generations](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation)

**Breaking change:** `Retired.streams` replaces `Retired.outputs` and `Retired.routed`. `Projection.RetiredOutput` is now `Projection.RetiredStream`, with an `output` field. The array includes shared wrapped outputs and a wrapped accepted stream under `"@streamsy/accepted"`. It sorts by output name and then stream id in code-unit order.

Before

```ts
const retired = {
  record: "retired",
  outputs: { totals: { id: "totals/g/1", result: "removed" } },
  routed: { rows: [{ id: "rows/east/g/1", result: "removed" }] },
};
```

After

```ts
const retired = {
  record: "retired",
  streams: [
    { output: "rows", id: "rows/east/g/1", result: "removed" },
    { output: "totals", id: "totals/g/1", result: "removed" },
  ],
};
```

`RouteRecords` now requires `state(key, output, streamId)` and `forget(key)`. Add these methods if you implement your own `routes` service. `CheckpointStore.fromStore` supplies both. [RouteRecords reference](https://streamsy.dev/docs/reference/projection/checkpoint-store#routerecords)

Do not run 0.13.0 against a store after this release has forgotten a routed binding. 0.13.0 treats forgotten route records as live and can append replayed data to old streams. Bindings forgotten under 0.13.0 have no route records left, so retirement cannot discover their routed streams. [Forget](https://streamsy.dev/docs/projections/checkpoints#forget)
