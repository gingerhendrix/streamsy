---
packages:
  "@streamsy/projection": minor
---

## Retire routed streams after forgetting a binding

`Projection.forget(bound)` now marks route records forgotten so `Projection.retire` can still find the streams. Replay checks that each forgotten stream starts empty and open. Pass the binding from `declaration.at(params)` with its routed outputs. MySQL lacks owner-record listing, so routed forget for an existing checkpoint and routed retirement fail with `unsupported-composition`.

`Projection.retire` can now write a tombstone over an undecodable checkpoint and clean up its streams in the same call. [Checkpoints and generations](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation)

**Breaking change:** `Retired.streams` replaces `Retired.outputs` and `Retired.routed`. `Projection.RetiredOutput` becomes `Projection.RetiredStream` with an `output` field. The array sorts by output name and stream id, and includes shared wrapped outputs and the wrapped accepted stream under `"@streamsy/accepted"`.

Custom `RouteRecords` implementations must provide `state` and `forget`. `fromStore` from `@streamsy/projection/checkpoint` supplies both. [RouteRecords reference](https://streamsy.dev/docs/reference/projection/checkpoint-store#routerecords)

Keep 0.13.0 runners stopped after upgrading. A 0.13.0 runner trusts forgotten route records and can append replayed data to old streams. [Forget](https://streamsy.dev/docs/projections/checkpoints#forget)
