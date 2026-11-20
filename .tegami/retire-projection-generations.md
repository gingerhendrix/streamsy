---
packages:
  "@streamsy/projection": minor
---

## Retire a projection generation

Call `Projection.retire(declaration, params, { generation })` to fence the generation's checkpoint and remove its exclusively owned generation-addressed outputs. Pass `streams: "close"` to keep their data and let readers reach the end. Use `StateTable.clear(table)` to delete a retired stream's cached rows and applied offset.

`ProjectionFault.reason` now includes the non-retryable `retired-generation` case. Add it to exhaustive switches over `reason`. See [Retire a generation](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation).
