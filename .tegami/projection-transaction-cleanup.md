---
packages:
  "@streamsy/projection": minor
---

## Clean up SQL state with a projection binding

Use `Commit.transactionWithCleanup` to remove a binding's SQL state in the same transaction as `Projection.forget` or `Projection.retire`. Use the supplied SQL client so a cleanup failure rolls back the lifecycle operation and the cleanup SQL. [Commit reference](https://streamsy.dev/docs/reference/projection/commit#committransactionwithcleanup)

Use `Projection.isPerGeneration` to recognize a generation-addressed output and `Projection.generationBinding` to read its route, generation parameter, and `shared` flag. [Generation-addressed outputs](https://streamsy.dev/docs/projections/outputs#generation-addressed-outputs)
