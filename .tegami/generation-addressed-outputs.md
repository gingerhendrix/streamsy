---
packages:
  "@streamsy/projection": minor
---

## Replay into a fresh stream for each generation

Wrap a projection output or its accepted wake class with `Projection.perGeneration(route, param)` to put the declaration's generation in the stream address. Raise the generation when you rebuild, and pin readers to the previous address until the new streams catch up. The built-in commits check for an empty stream on the first write by default. Use `Projection.createOutputs(bound)` when readers need the wrapped streams before the first unit. [Generation-addressed outputs](https://streamsy.dev/docs/projections/outputs#generation-addressed-outputs)
