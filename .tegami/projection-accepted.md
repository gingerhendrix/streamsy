---
packages:
  "@streamsy/projection": minor
---

## Publish accepted input offsets from projection declarations

Set `accepted: WakeClass` to publish wake rows in each unit's commit, through run, follow, attach, or ProjectionWakeup. State tables accept `StateTable.define(StateClass, { accepted: WakeClass })`. Accepted streams support `Commit.transaction` and `Commit.producer` only; other strategies, including the fold default, fail when the declaration is built. Folds opt in to a supported strategy explicitly.
