---
packages:
  "@streamsy/projection": minor
---

## Publish accepted input offsets from projection declarations

Set `accepted: WakeClass` to publish wake rows in each unit's commit, through run, follow, attach, or ProjectionWakeup. State tables accept `StateTable.define(StateClass, { accepted: WakeClass })`. Accepted streams support `Commit.transaction` and `Commit.producer` only; a fold with `accepted` requires an explicit supported `commit` at compile time. Runtime validation remains a backstop for omitted or unsupported strategies.
