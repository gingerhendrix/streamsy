---
packages:
  "@streamsy/projection": minor
---

## Trigger attached projections from local wake streams

Pass `triggers: [WakeClass]` to `Projection.attach` or `ProjectionWakeup.attach` to request bindings by source id, including inputs on other owners. Durable ingestion commits its cursor and pending binding marks together; unfinished work survives restarts on persistent storage. Non-durable ingestion saves the cursor but can lose unfinished requests on a stop. Triggered bindings use the existing registry, events and retry paths.
