---
packages:
  "@streamsy/projection": patch
  "@streamsy/serve": patch
---

## Read registry snapshots only for observers

Projection passes now publish change notifications without reading runner status or durable mark summaries. Each changes subscriber reads the current full snapshot as it consumes a notification. Durable Object alarms notify only wakeups whose run was attempted.
