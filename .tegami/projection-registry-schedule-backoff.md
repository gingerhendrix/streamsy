---
packages:
  "@streamsy/projection": patch
---

## Resume scheduled bindings on Durable Objects

Registry schedule requests now resume a Durable Object runner during application-failure backoff and after a finite retry policy stops. Storage and deadline-read backoff remain in place.
