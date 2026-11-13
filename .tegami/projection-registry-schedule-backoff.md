---
packages:
  "@streamsy/projection": patch
---

## Resume requested bindings on both hosts

Registry schedule and retry requests end the current wait and start one pass on Bun and Durable Objects, including after storage or deadline-read failures and an exhausted finite retry policy. Requests preserve the retry schedule; another failure waits its next delay.
