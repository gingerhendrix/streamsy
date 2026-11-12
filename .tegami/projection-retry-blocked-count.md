---
packages:
  "@streamsy/projection": patch
  "@streamsy/serve": patch
---

## Count only bindings made due by a Durable Object retry

Registry retry returns zero while the host retains storage or deadline-read backoff. It leaves durable marks and in-memory retry requests unchanged. WakeupHost.state exposes an optional retryBlocked flag so projection can check the host before changing work.
