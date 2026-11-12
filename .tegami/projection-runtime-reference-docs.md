---
packages:
  "@streamsy/core": patch
  "@streamsy/storage": patch
  "@streamsy/serve": patch
---

## Update projection runtime reference signatures

The storage references now include durable mark summaries and retries, and the Cloudflare reference documents wakeup host binding and retry state. Their checked signatures match the current projection runtime.
