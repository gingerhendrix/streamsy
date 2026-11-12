---
packages:
  "@streamsy/serve": minor
---

## Bind wakeups to their Durable Object host

Wakeups can implement an optional `bind` hook to receive alarm reconciliation, retry state and application-backoff reset effects. Reconciliation also works inside requests, RPC methods and wakeup runs. An optional `changed` notification observes the retry state after a pass.
