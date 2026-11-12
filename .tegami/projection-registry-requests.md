---
packages:
  "@streamsy/projection": minor
---

## Schedule and retry attached projection bindings

`Projection.Registry` now offers `schedule(binding)` and `retry(declarationOrBinding)` on process hosts and Durable Objects. Requests wake the runner or arm the object's alarm without another source write. `NotAttached` identifies declarations with no registered runner.

Retry preserves durable attempts and counts failed bindings. Non-durable runners track failures separately from bounded work and can interrupt pass backoff. Durable Object runners register in the object's registry and report the host's retry state. Retry clears binding-failure backoff while retaining storage-fault backoff.
