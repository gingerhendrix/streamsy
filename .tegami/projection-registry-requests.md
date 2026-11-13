---
packages:
  "@streamsy/projection": minor
---

## Schedule and retry attached projection bindings

`Projection.Registry` now offers `schedule(binding)` and `retry(idOrDeclarationOrBinding)` on process hosts and Durable Objects. Requests wake the runner or arm the object's alarm without another source write. `NotAttached` identifies projection ids with no registered runner. Bindings resolve by id and their encoded params are decoded by the attached declaration.

Retry preserves durable attempts and counts failed bindings. Non-durable runners track failures separately from bounded work and can interrupt pass backoff. Durable Object runners register in the object's registry and report the host's retry state. Every request ends the current wait, whatever caused it. Repeated non-durable retries count the same failed bindings until they run successfully or are dropped; durable retries retain the existing count of marks changed.
