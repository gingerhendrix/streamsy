---
packages:
  "@streamsy/projection": patch
  "@streamsy/serve": patch
---

## Report explicitly armed retries as retrying

After a registry request resumes a failed Durable Object runner, its status now reports retrying with the reconciled retry time until the next pass starts. Changes subscribers also receive the request transition. Failed status remains reserved for stopped retries with no pending alarm for that runner.
