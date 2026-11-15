---
packages:
  "@streamsy/projection": minor
---

## Follow wake streams on another owner

`Projection.attach` can now use a wake stream on another owner in `triggers`.
Provide its stream class through the remote or other owner's protocol layer.
A scoped live-read follower wakes the runner on new rows. Each remote page's
cursor commits locally with its durable pending marks, so restart resumes from
that page boundary.

Remote read failures use `passRetry` after local bindings run; local commits
during pass backoff wait until it ends (up to 30 seconds by default). Invalid
trigger data stops only that stream. `ProjectionWakeup.attach` rejects remote
triggers with a `RangeError` naming the stream; it accepts only triggers on its
own storage owner. See [Trigger streams](https://streamsy.dev/docs/projections/run#trigger-streams).
