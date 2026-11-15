---
packages:
  "@streamsy/projection": minor
---

## Set a retry policy for each projection declaration

All five projection constructors accept `retry` with a timing `schedule`, a failed-run
budget `maxAttempts`, and an `onExhausted(unit, failure)` hook. Both attach hosts stop
exhausted bindings and list them in `registry.failed` with `exhausted: true`.
`registry.retry` permits another run. New input during backoff preserves the budget
and stored deadline; success clears the failure count. The default remains unlimited
retry with delays doubling from 1 s to 1 hour. Use schedules based on step count;
schedules depending on input or elapsed time are unsupported.

Hooks run at least once before stopping; use `unit.key` to dedupe. When the exhausting
failure occurs before a unit forms, the hook is skipped and logged. Hook failures are
logged and the binding stops anyway. Non-retryable faults stop without the hook.
`ProcessContext.attempt` starts at 1 for each binding and increases after failures;
direct `Projection.run` and `Projection.follow` always supply 1.

**Breaking change:** `AttachOptions.retry` is now `passRetry`. Rename the option on
`Projection.attach` and `ProjectionWakeup.attach`. Its meaning is unchanged: it paces a
failed drain pass. `retry` now names the binding retry policy on a declaration, and an
attach call that still passes `retry` fails with a `RangeError`.

See [binding retry](https://streamsy.dev/docs/reference/projection/runners#binding-retry).
