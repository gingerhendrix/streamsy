---
packages:
  "@streamsy/projection": minor
---

## Stop a binding on a permanent handler error

Mark a typed handler error as permanent with `readonly [NonRetryable] = true`, using `NonRetryable` from `@streamsy/projection`. When an attached binding fails with that error, the runner stops the binding at once. It does not step the retry schedule or call `onExhausted`. `registry.failed` reports the binding as stopped with `exhausted: false` and failure reason `non-retryable-handler`. Other handler errors and defects still retry under the declaration's `retry` policy. [Binding retry](https://streamsy.dev/docs/reference/projection/runners#binding-retry)

```ts
import { Schema } from "effect";
import { NonRetryable } from "@streamsy/projection";

class UnknownAccount extends Schema.TaggedError<UnknownAccount>()("UnknownAccount", {
  account: Schema.String,
}) {
  readonly [NonRetryable] = true;
}
```
