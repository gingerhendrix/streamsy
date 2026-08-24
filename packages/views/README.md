# `@streamsy/views`

This package builds frozen, inspectable relation declarations. Construction performs no I/O and stores no callbacks, Effect schemas, proxies, clocks, or runtime services in a compiled plan. `collectPlanIssues` returns every independently detectable issue, while `checkPlan` exposes the same result through Effect's typed error channel.

Optional properties preserve absence separately from JSON `null`. `isPresent()` tests absence, `.value` explicitly unwraps a present value, and `orElse` supplies a fallback. Optional values cannot be ordered directly. A declaration should place an `isPresent()` filter before relying on `.value`; A1 records and checks the expression tree, while an execution engine owns runtime evaluation.

`top` is always bounded. Literal limits must be positive integers. Parameter limits require a positive integer `maximum`, which is copied into the plan. The last sort term must be a stable ascending key reference so ties cannot reorder between hosts.

`encodePlan` sorts object keys and preserves array order. `planHash` is eight-lowercase-hex FNV-1a over the canonical UTF-8 encoding. It is a change-detection identity, not a security digest.

Scalar keys are strings, finite numbers, or booleans. Composite keys preserve the scalar values and their order in a JSON array; engines compare and canonically encode keys by value.
