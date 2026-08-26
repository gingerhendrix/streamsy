# `@streamsy/views`

This package builds frozen, inspectable relation declarations. Construction performs no I/O and stores no callbacks, Effect schemas, proxies, clocks, or runtime services in a compiled plan. `collectPlanIssues` returns every independently detectable issue, while `checkPlan` exposes the same result through Effect's typed error channel.

Optional properties preserve absence separately from JSON `null`. `isPresent()` tests absence, `.value` explicitly unwraps a present value, and `orElse` supplies a fallback. Optional values cannot be ordered directly. A declaration should place an `isPresent()` filter before relying on `.value`; A1 records and checks the expression tree, while an execution engine owns runtime evaluation.

`top` is always bounded. Literal limits must be positive integers. Parameter limits require a positive integer `maximum`, which is copied into the plan. The last sort term must be a stable ascending key reference so ties cannot reorder between hosts.

`encodePlan` sorts object keys and preserves array order. `planHash` is eight-lowercase-hex FNV-1a over the canonical UTF-8 encoding. It is a change-detection identity, not a security digest.

A source or view declares its key as a row field name, or as an ordered tuple of field names for a composite key. `keyExpression` lowers that declaration to the row-scoped expression the plan carries, and a view's declared key is lowered exactly once, beneath any trailing bounded-order operator, so the key is never restated inside a query. Because the declaration is a field name, a consumer such as a state sink can read the same key as collection metadata without re-deriving it from an expression tree.

`changes(relation)` names a keyed relation's change stream, so a declaration can publish what happened to a relation without publishing the relation itself. It carries the relation it is derived from and its declared key, and it fixes `order: "arrival"`: a fact fold observes Durable Stream arrival order, so a change stream is served in that order and is never re-sorted by a domain field. A producer that needs a domain order must append in that order.

Scalar keys are strings, finite numbers, or booleans. Composite keys preserve the scalar values and their order in a JSON array; engines compare and canonically encode keys by value.
