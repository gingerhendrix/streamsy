# `@streamsy/views`

One package holds the four tiers of the Streamsy view stack: the authoring DSL and plan compiler at the root, the serializable plan IR under `ir/`, the incremental graph runtime under `engine/`, and maintained-view storage under `store/`. They were `@streamsy/views`, `@streamsy/views-ir`, `@streamsy/views-engine` and `@streamsy/views-store` before `0.3.0`.

## Public subpaths

| Subpath                | Entry module               | Contents                                    |
| ---------------------- | -------------------------- | ------------------------------------------- |
| `.`                    | `src/relation.ts`          | the authoring DSL and the plan compiler     |
| `./ir`                 | `src/ir/contracts.ts`      | the serializable plan contracts             |
| `./engine`             | `src/engine/engine.ts`     | the incremental graph runtime               |
| `./engine/conformance` | `src/engine/reference.ts`  | the full-recompute reference implementation |
| `./store`              | `src/store/memory.ts`      | the store contracts and the memory backend  |
| `./store/sqlite`       | `src/store/sqlite.ts`      | the SQLite backend                          |
| `./store/conformance`  | `src/store/conformance.ts` | the store conformance harness               |

Every other module is internal and is reached by relative import inside the package. A module earns a subpath only when a consumer outside this package imports it.

## Purity rules

These were package dependency edges before the merge. They are now module conventions, and review enforces them:

- `engine/` imports only from `ir/`. It never imports the authoring root or `store/`.
- `store/` imports neither `engine/` nor the authoring root. It depends on `effect` alone.
- The authoring root imports `ir/` and nothing else in the package.

`ir/contracts.ts` and `store/contracts.ts` each declare their own `JsonValue` and `RowKey`. The two `RowKey` types are not the same type and are deliberately not unified; see `views-primitives.md` in the design stream.

## Authoring

This package builds frozen, inspectable relation declarations. Construction performs no I/O and stores no callbacks, Effect schemas, proxies, clocks, or runtime services in a compiled plan. `collectPlanIssues` returns every independently detectable issue, while `checkPlan` exposes the same result through Effect's typed error channel.

Optional properties preserve absence separately from JSON `null`. `isPresent()` tests absence, `.value` explicitly unwraps a present value, and `orElse` supplies a fallback. Optional values cannot be ordered directly. A declaration should place an `isPresent()` filter before relying on `.value`; A1 records and checks the expression tree, while an execution engine owns runtime evaluation.

`top` is always bounded. Literal limits must be positive integers. Parameter limits require a positive integer `maximum`, which is copied into the plan. The last sort term must be a stable ascending key reference so ties cannot reorder between hosts.

`encodePlan` sorts object keys and preserves array order. `planHash` is eight-lowercase-hex FNV-1a over the canonical UTF-8 encoding. It is a change-detection identity, not a security digest.

A source or view declares its key as a row field name, or as an ordered tuple of field names for a composite key. `keyExpression` lowers that declaration to the row-scoped expression the plan carries, and a view's declared key is lowered exactly once, beneath any trailing bounded-order operator, so the key is never restated inside a query. Because the declaration is a field name, a consumer such as a state sink can read the same key as collection metadata without re-deriving it from an expression tree.

`changes(relation)` names a keyed relation's change stream, so a declaration can publish what happened to a relation without publishing the relation itself. It carries the relation it is derived from and its declared key, and it fixes `order: "arrival"`: a fact fold observes Durable Stream arrival order, so a change stream is served in that order and is never re-sorted by a domain field. A producer that needs a domain order must append in that order.

Scalar keys are strings, finite numbers, or booleans. Composite keys preserve the scalar values and their order in a JSON array; engines compare and canonically encode keys by value.

## Store

Durable maintained-view state shared by the memory and Bun SQLite backends.

`ViewStore.commit` is the transaction boundary. Row mutations, opaque operator values and indexes, reducer working state, one ordered change batch, and the after-exclusive source cursor either advance together or do not advance. A caller supplies the expected prior cursor; stale maintainers receive `ViewCursorConflict`.

History positions are store-owned `{ epoch, sequence }` values. Retention removes complete batches and persists a floor; reads below it fail with `ViewHistoryExpired`. Transport adapters may map that result to snapshot fallback, but must not expose the position as a Durable Streams offset or token.

Checkpoint generations contain the plan hash, source identity and cursor, reducer identity and version, and a counted set of entries. SQLite creates and activates a generation in one transaction. `recover` loads the latest compatible generation and asks its source dependency for the strictly-after suffix.

The SQLite tables use the `streamsy_view_` prefix and the independent `streamsy_view_schema_version` migration namespace. The store tier does not modify retired protocol SQLite tables. `importLegacyIssueStore` can copy the accepted Slice 1 `view_rows`, `reducer_state`, and `view_progress` rows once while leaving the old tables intact.

The operator seam is deliberately opaque. A runtime supplies namespace IDs, canonical JSON keys, private JSON values, and exact/ordered index mutations. The store tier never branches on an IVM node kind.
