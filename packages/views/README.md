# @streamsy/views

Declare a keyed relation over sources as data, compile it to a plan, and
maintain its rows incrementally. Version 0.4.0 requires
`effect@4.0.0-rc.115`.

```sh
bun add @streamsy/views effect
```

The package has four tiers, one per entry:

| Entry                  | Contents                                                        |
| ---------------------- | --------------------------------------------------------------- |
| `@streamsy/views`      | The authoring DSL and the plan compiler                         |
| `./ir`                 | The serializable plan contracts                                 |
| `./engine`             | The incremental graph runtime                                   |
| `./engine/conformance` | A full-recompute reference implementation for testing an engine |
| `./store`              | The maintained-view store contract and its memory backend       |
| `./store/sqlite`       | The SQLite store backend                                        |
| `./store/conformance`  | The store conformance harness                                   |

## Authoring

A declaration is a frozen, inspectable value. Building one does no I/O and
stores no callbacks or runtime services, so a plan can be hashed, serialized,
and checked before anything runs. `checkPlan` reports plan issues through the
Effect error channel; `collectPlanIssues` returns all of them at once.

- Sources and views declare their key as a field name or a tuple of field
  names. A consumer can read that key as metadata without inspecting the plan.
- Optional properties keep absence separate from JSON `null`. Test with
  `isPresent()` before reading `.value`, or supply `orElse`.
- `top` is always bounded, and its last sort term must be a stable ascending
  key so ties never reorder between hosts.
- `changes(relation)` names a relation's change stream in arrival order. Use
  it to publish what happened without publishing the relation.
- `planHash` is a change-detection identity, not a security digest.

## Store

`ViewStore.commit` is the transaction boundary: rows, operator state, one
change batch, and the source cursor advance together or not at all. A stale
maintainer gets `ViewCursorConflict`. History positions are store-owned; reads
below the retention floor fail with `ViewHistoryExpired`. The SQLite backend
uses tables prefixed `streamsy_view_` with its own schema version.

Guide: [streamsy.dev/docs/projections/views](https://streamsy.dev/docs/projections/views).

## License

MIT
