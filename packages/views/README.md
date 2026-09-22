# @streamsy/views

Declare a keyed relation over sources as data, compile it to a plan, and
maintain its rows incrementally. Version 0.4.0 requires
`effect@4.0.0-rc.115`.

```sh
bun add @streamsy/views effect
```

The package has three pure parts: relation authoring, plan IR, and the incremental engine.

| Entry                  | Contents                                                        |
| ---------------------- | --------------------------------------------------------------- |
| `@streamsy/views`      | The authoring DSL and the plan compiler                         |
| `./ir`                 | The serializable plan contracts                                 |
| `./engine`             | The incremental graph runtime                                   |
| `./engine/conformance` | A full-recompute reference implementation for testing an engine |

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
- `changes(relation)` declares a relation's change stream in arrival order.
  A stream sink publishes it, and the host supplies the events.
- `planHash` is a change-detection identity, not a security digest.

## Incremental engine

`maintainGraph` from `@streamsy/views/engine` takes a plan, prior operator
state, and normalized source changes. It returns the next operator state,
row changes, and current rows without I/O. The host owns input delivery and
persistence.

For fold state between projection passes, use `Projection.fold` from
`@streamsy/projection`. Persist row sets in application tables through the
shared `SqlClient`; see the
[State guide](https://streamsy.dev/docs/projections/projections#state).

Guide: [streamsy.dev/docs/projections/views](https://streamsy.dev/docs/projections/views).

## License

MIT
