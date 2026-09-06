# Parked examples

Parked outside root build inputs by Step 1; restoration needs a separate scope decision.

These sources are historical reference, excluded from root workspaces, builds,
typechecks, lint, format, and tests. Their relative paths and dependencies have
not been adapted for standalone use. Each example was last green at `0d895c2`.

| Parked directory | Last green commit |
| --- | --- |
| `examples/issue-tracker` | `0d895c2` |
| `examples/issue-tracker-demo` | `0d895c2` |
| `examples/issue-tracker-projections` | `0d895c2` |
| `examples/risk-demo` | `0d895c2` |
| `examples/memory-server` | `0d895c2` |

## Source import inventory

Rows copied verbatim from the Step 0 `inventory/workspace-import-summary.tsv`
(consumer paths are the original locations):

```tsv
consumer	entrypoints
examples/issue-tracker	@streamsy/core, @streamsy/sinks, @streamsy/sinks/action, @streamsy/sinks/action/errors, @streamsy/sinks/action/outbox, @streamsy/sinks/action/runtime, @streamsy/sinks/action/sqlite, @streamsy/sinks/document, @streamsy/sinks/fingerprint, @streamsy/sinks/server/document, @streamsy/sinks/server/state, @streamsy/sinks/server/stream, @streamsy/sinks/stream, @streamsy/state, @streamsy/storage/sqlite, @streamsy/streams, @streamsy/streams/binding, @streamsy/streams/identity, @streamsy/tanstack-db, @streamsy/views, @streamsy/views/engine, @streamsy/views/ir, @streamsy/views/store, @streamsy/views/store/sqlite
examples/issue-tracker-demo	@streamsy/core, @streamsy/core/json
examples/issue-tracker-projections	@streamsy/core, @streamsy/projection/mesh, @streamsy/storage/durable-object, @streamsy/storage/durable-object/storage, @streamsy/storage/sqlite, @streamsy/streams, @streamsy/streams/binding, @streamsy/streams/causal, @streamsy/streams/identity
examples/memory-server	@streamsy/core
examples/risk-demo	@streamsy/core, @streamsy/core/json, @streamsy/projection/mesh, @streamsy/storage/sqlite, @streamsy/streams, @streamsy/streams/binding, @streamsy/streams/causal, @streamsy/streams/identity
```

The removed `@streamsy/tanstack-db` package is replaced by the official
`@durable-streams/state/db` binding. The live sink package is now `@streamsy/serve`.
