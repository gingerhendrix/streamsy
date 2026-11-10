---
packages:
  "@streamsy/projection": minor
  "@streamsy/core": minor
---

## State tables and keyed folds

`StateTable` keeps the rows of a Durable State stream in SQL tables on the stream's storage. `StateTable.define(State)` returns an ordinary projection declaration, so `Projection.attach`, `ProjectionWakeup.attach`, `follow` and `run` keep the table current. Attach it with `durable: true`. The table stores the canonical JSON of each change's value and never decodes it with the collection schema, so `StateTable.rebuild` makes the same rows from the stream alone. A bound table reads rows with `get`, `getMany` and `list`, each with the offset that the rows reflect. `sync` applies the stream up to its tail in the caller's transaction, and fails only with a `ProjectionFault`. The declaration and the bound table have the types `StateTable.Declaration` and `StateTable.Bound`.

Run `StateTable.migrate(sql)` once at host start. It creates `streamsy_state_heads` and `streamsy_state_rows` outside the storage migrations, so the storage version does not change. The tables support the SQLite family only.

`FoldByKey.process(table, spec)` builds a `Projection.process` handler that folds input records into one collection of a State stream. Each unit calls `sync`, loads the previous rows of its keys, and appends one change for each changed key. The handler has the type `FoldByKey.Handler`. A row whose key field differs from its key fails the unit with a typed `FoldByKeyError`. A wire, SQL or schema failure fails it with a `ProjectionFault`. A callback that throws is a defect, as in every other handler kind. Attach folds with `durable: true`.

`Projection.attach` now logs and retries a pass that fails with a defect. Before, a handler that threw under memory-mode `attach` ended the runner of that projection with no log.

In `@streamsy/core`, a `Streamsy.state` class and a `StreamRoute.state` route keep their `params` codec record as `paramCodecs`. `StateTable.define` reads it.

**Breaking change:** a `Projection.process` handler takes a context object as its third argument, in place of `prepared`. Write `process: (batch, unit, { prepared }) => ...` where you wrote `process: (batch, unit, prepared) => ...`. A handler that takes only `(batch, unit)` is unchanged, and so is `prepare`. The context types are `ProcessContext` and `OutputsContext`.

`Projection.process` takes an optional `outputs` map under `Commit.transaction`. The Commit checks each output before the handler runs, and `prepare` and `process` get the bound output addresses as `context.outputs`.

A fold's `id`, `version` and `generation` are its identity. A renamed fold folds its sources again over the old rows and doubles counters with no error, so treat a rename as a fold fix. See [Operate State tables](https://streamsy.dev/docs/projections/state-table-operations).
