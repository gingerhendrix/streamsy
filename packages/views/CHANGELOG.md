## @streamsy/views@0.13.0

### Maintain incremental SQL views with `@streamsy/views`

`@streamsy/views` is now published. Install it with `bun add @streamsy/views @streamsy/projection @streamsy/core effect`.

Build relations over `Streamsy.state` collections with `View.from`, then join, filter, select, group with `View.count`, `View.sum`, `View.min` and `View.max`, and take a bounded `top`. `View.circuit` compiles the relations into one projection. Each unit updates only the affected keys in SQLite operator tables, appends the changed rows to generation-addressed State output streams, and commits the input checkpoint in the same transaction. Wrap the output with `Projection.perGeneration` and create the tables with `View.migrate`.

Views supports SQLite only: Bun SQLite and Cloudflare Durable Object SQLite. `View.migrate` fails on Postgres and MySQL.

The package is pre-1.0. When you change a circuit's plan, raise its `generation` so the binding rebuilds into a fresh namespace and output stream. A circuit whose stored plan hash differs fails with `PlanChanged`. A later release that changes the operator-table layout will mark it as a breaking change, and you raise `generation` on every affected circuit when you upgrade. [Views](https://streamsy.dev/docs/views)
