# Views shop

A shop circuit maintains each customer's order count, total revenue, and highest-value
order. `Projection.perGeneration` gives each generation its own State output stream.

The smoke runs against a fresh in-memory Bun SQLite database. It writes, edits, and
removes orders, checking both output collections after each run. It then rebuilds
with a minimum order value in generation 2 and verifies that retiring generation 1
removes its stream and prevents it from running again.

Run from the repository root.

```bash
bun install --frozen-lockfile
bun run build
bun run smoke:views-shop
```

After building, `bun run smoke` also works from `examples/views-shop`. This example
and `@streamsy/views` are private workspace packages. Views supports SQLite only.
