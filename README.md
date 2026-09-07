# Streamsy

Streamsy provides an Effect-first [Durable Streams](https://durablestreams.com) protocol,
a typed toolkit, a storage contract, an in-process memory Layer and Effect SQL storage.

The `0.4.0` package surface is `@streamsy/core`, `@streamsy/storage`, `@streamsy/views`
and `@streamsy/serve`. Use `@streamsy/storage/bun` for file-backed SQLite and
`@streamsy/storage/durable-object` inside a SQLite Durable Object. See [the API](docs/api.md),
[storage contract](docs/storage-contract.md), [SQLite migration policy](docs/migration-0.4.md)
and [HTTP behavior](docs/http.md).

The active examples are [Fold agent](examples/fold-agent/README.md) and
[Hacker News](examples/hackernews-newest-stream/README.md). They still use memory;
wiring Fold restart and the Bun protocol host to SQLite remains Step 2 follow-up.
Hosted Durable Object protocol delivery and an Effect fetch transport remain later work.

## Development

```sh
bun install
bun run typecheck
bun run lint
bun run lint:policy
bun run format:check
bun run test:unit
bun run test:conformance
bun run check:perimeter
bun run pack:dry-run
```

## License

MIT
