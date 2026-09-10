# Streamsy

Streamsy provides an Effect-first [Durable Streams](https://durablestreams.com) protocol,
a typed toolkit, a storage contract, an in-process memory Layer and Effect SQL storage.

The `0.4.0` package surface is `@streamsy/core`, `@streamsy/storage`, `@streamsy/views`,
`@streamsy/serve` and `@streamsy/derive`. Use `@streamsy/storage/bun` for file-backed SQLite and
`@streamsy/storage/durable-object` inside a SQLite Durable Object. See [the API](docs/api.md), [hosting reference](docs/hosting.md),
[storage contract](docs/storage-contract.md), [SQLite migration policy](docs/migration-0.4.md)
and [HTTP behavior](docs/http.md).

The active examples are [Fold agent](examples/fold-agent/README.md) and
[Hacker News](examples/hackernews-newest-stream/README.md). The Bun protocol host
supports retained-file SQLite through `@streamsy/storage/bun`; Fold uses that same
Layer for retained-file restart and provider-free CLI recovery tests.
The local Cloudflare entry `@streamsy/serve/cloudflare` provides placement routing
and a Durable Object protocol host with local workerd evidence. The official
workerd profile uses one `byKey` object for chain semantics. Forks require the
source and child to share an object. Hosted execution, release acceptance, and the budget
decision remain pending; see the [hosting reference](docs/hosting.md).

Derive commits stream output, current state and source progress in one host-local
transaction. See [the Derive guide](packages/derive/README.md) for memory, Bun SQLite
and same-object Durable Object SQLite composition.

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
