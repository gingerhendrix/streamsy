# Streamsy

Streamsy provides an Effect-first [Durable Streams](https://durablestreams.com) protocol,
a typed toolkit, a storage contract and an in-process memory Layer.

The `0.4.0` package surface is `@streamsy/core`, `@streamsy/views` and `@streamsy/serve`.
Use `@streamsy/serve/bun` for the memory HTTP host. See [the API](docs/api.md),
[storage contract](docs/storage-contract.md) and [HTTP behavior](docs/http.md).

The active examples are [Fold agent](examples/fold-agent/README.md) and
[Hacker News](examples/hackernews-newest-stream/README.md). Their proofs use memory;
Fold reconstruction lasts for one store lifetime. Persistent protocol storage,
hosted Durable Objects and an Effect fetch transport remain later work.
The site content is awaiting its separate Step 1 Batch 7 rewrite.

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
