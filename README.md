# Streamsy

Streamsy is a work-in-progress Durable Streams server implementation. It is currently focused on implementing the `@durable-streams/server-conformance-tests` protocol target for both in-memory and Cloudflare Durable Object storage backends.

## Status

WIP. The current local implementation has been validated against `@durable-streams/server-conformance-tests@0.3.0` with both storage backends passing the full conformance suite:

- `@streamsy/storage-memory`: 299/299 passing
- `@streamsy/storage-durable-object` (Cloudflare Durable Object storage): 299/299 passing

The API and package boundaries are not yet stable, and all packages remain private while the project is being shaped.

## Packages

- `@streamsy/core` (`packages/core`) — shared protocol types, request handling, response helpers, and server logic used by storage backends.
- `@streamsy/storage-memory` (`packages/storage-memory`) — in-memory storage backend used for local development and conformance testing.
- `@streamsy/storage-durable-object` (`packages/storage-durable-object`) — Cloudflare Durable Object storage backend with SQLite-backed persistence, long-polling, SSE support, TTL/expiry handling, and stream metadata.
- `@streamsy/conformance-tests` (`packages/conformance-tests`) — private conformance harness for the memory and Durable Object adapters.

## Examples

- `examples/memory-server` — Bun/HTTP server using the memory storage backend.

## Development

```bash
bun install
bun run typecheck
bun run test:unit
bun run test:conformance:memory
```

Durable Object conformance uses an Alchemy-managed Cloudflare deployment and can take longer than local checks:

```bash
bun run test:conformance:do
```

`test:conformance:do` deploys the `packages/conformance-tests` worker with `STAGE=conformance`, runs the Durable Object conformance tests against the deployed workers.dev URL, and destroys the conformance deployment in a `finally` step.

## License

MIT
