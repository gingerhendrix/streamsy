# Streamsy

Streamsy is a work-in-progress Durable Streams server implementation. It is currently focused on implementing the `@durable-streams/server-conformance-tests` protocol target for both in-memory and Cloudflare Durable Object storage backends.

## Status

WIP. The current local implementation has been validated against `@durable-streams/server-conformance-tests@0.3.0` with both example servers passing the full conformance suite:

- `examples/memory-server`: 299/299 passing
- `examples/test-server` (Cloudflare Durable Object storage): 299/299 passing

The API and package boundaries are not yet stable, and all packages remain private while the project is being shaped.

## Packages

- `@streamsy/core` (`packages/core`) — shared protocol types, request handling, response helpers, and server logic used by storage backends.
- `@streamsy/storage-memory` (`packages/storage-memory`) — in-memory storage backend used for local development and conformance testing.
- `@streamsy/storage-durable-object` (`packages/storage-durable-object`) — Cloudflare Durable Object storage backend with SQLite-backed persistence, long-polling, SSE support, TTL/expiry handling, and stream metadata.

## Examples

- `examples/memory-server` — Bun/HTTP server using the memory storage backend and conformance tests.
- `examples/test-server` — Cloudflare Worker/Durable Object example deployed with Alchemy and exercised by the conformance tests.

## Development

```bash
bun install
bun run typecheck
bun run --cwd examples/memory-server test
bun run --cwd examples/test-server test
```

The Durable Object conformance test command above expects an Alchemy dev server via `SERVER_BASE_URL` (defaulting to `http://localhost:1337`). To validate against a deployed Cloudflare Worker instead, use the dedicated conformance stage script:

```bash
bun run test:conformance
```

This deploys `examples/test-server` with `STAGE=conformance`, runs the Durable Object conformance tests against the deployed workers.dev URL, and destroys the conformance deployment in a `finally` step.

## License

MIT
