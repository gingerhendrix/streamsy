# Streamsy

Streamsy is a work-in-progress Durable Streams server implementation. It is currently focused on implementing the `@durable-streams/server-conformance-tests` protocol target for both in-memory and Cloudflare Durable Object storage backends.

## Status

WIP. The current local implementation has been validated against `@durable-streams/server-conformance-tests@0.3.0` with both storage backends passing the full conformance suite:

- `@streamsy/storage-memory`: 299/299 passing
- `@streamsy/storage-durable-object` (Cloudflare Durable Object storage): 299/299 passing

The API and package boundaries are not yet stable, and all packages remain private while the project is being shaped.

## Packages

- `@streamsy/core` (`packages/core`) — public protocol and HTTP facades, plus shared protocol/storage types.
- `@streamsy/storage-memory` (`packages/storage-memory`) — in-memory storage adapter used for local development, examples, and conformance testing.
- `@streamsy/storage-durable-object` (`packages/storage-durable-object`) — Cloudflare Durable Object storage adapter with SQLite-backed persistence, long-polling, SSE support, TTL/expiry handling, and stream metadata.
- `@streamsy/conformance-tests` (`packages/conformance-tests`) — private conformance harness for the memory and Durable Object adapters.

## Public API

Streamsy applications should usually compose:

1. a `StreamStoreAdapter` from a storage package;
2. a protocol facade from `createStreamProtocol(store)`;
3. an HTTP facade from `createHttpHandler({ protocol, pathPrefix })` when serving the Durable Streams HTTP protocol.

```ts
import { createHttpHandler, createStreamProtocol } from "@streamsy/core";
import { createMemoryStreamStore } from "@streamsy/storage-memory";

const store = createMemoryStreamStore();
const protocol = createStreamProtocol(store);
const handler = createHttpHandler({ protocol, pathPrefix: "/" });

Bun.serve({ fetch: (request) => handler.fetch(request) });
```

See [`docs/api.md`](docs/api.md) for package boundaries, exported types, storage adapter semantics, and what remains internal.

## Examples

- `examples/memory-server` — Bun/HTTP server using the memory storage backend and public API factories.

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
