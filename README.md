# Streamsy

Streamsy is a work-in-progress Durable Streams server implementation. It is currently focused on implementing the `@durable-streams/server-conformance-tests` protocol target for both in-memory and Cloudflare Durable Object storage backends.

## Status

WIP. The current local implementation has been validated against `@durable-streams/server-conformance-tests@0.3.0` with both storage backends passing the full conformance suite:

- `@streamsy/storage-memory`: 299/299 passing
- `@streamsy/storage-sqlite` (Bun `bun:sqlite` storage): 299/299 passing
- `@streamsy/storage-durable-object` (Cloudflare Durable Object storage): 299/299 passing

The API and package boundaries are not yet stable. The public packages are prepared for npm publishing under the `@streamsy/*` scope; the conformance harness and examples remain private.

## Packages

- `@streamsy/core` (`packages/core`) — public protocol and HTTP facades, plus shared protocol/storage types.
- `@streamsy/storage-memory` (`packages/storage-memory`) — in-memory storage adapter used for local development, examples, and conformance testing.
- `@streamsy/storage-sqlite` (`packages/storage-sqlite`) — Bun SQLite (`bun:sqlite`) storage adapter providing durable local persistence, automatic migrations, in-process mutation locking, live-read notification, and lazy/in-process expiry. Requires the Bun runtime.
- `@streamsy/storage-durable-object` (`packages/storage-durable-object`) — Cloudflare Durable Object storage adapter with SQLite-backed persistence, long-polling, SSE support, TTL/expiry handling, and stream metadata.
- `@streamsy/conformance-tests` (`packages/conformance-tests`) — private conformance harness for the memory, SQLite, and Durable Object adapters.

## Public API

Streamsy applications should usually compose:

1. a `StreamFactory` from a storage package;
2. a protocol facade from `createStreamProtocol({ storage: { factory } })`;
3. an HTTP facade from `createHttpHandler({ protocol, pathPrefix })` when serving the Durable Streams HTTP protocol.

```ts
import { createHttpHandler, createStreamProtocol } from "@streamsy/core";
import { createMemoryStreamFactory } from "@streamsy/storage-memory";

const factory = createMemoryStreamFactory();
const protocol = createStreamProtocol({ storage: { factory } });
const handler = createHttpHandler({ protocol, pathPrefix: "/" });

Bun.serve({ fetch: (request) => handler.fetch(request) });
```

See [`docs/api.md`](docs/api.md) for package boundaries, exported types, storage adapter semantics, and what remains internal.

## Examples

- `examples/memory-server` — Bun/HTTP server using the memory storage backend and public API factories.
- `examples/issue-tracker-demo` — React/Vite issue tracker using a Bun API server, Streamsy memory storage, and TanStack DB client collections to demonstrate a small durable-sync app.

## Development

```bash
bun install
bun run build
bun run typecheck
bun run lint
bun run format:check
bun run test:unit
bun run test:conformance:memory
bun run test:conformance:sqlite
```

Durable Object conformance uses an Alchemy-managed Cloudflare deployment and can take longer than local checks:

```bash
bun run test:conformance:do
```

`test:conformance:do` deploys the `packages/conformance-tests` worker with `STAGE=conformance`, runs the Durable Object conformance tests against the deployed workers.dev URL, and destroys the conformance deployment in a `finally` step.

## Release

See [`docs/release.md`](docs/release.md) for the npm release runbook, including first manual publish steps and GitHub Actions trusted publishing setup.

## License

MIT
