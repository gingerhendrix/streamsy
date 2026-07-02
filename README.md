# Streamsy

Streamsy is a work-in-progress Durable Streams server implementation. It is currently focused on implementing the `@durable-streams/server-conformance-tests` protocol target for both in-memory and Cloudflare Durable Object storage backends.

## Status

WIP. The current local implementation has been validated against `@durable-streams/server-conformance-tests@0.3.5` with all storage backends passing the full conformance suite:

- in-memory storage (bundled with `@streamsy/core`): 326/326 passing
- `@streamsy/storage-sqlite` (Bun `bun:sqlite` storage): 326/326 passing
- `@streamsy/storage-durable-object` (Cloudflare Durable Object storage): 326/326 passing

The API and package boundaries are not yet stable. The public packages are prepared for npm publishing under the `@streamsy/*` scope; the conformance harness and examples remain private.

## Packages

- `@streamsy/core` (`packages/core`) — public protocol and HTTP facades, shared protocol/storage types, and the in-memory storage adapter (`createMemoryStorageAdapter`) used for local development, examples, and conformance testing.
- `@streamsy/json` (`packages/json`) — typed JSON protocol and stream wrappers (`JsonProtocol<T>`/`JsonStream<T>`) that encode/decode `application/json` messages through a codec or Standard Schema.
- `@streamsy/state` (`packages/state`) — Durable State protocol and stream wrappers (`DurableStateProtocol<S>`/`DurableStateStream<S>`) for typed change/control messages over collections.
- `@streamsy/storage-sqlite` (`packages/storage-sqlite`) — Bun SQLite (`bun:sqlite`) storage adapter providing durable local persistence, automatic migrations, in-process mutation locking, live-read notification, and lazy/in-process expiry. Requires the Bun runtime.
- `@streamsy/storage-durable-object` (`packages/storage-durable-object`) — Cloudflare Durable Object storage adapter with SQLite-backed persistence, long-polling, SSE support, TTL/expiry handling, and stream metadata.
- `@streamsy/conformance-tests` (`packages/conformance-tests`) — private conformance harness for the memory, SQLite, and Durable Object adapters.

## Public API

Streamsy applications should usually compose:

1. a `StorageAdapter` from a storage package (or the in-memory adapter from `@streamsy/core`);
2. a protocol facade from `createStreamProtocol({ storage: { adapter } })`;
3. an HTTP facade from `createHttpHandler({ protocol, pathPrefix })` when serving the Durable Streams HTTP protocol.

```ts
import {
  createHttpHandler,
  createMemoryStorageAdapter,
  createStreamProtocol,
} from "@streamsy/core";

const adapter = createMemoryStorageAdapter();
const protocol = createStreamProtocol({ storage: { adapter } });
const handler = createHttpHandler({ protocol, pathPrefix: "/" });

Bun.serve({ fetch: (request) => handler.fetch(request) });
```

### Typed layers

`@streamsy/json` and `@streamsy/state` wrap a `StreamProtocolFactory` with typed facades:

```ts
import { createJsonProtocol } from "@streamsy/json";
import { createDurableStateProtocol } from "@streamsy/state";

// Typed JSON streams: values validated through a JsonCodec or Standard Schema.
const json = createJsonProtocol(protocol, userCodec);
const created = await json.create("users", { initialMessage: { id: "u1", name: "Alice" } });

// Durable State streams: typed change/control messages over collections.
const durable = createDurableStateProtocol(protocol, {
  users: { type: "user", schema: userCodec, primaryKey: "id" },
});
```

See [`docs/api.md`](docs/api.md) for package boundaries, exported types, storage adapter semantics, and what remains internal.

## Examples

- `examples/memory-server` — Bun/HTTP server using the memory storage backend and public API factories.
- `examples/issue-tracker-demo` — React/Vite issue tracker using a Bun API server, Streamsy memory storage, and TanStack DB client collections to demonstrate a small durable-sync app.
- `examples/hackernews-newest-stream` — Hacker News "newest" feed streamed through Streamsy with a materializer/projection pattern and a TanStack DB client.

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
