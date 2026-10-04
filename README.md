# Streamsy

An Effect implementation of the [Durable Streams](https://durablestreams.com)
protocol: typed stream refs, checkpointed projections, SQLite storage, and
hosts for Bun and Cloudflare.

Documentation: [streamsy.dev](https://streamsy.dev)

## Packages

| Package                                                 | What it gives you                                                            |
| ------------------------------------------------------- | ---------------------------------------------------------------------------- |
| [`@streamsy/core`](packages/core/README.md)             | Protocol, typed refs, stream services, memory Layer, HTTP app, remote client |
| [`@streamsy/storage`](packages/storage/README.md)       | SQLite storage for Bun and Durable Objects                                   |
| [`@streamsy/projection`](packages/projection/README.md) | Checkpointed projections with named stream, rows and value outputs           |
| [`@streamsy/serve`](packages/serve/README.md)           | HttpRouter routes, Bun and Cloudflare hosts, action delivery                 |

All packages are version `0.4.0` and require `effect@4.0.0-rc.115`.

## Quick start

```sh
bun add @streamsy/core effect
```

```ts
import { Effect, Schema, Stream } from "effect";
import { MemoryStorage, Streamsy } from "@streamsy/core";

class Events extends Streamsy.json<Events>()("Readme/Events", "events", {
  params: {},
  schema: Schema.String,
}) {}
const program = Effect.gen(function* () {
  const events = yield* Events;
  yield* events.create({});
  yield* events.append({}, ["hello"]);
  return yield* events.read({}).pipe(Stream.runCollect);
});
await Effect.runPromise(program.pipe(Effect.provide(MemoryStorage.layer(Streamsy.group(Events)))));
```

Read the [tutorial](https://streamsy.dev/docs/tutorial) for the path
from a typed stream address to a served projection.

## Examples

- [Memory server](examples/memory-server/README.md): the smallest Bun host,
  backed by one process-local memory store and exercised with `curl`.
- [Issue tracker](examples/issue-tracker-demo/README.md): a complete application
  with conditional writes, SQLite persistence, and live browser collections.
- [Hacker News newest](examples/hackernews-newest-stream/README.md): a Bun
  server, a projection, and a browser client on the official Durable Streams
  client.
- [Issue tracker projection](examples/issue-tracker/README.md): five workspace
  inputs in two checkpointed SQLite projections, with output streams and HTTP
  routes.
- [Hex board](examples/hex-board/README.md): a whole game board kept as fold
  state in the checkpoint transaction, with SQLite restart tests.
- [Fold agent](examples/fold-agent/README.md): an agent loop whose durable log
  lives in Streamsy and fences competing writers by offset.

## Development

```sh
bun install
bun run typecheck
bun run lint
bun run test:unit
bun run test:conformance
```

## License

MIT
