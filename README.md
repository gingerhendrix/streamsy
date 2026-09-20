# Streamsy

An Effect implementation of the [Durable Streams](https://durablestreams.com)
protocol: typed stream refs, checkpointed projections, SQLite storage, and
hosts for Bun and Cloudflare.

Documentation: [streamsy.dev](https://streamsy.dev)

## Packages

| Package                                                 | What it gives you                                                         |
| ------------------------------------------------------- | ------------------------------------------------------------------------- |
| [`@streamsy/core`](packages/core/README.md)             | Protocol, typed refs, `Streams` API, memory Layer, HTTP app, fetch client |
| [`@streamsy/storage`](packages/storage/README.md)       | SQLite storage for Bun and Durable Objects                                |
| [`@streamsy/projection`](packages/projection/README.md) | Checkpointed projections over one or more streams                         |
| [`@streamsy/serve`](packages/serve/README.md)           | Bun and Cloudflare hosts, sink contracts, action delivery                 |
| [`@streamsy/views`](packages/views/README.md)           | Declarative keyed relations with incremental maintenance                  |

All packages are version `0.4.0` and require `effect@4.0.0-rc.115`.

## Quick start

```sh
bun add @streamsy/core effect
```

```ts
import { Effect, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";

const events = StreamRef.json("events", { schema: Schema.String });
const program = Effect.gen(function* () {
  yield* Streams.create(events);

  yield* Streams.append(events, ["hello"]);

  return yield* Streams.read(events).pipe(Stream.runCollect);
});
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
```

Read the [introduction](https://streamsy.dev/docs/introduction) for the path
from a typed ref to a served projection.

## Examples

- [Hacker News newest](examples/hackernews-newest-stream/README.md): a Bun
  server, a projection, and a browser client on the official Durable Streams
  client.
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
