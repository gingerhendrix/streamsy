# @streamsy/core

An Effect implementation of the [Durable Streams](https://durablestreams.com)
protocol. Typed stream refs, a `Streams` API, an in-process memory Layer, an
HTTP app, and a fetch client for remote hosts. Version 0.4.0 requires
`effect@4.0.0-rc.115`.

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

Compiled source: [packages/core/examples/readme.ts](https://github.com/gingerhendrix/streamsy/blob/effect-first-live-perimeter/packages/core/examples/readme.ts).

## What you get

- `StreamRef` names a stream with its content type and schema, including
  `StreamRef.state(id, { collections })` for a Durable State stream. One ref
  declares several collections keyed by their wire `type`. `StreamRoute` names
  a family of streams from an id template.
- `Streams` creates, appends, reads, follows, and removes streams. Create and
  append return `_tag` variants such as `Created` / `Exists` and
  `Appended` / `Duplicate`. Protocol failures are tagged errors
  (`StreamNotFound`, `StreamGone`, `OffsetMismatch`, `AppendConflict`) for
  `Effect.catchTag`.
- `Producer` appends under a producer tuple so retries are safe.
- `Fold` reduces a stream into a value.
- `Backend` and `Streams.layerRouted` select a backend per stream id.

### Producer restarts

The protocol does not allocate producer epochs. If a producer cannot recompute
a payload after restart, persist the payload together with its `producerId`,
`producerEpoch`, and `producerSeq` before appending. Re-send that same tuple and
payload after a restart; `Duplicate` then means the earlier send landed.

## Entries

| Entry                    | Contents                                                                            |
| ------------------------ | ----------------------------------------------------------------------------------- |
| `@streamsy/core`         | Streams, StreamRef, StreamRoute, Backend, Fold, Producer, storage contracts, errors |
| `@streamsy/core/fetch`   | `Fetch.layer({ baseUrl })`: the same reader and writer over HTTP                    |
| `@streamsy/core/http`    | `Http.app` and `makeEdge` to serve the protocol                                     |
| `@streamsy/core/testing` | Contract tests and fault injection for backends                                     |

## Storage and hosts

The memory Layer is process-local and not persistent. Its commit boundary copies
the whole store for each mutation, so one append costs linear time in the number
of stored messages. It is a development host; for anything that grows, use
SQLite from [`@streamsy/storage`](https://www.npmjs.com/package/@streamsy/storage),
on Bun or in a Durable Object.
To serve the protocol, use [`@streamsy/serve`](https://www.npmjs.com/package/@streamsy/serve).

Browsers use the official `@durable-streams/client` and
`@durable-streams/state` packages directly against any Streamsy host.

`Protocol.layer({ readLimit: 1000 })` sets the server catch-up page size.
`Streams.layerMemory({ readLimit })` accepts the same option. The default is
1000 messages; `readNext` returns the whole available tail. Fetch clients send
only an offset and accept the remote server’s page size. Both direct read
methods reject malformed offsets with `InvalidReadRequest`; accepted offsets
are canonical tokens, `-1`, and `now` (or an omitted catch-up offset).

Producer options and `Producer.Position` use
`{ producerId, producerEpoch, producerSeq }`. Append results carry
`producerEpoch` and `producerSeq` when the acknowledgement includes producer
state. A close-only producer write uses HTTP 204 and decodes as `Duplicate`
when that state is present. A close on an already closed stream with a fresh
tuple returns `Appended` without producer state, including retries of that
unpersisted tuple.

`Http.app({ sseDeadlineMs })` and `Http.makeEdge` accept an SSE lifetime in
milliseconds. It defaults to 60,000 and closes the body normally at the
deadline. Host-forced interruption and client cancellation remain owned by
the host. Read results include `contentType`; a fetch long poll returning 204
without Content-Type resolves it with HEAD.

## Documentation

- [Streams](https://streamsy.dev/docs/streams)
- [Runtime and storage](https://streamsy.dev/docs/runtime)
- [HTTP and the fetch Layer](https://streamsy.dev/docs/runtime/http)
- [Writing a storage adapter](https://streamsy.dev/docs/advanced/storage-adapter)

Source: https://github.com/gingerhendrix/streamsy

## License

MIT
