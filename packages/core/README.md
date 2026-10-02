# @streamsy/core

An Effect implementation of the [Durable Streams](https://durablestreams.com)
protocol. Typed stream declarations, stream services, an in-process memory Layer, an
HTTP app, and a fetch client for remote hosts. Version 0.4.0 requires
`effect@4.0.0-rc.115`.

```sh
bun add @streamsy/core effect
```

```ts
import { Effect, Schema, Stream } from "effect";
import { Memory, Streamsy } from "@streamsy/core";

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
await Effect.runPromise(program.pipe(Effect.provide(Memory.layer(Streamsy.group(Events)))));
```

Compiled source: [packages/core/examples/readme.ts](https://github.com/gingerhendrix/streamsy/blob/effect-first-live-perimeter/packages/core/examples/readme.ts).

## What you get

- `StreamAddress` names a stream with its content type and schema, including
  `StreamAddress.state(id, { collections })` for a Durable State stream. One address
  declares several collections keyed by their wire `type`. `Streamsy` declares
  a named stream service from an id template.
- Stream services create, append, read, follow, and remove streams. Create and
  append return `_tag` variants such as `Created` / `Exists` and
  `Appended` / `Duplicate`. Protocol failures are tagged errors
  (`StreamNotFound`, `StreamGone`, `OffsetMismatch`, `AppendConflict`) for
  `Effect.catchTag`.
- `Streams.follow(address, { offset, retry })` catches up and then waits for new
  items. When a read fails with a `TransportFault` or `StorageFault`, it tries
  that read again from the same offset, so a server restart or network drop
  yields each item once. The default `retry` schedule starts at 200 ms, adds
  jitter, stops growing at 30 s and never gives up. Other failures end the
  stream. One-shot reads are not retried.
- `Producer.Position` describes a producer tuple. `Producer.next` advances its sequence after acknowledgement.
- `Streamsy` and storage layers that take a group bind each stream to its backend.

### Producer restarts

The protocol does not allocate producer epochs. If a producer cannot recompute
a payload after restart, persist the payload together with its `producerId`,
`producerEpoch`, and `producerSeq` before appending. Re-send that same tuple and
payload after a restart; `Duplicate` then means the earlier send landed.

## Entries

| Entry                     | Contents                                                                           |
| ------------------------- | ---------------------------------------------------------------------------------- |
| `@streamsy/core`          | Streamsy, Memory, StreamAddress, DurableState, Producer, storage contracts, errors |
| `@streamsy/core/fetch`    | `Fetch.layer(streams, { baseUrl })`: the same stream services over HTTP            |
| `@streamsy/core/http`     | `Http.routes`, `Http.app`, `Http.read`, and `securityHeaders`                      |
| `@streamsy/core/protocol` | StreamsReader, StreamsWriter, Streams, Protocol, StreamRoute, and raw routes       |
| `@streamsy/core/testing`  | Contract tests and fault injection for backends                                    |

## Storage and hosts

The memory Layer is process-local and not persistent. Its commit boundary copies
the whole store for each mutation, so one append costs linear time in the number
of stored messages. It is a development host; for anything that grows, use
SQLite from [`@streamsy/storage`](https://www.npmjs.com/package/@streamsy/storage),
on Bun or in a Durable Object.
Serve `Http.routes` with Effect’s `HttpRouter.serve` or `HttpRouter.toWebHandler`.
`Http.read(id, options)` reads a resolved stream id; `securityHeaders` supplies the
shared response headers. [`@streamsy/serve`](https://www.npmjs.com/package/@streamsy/serve)
provides output routes and host glue.

Browsers use the official `@durable-streams/client` and
`@durable-streams/state` packages directly against any Streamsy host.

`Memory.layer(streams, { readLimit: 1000 })` sets the server catch-up page size.
Protocol adapters can use `Protocol.layer({ readLimit })` from `@streamsy/core/protocol`. The default is
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

`Http.routes(streams, { access: "read", sseDeadlineMs })`, `Http.app`, and `Http.read` accept an SSE lifetime in
milliseconds. It defaults to 60,000 and closes the body normally at the
deadline. Each connection draws its own deadline from 90 to 110 percent of
this value, so connections opened together do not all close together. Each SSE
batch goes out as one chunk, so the body ends only after a whole batch and its
control event. Host-forced interruption and client cancellation remain owned by
the host. Read results include `contentType`; a fetch long poll returning 204
without Content-Type resolves it with HEAD.

## Row changes

`DurableState` is exported from `@streamsy/core`. Direct writers and projection
outputs use it to make row changes for `Streamsy.state` routes.

| Change | Call                                                                  |
| ------ | --------------------------------------------------------------------- |
| Upsert | `DurableState.upsert("card", row)`                                    |
| Delete | `DurableState.remove("card", key)`                                    |
| Delete | `DurableState.remove("card", key, { oldValue })` (writes `old_value`) |

`DurableState.Change<Collections>` checks the collection and row types. Each
change has a `_tag` of `"Upsert"` or `"Delete"`.
`DurableState.changes(address, { offset }, changes)` makes the wire events. With
an `offset`, each event gets that offset and a default `txid`. Projection outputs
have no source offset. The route's collection names and key fields define the
Durable State wire events.

## Documentation

- [Declare streams](https://streamsy.dev/docs/streams/declare)
- [Layers and storage](https://streamsy.dev/docs/hosting)
- [Protocol routes](https://streamsy.dev/docs/http/routes)
- [Fetch client](https://streamsy.dev/docs/http/fetch)
- [Write a storage adapter](https://streamsy.dev/docs/hosting/storage-adapter)

Source: https://github.com/gingerhendrix/streamsy

## License

MIT

An address names one stream. It holds no data. It is not an Effect `Ref`.
