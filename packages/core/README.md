# @streamsy/core

An Effect implementation of the [Durable Streams](https://durablestreams.com)
protocol. Typed stream declarations, stream services, an in-process memory Layer, an
HTTP app, and a client for remote hosts. Requires
`effect@4.0.5`.

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

Compiled source: [packages/core/examples/readme.ts](https://github.com/gingerhendrix/streamsy/blob/effect-first-live-perimeter/packages/core/examples/readme.ts).

## What you get

- `StreamAddress` names a stream with its content type and schema, including
  `StreamAddress.state(id, { collections })` for a Durable State stream. One address
  declares several collections keyed by their wire `type`. Its items are the
  changes of those collections and the State control messages `snapshot-start`,
  `snapshot-end` and `reset`. `Streamsy` declares a named stream service from an
  id template.
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

| Entry                     | Contents                                                                                  |
| ------------------------- | ----------------------------------------------------------------------------------------- |
| `@streamsy/core`          | Streamsy, MemoryStorage, StreamAddress, DurableState, Producer, storage contracts, errors |
| `@streamsy/core/remote`   | `RemoteStreams.layer(streams, { baseUrl })`: the same stream services over HTTP           |
| `@streamsy/core/http`     | `Http.routes`, `Http.app`, `Http.read`, and `securityHeaders`                             |
| `@streamsy/core/protocol` | StreamsReader, StreamsWriter, Streams, Protocol, StreamRoute, and raw routes              |
| `@streamsy/core/testing`  | Vitest contract tests and fault injection for backends                                    |

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

`MemoryStorage.layer(streams, { readLimit: 1000 })` sets the server catch-up page size.
Protocol adapters can use `Protocol.layer({ readLimit })` from `@streamsy/core/protocol`. The default is
1000 messages; `readNext` returns the whole available tail. Remote clients send
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
the host. Read results include `contentType`; a remote long poll returning 204
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

## Control messages

A State stream can also hold the control messages of the Durable State
protocol. A control has `headers.control` and an optional `headers.offset`. It
has no `type` or `key`.

| Control          | Make it with                    | A reader should                                               |
| ---------------- | ------------------------------- | ------------------------------------------------------------- |
| `reset`          | `StreamAddress.reset()`         | Clear its rows, then apply the items after it                 |
| `snapshot-start` | `StreamAddress.snapshotStart()` | Treat the changes up to `snapshot-end` as a complete snapshot |
| `snapshot-end`   | `StreamAddress.snapshotEnd()`   | Close the snapshot                                            |

Each constructor takes `{ offset }`. Items of a State address have the type
`StreamAddress.CollectionsEvent<Collections>`. Narrow an item with
`StreamAddress.isChange` or `StreamAddress.isControl` before you read `type`,
`key` or `headers.operation`:

```ts
for (const item of batch.items) {
  if (StreamAddress.isControl(item)) {
    if (item.headers.control === "reset") rows.clear();
  } else if ("value" in item) rows.set(item.key, item.value);
  else rows.delete(item.key);
}
```

`DurableState.changes` and projection outputs accept a control beside row
changes and write it unchanged. With an `offset`, a control that has no offset
gets that offset. `Serve.state` serves stored controls as they are. It still
refuses the `x-streamsy-state-reset: snapshot` request header, because the
server does not make snapshots.

## Materialize State

`DurableState.materialize(state, events)` folds State items into one map of rows
for each collection. Start from `DurableState.materialized(address)`. It applies
the State rules: an upsert sets a row, a delete removes it, `reset` clears every
collection, and `snapshot-end` removes each row that the snapshot did not name. A
snapshot can span several calls. The input state does not change.

```ts
let state = DurableState.materialized(Board.at({ id }));

for (const batch of batches) state = DurableState.materialize(state, batch.items);

state.rows.card; // ReadonlyMap<string, Card>
```

## Wake streams

A wake stream indexes the writes of a family of streams. It is a State stream
with one row for each source stream: `{ stream, offset, closed? }`, keyed by the
source id. The row holds the source tail after its last write. A client follows
one wake stream and reads only the source streams that moved past its saved
cursors.

```ts
class Files extends Streamsy.state<Files>()("App/Files", "folders/:folderId/files", {
  params: { folderId: FolderKey },
  collections: { file: { schema: FileValue, key: "path" } },
}) {}

class UserWake extends Streamsy.wake<UserWake>()("App/UserWake", "users/:user/wake", {
  params: { user: UserKey },
}) {}

const streams = Streamsy.group(Files, UserWake);
```

A wake declaration names no sources. A projection writes its rows:
`Projection.wake` from `@streamsy/projection` links one source stream to one
wake stream, with params of its own. The source and the wake stream can live on
different storage owners. See the projection README.

`Streamsy.wake` is a preset for `Streamsy.state` with `wakeCollections`.
It has the normal State service, including `create`, `append`, `remove`,
`read`, `follow`, `fold` and `collect`. Use `DurableState.materialize` to fold
its events into the latest rows.

`Projection.wake` opts its output into creation on the first append, including
on another owner. The owner can call the normal State service's `create` to
make an empty stream available before that append. A repeated create answers
`Exists`:

```ts
const start = Effect.gen(function* () {
  const wake = yield* UserWake;
  yield* wake.create({ user });
});
```

HTTP access is mount policy. Choose read access for projection outputs on a
public mount and full access on an internal mount where producers create and
append them:

```ts
Http.routes(streams, {
  prefix: "/streams",
  access: (stream) => (stream === UserWake ? "read" : "full"),
});
Http.routes(Streamsy.group(UserWake), { prefix: "/__wake", access: "full" });
```

`Streamsy.followWake` turns wake batches into actions:

```ts
const actions = Streamsy.followWake(
  UserWake,
  { user },
  {
    from: saved.wake,
    cursors: saved.cursors,
  },
);
// Each batch: { items: WakeItem[], offset }. An item is Dirty, Removed, Reset or Snapshot.
// Save each source cursor first, then save `offset` as the wake cursor.
```

A client with its own HTTP reader does not need a `RemoteStreams` stack.
It decodes each page with `Streamsy.WakeMessage` and calls the pure
`Streamsy.wakeActions`, which `followWake` also uses:

```ts
const decodePage = Schema.decodeUnknownEffect(Schema.Array(Streamsy.WakeMessage));

const handle = (page: unknown, snapshot?: ReadonlySet<StreamId>) =>
  Effect.map(decodePage(page), (events) =>
    Streamsy.wakeActions(events, { cursors: saved.cursors, snapshot }),
  );
// { items, snapshot }: items as above. Pass snapshot to the call for the next page.
```

A wake row lags its source by one projection pass, on the same owner and on
another one. The client reads each source to its tail, so the lag costs
latency only.

A wake row relies on append-only streams. The `offset` of a source only goes up,
so a client compares it with its saved cursor. Never reuse a source id: do not
create a stream again under the id of a removed or expired stream. The new
stream starts at a lower offset, and a client with a saved cursor skips its
items. The projection also treats a gone source as removed for good.

## Local commit events

`Storage.committed` is a scoped
`Effect<PubSub.Subscription<ReadonlyArray<StreamId>>>`. The subscription exists
when the effect returns, so a later commit always lands in it. Memory and SQL
storage publish the ids touched by each successful local commit after it
commits. Rolled-back writes publish nothing. Each subscriber gets its own
subscription, which its scope releases. Events are best effort and have no replay; commits from
another process do not appear. `Storage.changes(id)` remains the per-stream
read notification source. Custom storage adapters must provide `committed`.

## Documentation

- [Declare streams](https://streamsy.dev/docs/streams/declare)
- [Layers and storage](https://streamsy.dev/docs/hosting)
- [Protocol routes](https://streamsy.dev/docs/http/routes)
- [Remote client](https://streamsy.dev/docs/http/remote)
- [Write a storage adapter](https://streamsy.dev/docs/hosting/storage-adapter)

Source: https://github.com/gingerhendrix/streamsy

## License

MIT

An address names one stream. It holds no data. It is not an Effect `Ref`.
