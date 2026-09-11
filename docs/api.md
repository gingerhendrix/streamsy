# Streamsy 0.4.0 API

The library describes work as Effects and Streams. Applications provide a Layer
once at an executable edge and own its scope. Operations take an inert ref or id;
there are no lifetime-bearing stream handles.

## Entry points

| Entry                              | Intended surface                                                                                                                                                                       |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@streamsy/core`                   | Schema values and faults, protocol reader/writer tags, results and errors, `Protocol`, `Streams`, `StreamRef`, `Fold`, `Producer`, `Memory`, `Storage`, mutation values, `ZERO_OFFSET` |
| `@streamsy/core/storage`           | `Storage` / `StorageShape`, capabilities, mutation values and `Memory`                                                                                                                 |
| `@streamsy/core/http`              | `makeEdge(options, layer)` and `HttpOptions`                                                                                                                                           |
| `@streamsy/core/testing`           | Bun `StorageContract.run`, `faultyStorage`, `StreamsTest`, `layerTest`, `expectFailureTag`                                                                                             |
| `@streamsy/serve/bun`              | Bun `serve` host and `ServeOptions`; owns listener and HTTP edge disposal                                                                                                              |
| `@streamsy/storage`                | Driver-package-free SQLite-family `Storage` Layer, `CommitBoundary` and bounded transaction defaults                                                                                   |
| `@streamsy/storage/bun`            | Official Bun SQLite storage Layer and complete persistent `layerProtocol` composition                                                                                                  |
| `@streamsy/storage/durable-object` | Official Durable Object SQLite storage Layer and `layerProtocol`; its long-poll default is 25 seconds                                                                                  |
| `@streamsy/serve/cloudflare`       | Cloudflare Durable Object router and one-scope protocol host                                                                                                                           |

Internal offset generation, policy helpers and HTTP implementation modules have no
public subpaths. Core depends only on `effect@4.0.0-rc.112` at runtime. Its testing
entry is a Bun test registration boundary; importing the ordinary root does not
load `bun:test`. Views and serve retain their existing curated subpaths.

## Typed in-process work

```ts
import { Effect, Schema, Stream } from "effect";
import { Streams, StreamRef } from "@streamsy/core";

const events = StreamRef.json("events", { schema: Schema.Struct({ text: Schema.String }) });
const program = Effect.gen(function* () {
  yield* Streams.create(events);
  yield* Streams.append(events, [{ text: "hello" }]);
  return yield* Streams.read(events).pipe(Stream.runCollect);
});
// The application owns this runtime; reusable library functions return Effects.
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
```

`Streams.read` catches up; `follow` includes live reads; `items` flattens batches.
Missing/gone streams fail with `StreamNotFound` / `StreamGone`; codec failures use `EncodeFault`
and `DecodeFault`. A decode failure stops that read/follow; restarting from an
explicit offset is the caller's policy, with no automatic bad-item skip.
`Streams.session(ref, { offset })` returns a long-poll batch or fails with a protocol error. `Fold.run` reduces a
stream. `Producer.append` takes a producer id, epoch and sequence; `Producer.next`
advances a tuple only after an acknowledged append or duplicate.

Create succeeds with `_tag: "Created" | "Exists"`; append succeeds with
`_tag: "Appended" | "Duplicate"`. Head and read return metadata or batches directly,
remove succeeds with `void`, and `readNext` adds `cursor` and `timedOut: boolean`.
Every success that carries `closed` supplies a boolean. Read messages retain only
`data`; per-message offsets and timestamps belong to storage.

An append carries at least one message or it closes the stream. A body that frames
no message, including the JSON empty array `[]`, fails with `InvalidAppendRequest`.
A close-only append is an empty body with `close: true`, which `Streams.append`
encodes for an empty item list. The direct Layer and every HTTP transport apply
this one rule.

Protocol rejections are `Schema.TaggedError` classes in the Effect error channel.
Every error carries the stream `id`. Use `Effect.catchTag` / `Effect.catchTags` to
handle `StreamNotFound`, `StreamGone`, `StreamClosed`, `OffsetMismatch`,
`AppendConflict`, `StreamBusy`, `StaleEpoch`, `ProducerGap`, `InvalidEpochSeq`,
`InvalidAppendRequest`, `CreateConflict`, `ForkSourceNotFound`, `InvalidForkRequest`,
or `NotSupported`. The per-operation `HeadError`, `ReadError`, `ReadNextError`,
`CreateError`, `AppendError`, and `RemoveError` unions describe each service method.
Infrastructure errors remain `StorageFault` (direct) or `TransportFault` (fetch).
The separate storage boundary reports an expected rejection as `MutationRejected`
in the error channel and rolls the transaction back; [the storage
contract](storage-contract.md) states the recovery rule.

Content-type and stream-sequence conflicts use one `AppendConflict` with a message.
`CreateConflict.reason` has four direct reasons: `config-mismatch`, `soft-deleted`,
`fork-content-type`, and `fork-source-soft-deleted`. Fetch omits the reason because
standard HTTP does not encode it. A bare append 400 becomes `InvalidAppendRequest`
with its body as the message; fetch does not infer `InvalidEpochSeq` from wording.
`ForkSourceNotFound` retains the requested source, and HTTP answers it as
`404 Source stream not found: <source>`.

A duplicate proves an accepted tuple, not equality of retry payloads: owners must
retain exact bytes. Fold's journal enforces that ownership and equality on memory
and retained-file SQLite. Protocol errors propagate through Streams, Fold and
Producer; they do not advance application state as successful results.

`Stream-Fork-Sub-Offset` is an upstream fork header used with `Stream-Forked-From`
and an anchor `Stream-Fork-Offset`. It selects an additional prefix after that
anchor: a count of flattened JSON messages for JSON streams, or decoded body
bytes for text and binary streams. Zero selects no additional prefix; a positive
value requires an explicit anchor offset. Core validates the requested prefix
against the source data and includes the sub-offset in fork retry identity.

## Expected-offset concurrency

An append's `expectedOffset` checks the current tail atomically with its mutation.
A mismatch fails with `OffsetMismatch { id, expected, actual }`, without
writing messages or producer state. `ZERO_OFFSET` names an empty stream. Offsets
are canonical fixed-width opaque tokens, ordered lexicographically; do not compare
positions belonging to different streams.

HTTP maps malformed expected offsets to 400, mismatches to 409 with
`stream-next-offset`, and closed conflicts to 409 with `stream-closed: true`.
Producer validation and the existing content-type/sequence/closed checks retain
their precedence. Close-only compatibility corners, expiry parsing, caching,
batch limits and cancellation are documented in [HTTP behavior](http.md).

## Host and storage lifetime

```ts
import { Streams } from "@streamsy/core";
import * as BunHost from "@streamsy/serve/bun";

const host = await BunHost.serve({ layer: Streams.layerMemory(), port: 3000 });
// On shutdown:
await host.stop();
```

Replace the memory Layer with
`SqlStorage.layerProtocol({ client: { filename: "./streamsy.sqlite" } })` from
`@streamsy/storage/bun` for durable local protocol state. Its scoped shutdown
closes subscriptions, repair fibers and the SQL client before the same port is
rebound.

For application SQL that must share a transaction with Streamsy mutation, build
`BunStorage.layer(...)` rather than `layerProtocol`. It exposes the one official
`SqlClient`, `Reactivity`, `Storage`, and `CommitBoundary` graph. Raise a private
failure for a rejected mutation inside the boundary and recover only outside it;
the compiled example in the SQL guide checks that the application row rolls back.

Memory is process-local and nonpersistent. Its default mode provides store-wide
atomic mutations and chain forks; constrained mode provides stream atomicity,
copy forks, polling wakes and lazy expiry. The Bun host acquires its Layer lazily
and expires streams on access. See [storage contract](storage-contract.md) for the
authoring seam. Persistent Bun SQLite is shipped locally. The Cloudflare entry
routes raw stream paths to placement-selected Durable Objects and owns one Layer
scope per in-memory object. Any Worker holding the namespace binding can reach
any object; neither host adds authorization. Same-object forks chain atomically;
forks require placement that co-locates the source and child. A source in another
object is not found (404). The Durable Object `layerProtocol` long-poll default is 25 seconds (Bun is
30 seconds), while core bounds SSE connections at 60 seconds on both hosts. A
failed Cloudflare Layer build returns `503` with `retry-after: 1` and is retried
on the next request. Alarm retries are finite and platform-owned; lazy expiry
and later mutations repair missed or exhausted alarms. See the complete
[hosting reference](hosting.md) for the public host contract, local evidence,
fork placement, cancellation, and the still-pending hosted boundary.

## Derive

`@streamsy/derive` provides `Source`, `Sink`, `Projection.make`, `Projection.pass`,
`Projection.catchUp`, `Projection.follow`, and the fused `Commit` service with
`CheckpointStore` and `StateStore` contracts. Host adapters live at
`@streamsy/derive/memory` and `@streamsy/derive/sqlite`. See the
[Derive package guide](../packages/derive/README.md) for the contract and compiled example.
