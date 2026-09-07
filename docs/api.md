# Streamsy 0.4.0 API

The library describes work as Effects and Streams. Applications provide a Layer
once at an executable edge and own its scope. Operations take an inert ref or id;
there are no lifetime-bearing stream handles.

## Entry points

| Entry                              | Intended surface                                                                                                                                                                |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@streamsy/core`                   | Schema values and faults, protocol reader/writer tags and outcomes, `Protocol`, `Streams`, `StreamRef`, `Fold`, `Producer`, `Memory`, `Storage`, mutation values, `ZERO_OFFSET` |
| `@streamsy/core/storage`           | `Storage` / `StorageShape`, capabilities, mutation values and `Memory`                                                                                                          |
| `@streamsy/core/http`              | `makeEdge(options, layer)` and `HttpOptions`                                                                                                                                    |
| `@streamsy/core/testing`           | Bun `StorageContract.run`, `faultyStorage`, `StreamsTest`, `layerTest`                                                                                                          |
| `@streamsy/serve/bun`              | Bun `serve` host and `ServeOptions`; owns listener and HTTP edge disposal                                                                                                       |
| `@streamsy/storage`                | Driver-package-free SQLite-family `Storage` Layer, `CommitBoundary` and bounded transaction defaults                                                                            |
| `@streamsy/storage/bun`            | Official Bun SQLite storage Layer and complete persistent `layerProtocol` composition                                                                                           |
| `@streamsy/storage/durable-object` | Official Durable Object SQLite storage Layer and `layerProtocol`; its long-poll default is 25 seconds                                                                           |
| `@streamsy/serve/cloudflare`       | Cloudflare Durable Object router and one-scope protocol host                                                                                                                    |

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
  const result = yield* Streams.append(events, [{ text: "hello" }]);
  if (result.status !== "appended") return result;
  return yield* Streams.read(events).pipe(Stream.runCollect);
});
// The application owns this runtime; reusable library functions return Effects.
await Effect.runPromise(program.pipe(Effect.provide(Streams.layerMemory())));
```

`Streams.read` catches up; `follow` includes live reads; `items` flattens batches.
Missing/gone streams fail with `StreamUnavailable`; codec failures use `EncodeFault`
and `DecodeFault`. A decode failure stops that read/follow; restarting from an
explicit offset is the caller's policy, with no automatic bad-item skip.
`Streams.session(ref)` exposes protocol classifications. `Fold.run` reduces a
stream. `Producer.append` takes a producer id, epoch and sequence; `Producer.next`
advances a tuple only after an acknowledged append or duplicate.

Protocol outcomes are success values such as `created`, `exists`, `appended`,
`duplicate`, `conflict`, `not-found`, `gone`, `timeout`, `stale-epoch` and
`producer-gap`. Storage failures use the typed error channel. A duplicate proves
an accepted tuple, not equality of retry payloads: owners must retain exact bytes.
Fold's journal enforces that ownership and equality on memory and retained-file SQLite.

## Expected-offset concurrency

An append's `expectedOffset` checks the current tail atomically with its mutation.
A mismatch returns `conflict` / `expected-offset` with the actual offset, without
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
authoring seam. Persistent Bun SQLite is shipped locally. The Cloudflare host
routes raw stream paths to placement-selected Durable Objects. Any Worker holding
the namespace binding can reach any object; the host adds no authorization.
Same-object forks chain atomically. Cross-object forks copy the selected prefix into the child,
bounded by `copyOnForkMaxBytes` (8 MiB of encoded frames by default), keep provenance for
idempotent retries, and leave no retention edge on the source, which may change or be deleted
afterwards. Its DO `layerProtocol`
default bounds long-poll reads at 25 seconds (Bun remains 30 seconds), while core
bounds SSE connections at 60 seconds on both hosts. A failed Layer build returns
`503` with `retry-after: 1` and is rebuilt on the next call. Alarm retries are
platform-owned and finite; lazy expiry on reads and reconciliation after the next
mutating request repair an alarm that is exhausted or missed. With pinned local
workerd, a client disconnect does not interrupt the object read, so the protocol
bounds are the local release guarantee.
