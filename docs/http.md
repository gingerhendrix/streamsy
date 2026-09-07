# Effect HTTP and the Bun host (0.4.0)

The `@streamsy/core/http` export provides `makeEdge(options, layer)`.
It returns Effect's `{ handler, dispose }` Web conversion edge over a layer that
supplies `StreamsReader` and `StreamsWriter`. The executable owner must dispose
that edge. `@streamsy/serve/bun` provides the Bun listener and owns both resources:

```ts
import { Streams } from "@streamsy/core";
import * as BunHost from "@streamsy/serve/bun";

const host = await BunHost.serve({
  layer: Streams.layerMemory({ longPollTimeoutMs: 1500 }),
  port: 3000,
  pathPrefix: "/v1/stream",
  maxMessageSize: 1024 * 1024,
  cacheVisibility: "private",
});
// host.url is a URL; host.port is the actual bound port (port: 0 selects one).
// On shutdown:
await host.stop();
```

For durable Bun storage, compose the complete protocol Layer at the storage host
entry:

```ts
import * as SqlStorage from "@streamsy/storage/bun";
import * as BunHost from "@streamsy/serve/bun";

const host = await BunHost.serve({
  layer: SqlStorage.layerProtocol({ client: { filename: "./streamsy.sqlite" } }),
  port: 3000,
});
await host.stop();
```

That scope owns the listener, protocol services, SQL client, active changes
subscriptions and their 1,000 ms repair fibers. The Bun storage entry defaults
`busyTimeout` to zero and retries a standalone owned transaction 15 times after
its initial attempt with a fixed 25 ms yield. Fused boundary transactions remain
caller-owned and unretried; uncertain commit failures are never replayed.
The read-only format preflight, WAL preparation and migrations use the same
bounded yielding policy during scoped Layer acquisition.

The host defaults to `127.0.0.1:3000`; set `hostname` explicitly for another bind
address. The default prefix is `/`. Bun's idle timeout is disabled so the
protocol owns long-poll and SSE deadlines. `stop()` force-closes connections,
then disposes the Effect layer, and is idempotent. The layer is acquired lazily
on the first request by Effect's Web edge. No platform-bun package is needed.

The conversion edge explicitly makes application work interruptible: rc.112's
`HttpEffect.toHandled` masks interruption around the handled request. Bun supplies
the request abort event and HttpEffect interrupts its fiber; the explicit inner
`Effect.interruptible` lets parked long-poll work observe it. HttpEffect still owns
response delivery and request-scope finalization. No host dependency is added.

## Wire behavior

PUT, POST, GET, HEAD, DELETE and OPTIONS retain the old handler's status,
response text, security, cache and protocol header conventions. Static responses
use `HttpServerResponse.raw` to preserve Web body defaults; SSE uses an Effect
byte stream. The original Web request URL supplies the create `Location` header.
HEAD has no body at the framework edge, including errors, as on the old Bun wire.

`Stream-Expires-At` uses Effect rc.112 `DateTime.make` for validation and retains
the original accepted string for storage and HEAD. Uppercase ISO UTC
(`2028-01-01T00:00:00Z`), ISO offsets and no-zone ISO remain accepted. Lowercase ISO
(`2028-01-01t00:00:00z`) and RFC UTC (`Sat, 01 Jan 2028 00:00:00 UTC`) intentionally
return `400 Invalid Stream-Expires-At format`, where the old Date parser returned 201. A rejected create leaves no stream (HEAD 404). This accepted format narrowing
keeps the Effect parser; invalid dates and numeric millisecond text remain rejected.

Read `batch_size` is converted with JavaScript `Number` and must yield an integer
from 1 through 10,000. Exponent (`1e0`), hexadecimal (`0x1`), leading zero (`01`)
and whitespace-padded (`%201%20` in the query) forms are accepted for compatibility. Invalid values receive
400; it limits catch-up reads only. `readNext`, long-poll, SSE and toolkit follow
return all currently available stored messages after the offset, so a large
burst can occupy a large response and framing buffer. Backpressure does not bound
that batch's size. Byte toolkit append joins supplied items into one stored
message; reads return one item per stored message.

Cursors at HTTP ingress must be canonical nonnegative decimal safe integers,
with room for the generator's maximum jitter (180). Malformed or overflowing
values receive `400 Invalid cursor`. This deliberately tightens the old HTTP
parser, which forwarded arbitrary text to a lenient `parseInt` cursor generator.
Producer epochs/sequences preserve the old canonical nonnegative safe-integer
validation. Direct protocol cursor parsing and zero-limit behavior are unchanged:
a direct zero-limit read returns no messages and reports the current tail.

Empty close-only appends preserve established semantics. On an open stream they
bypass content-type and Stream-Seq conflict checks and may replace a prior
sequence with a lower supplied value. On an already closed stream, producer
validation still occurs first, but a fresh accepted tuple is acknowledged without
persisting that tuple or returning producer headers. A subsequent seq=1 for that
unpersisted producer therefore gets a producer-gap response. These are retained
compatibility corners, not new producer guarantees.

`StorageFault` before response transmission becomes `500 Internal server error`,
including retryable faults. HTTP does not retry opaque writes. The protocol's
bounded semantic CAS rejection loop remains distinct. Once an SSE response has
started, a later fault fails the body; it cannot change an already sent status.

## Ownership and limits

SSE reads are scoped Effect streams. Plain long-poll client cancellation also
interrupts its pending read before the protocol timeout. Client cancellation and host shutdown
interrupt pending reads and release changes subscriptions. Each connection ends
after 60 seconds, at closure, or when the stream becomes unavailable. The deadline
interrupts even a pending long poll. Memory notifications cost O(active
subscriptions) per publication with one retained wake per subscription; no host
throughput or memory budget is established by these functional tests.

Request bodies retain the old read-then-size-check policy; `maxMessageSize` is an
acceptance limit, not a streaming allocation bound. The underlying Bun request
body cap also applies. All request-body read failures map to 413 as in the old
handler, even when the failure was a body I/O error rather than an oversized
payload; this response alone does not identify the underlying cause.
Storage expiry remains lazy-on-access in both memory and SQLite hosts; there is no
background expiry sweeper or expiry fiber in this host.

`bun run test:conformance` executes the unchanged bundled official suite once on
memory and once on retained-file Bun SQLite with the approved Vitest-under-Bun
runner. Each backend passes 332 tests with six declared skips. Every authored test uses `bun:test`.
The old graph and its runner exceptions have been removed. Frozen response
fixtures preserve status, status text, every header and body byte after removal
of the comparison implementation. Hosted Durable Object protocol, browser Effect
transport and release support are not added; retained-file Bun SQLite is the local
persistent host described above.
