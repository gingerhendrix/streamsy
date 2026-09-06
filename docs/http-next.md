# Effect HTTP and the Bun host (Step 1 Batch 4)

The private `@streamsy/core-next/http` export provides `makeEdge(options, layer)`.
It returns Effect's `{ handler, dispose }` Web conversion edge over a layer that
supplies `StreamsReader` and `StreamsWriter`. The executable owner must dispose
that edge. `@streamsy/serve/bun` provides the Bun listener and owns both resources:

```ts
import { Streams } from "@streamsy/core-next";
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

The host defaults to `127.0.0.1:3000`; set `hostname` explicitly for another bind
address. The default prefix is `/`. Bun's idle timeout is disabled so the
protocol owns long-poll and SSE deadlines. `stop()` force-closes connections,
then disposes the Effect layer, and is idempotent. The layer is acquired lazily
on the first request by Effect's Web edge. No platform-bun package is needed.

## Wire behavior

PUT, POST, GET, HEAD, DELETE and OPTIONS retain the old handler's status,
response text, security, cache and protocol header conventions. Static responses
use `HttpServerResponse.raw` to preserve Web body defaults; SSE uses an Effect
byte stream. The original Web request URL supplies the create `Location` header.
HEAD has no body at the framework edge, including errors, as on the old Bun wire.

Read `batch_size` is an integer from 1 through 10,000. Invalid values receive
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

SSE reads are scoped Effect streams. Client cancellation and host shutdown
interrupt pending reads and release changes subscriptions. Each connection ends
after 60 seconds, at closure, or when the stream becomes unavailable. The deadline
interrupts even a pending long poll. Memory notifications cost O(active
subscriptions) per publication with one retained wake per subscription; no host
throughput or memory budget is established by these functional tests.

Request bodies retain the old read-then-size-check policy; `maxMessageSize` is an
acceptance limit, not a streaming allocation bound. The underlying Bun request
body cap also applies. Storage expiry remains lazy-on-access in this memory host;
this batch adds no background expiry sweeper.

The temporary core-next alias and old host remain for Batch 5. The new command is
`bun run test:conformance:memory-next`. It executes the unchanged bundled official
suite with Vitest under Bun, while newly authored tests and the local deployment
state unit tests use `bun:test`. The approved Vitest dependency is development-only
in conformance-tests; staged old-graph runner exceptions remain until their
planned removal. No hosted, SQL, browser transport or release support is added.
