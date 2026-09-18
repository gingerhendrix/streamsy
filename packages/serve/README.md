# @streamsy/serve

Hosts and HTTP contracts for Streamsy. Serve the Durable Streams protocol on
Bun or Cloudflare, publish relations and documents through checked sink
contracts, and deliver external effects through a durable outbox. Version
0.4.0 requires `effect@4.0.0-rc.115`.

```sh
bun add @streamsy/serve @streamsy/core effect
```

## Serve the protocol

On Bun, `start` owns one listener and one HTTP edge and returns a `stop`
Effect. `layer` is the same host as a Layer for `Layer.launch`. This entry
needs the optional peer `@effect/platform-bun@4.0.0-rc.115`.

```ts
import { Effect } from "effect";
import { Streams } from "@streamsy/core";
import { start } from "@streamsy/serve/bun";

const host = await Effect.runPromise(start({ layer: Streams.layerMemory(), port: 3000 }));
await Effect.runPromise(host.stop);
```

On Cloudflare, a Worker routes each stream path to a Durable Object by
placement, and the object owns the protocol and its SQLite storage.
`@streamsy/serve/cloudflare` gives you `StreamsyObject.make` and `Placement`;
`@streamsy/serve/alchemy` gives the same handlers as Effects for an Alchemy
stack (optional peer `alchemy@2.0.0-beta.76`). Forks require the source and
child to share one object, so use `Placement.byKey` when you fork.

Guides: [HTTP](https://streamsy.dev/docs/runtime/http),
[Cloudflare](https://streamsy.dev/docs/runtime/cloudflare),
[Alchemy](https://streamsy.dev/docs/runtime/alchemy).

## Sinks

A sink is an inert, checked contract for publishing over HTTP. Declare it
once; a server handler serves it and a browser can decode it without loading
Effect.

| Family   | Publishes                                                   | Declare with         | Serve with           |
| -------- | ----------------------------------------------------------- | -------------------- | -------------------- |
| state    | a keyed collection as durable state                         | `defineStateSink`    | `handleStateSink`    |
| stream   | the change stream of a relation                             | `defineStreamSink`   | `handleStreamSink`   |
| document | one derived value per route                                 | `defineDocumentSink` | `handleDocumentSink` |
| action   | nothing; delivers an effect at least once through an outbox | `defineActionSink`   | the action runtime   |

Guides: [Sinks](https://streamsy.dev/docs/projections/sinks),
[Action sink](https://streamsy.dev/docs/projections/action-sink).

## Entries

| Entry                                                                                   | Contents                                               |
| --------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| `.`                                                                                     | `defineStateSink`, state protocol headers and errors   |
| `./stream`                                                                              | `defineStreamSink`                                     |
| `./document`                                                                            | `defineDocumentSink`                                   |
| `./route`, `./route-params`, `./fingerprint`                                            | Route compilation, parameter codecs, entity tags       |
| `./server/state`, `./server/stream`, `./server/document`                                | The Effect handlers                                    |
| `./action`, `./action/errors`, `./action/outbox`, `./action/runtime`, `./action/sqlite` | Action sinks, outbox, delivery runtime, SQLite backing |
| `./bun`                                                                                 | The Bun host                                           |
| `./cloudflare`                                                                          | Durable Object host and placement routing              |
| `./alchemy`                                                                             | Effect handlers and router for Alchemy                 |

The root and its sibling declaration entries do not import `effect`. There is
no barrel; each entry names the module that owns its symbols.

Sinks carry no authorization. Put access control at the HTTP or session
boundary that wraps the handlers.

## License

MIT
