# @streamsy/streams

Effect-native stream capabilities for Streamsy durable streams, together with the causal vocabulary and stream bindings they operate on. Each public API is reached through the subpath that owns it; there are no re-export modules.

| Subpath                         | Module               | Contents                                                                             |
| ------------------------------- | -------------------- | ------------------------------------------------------------------------------------ |
| `@streamsy/streams`             | `src/streams.ts`     | `CreateStreams`, `ReadStreams`, `AppendStreams`, and the schema-backed tagged errors |
| `@streamsy/streams/binding`     | `src/binding.ts`     | `bindStream()` and `StreamBinding`                                                   |
| `@streamsy/streams/identity`    | `src/identity.ts`    | `streamIdentity()` and its canonical encoding                                        |
| `@streamsy/streams/causal`      | `src/causal.ts`      | `coverage()`, `sourceAck()`, `sourceWatermark()`, stream positions                   |
| `@streamsy/streams/testing`     | `src/testing.ts`     | `TestStreams` and `TestStreamsLayer`                                                 |
| `@streamsy/streams/test-layers` | `src/test-layers.ts` | `provideTestLayers()` for the executable test boundary                               |

## Causal vocabulary

Import stream identity helpers from `@streamsy/streams/identity`. The broader pure causal API lives in `@streamsy/streams/causal`.

`streamIdentity()` constructs a structured, mesh-assigned identity independently of a stream URL or application stream id. `encodeStreamIdentity()` provides its versioned canonical durable-key encoding; the v1 encoding deliberately leaves lifetime/incarnation for a later encoding version.

`sourceAck()` and `sourceWatermark()` accept only real durable-stream positions. The protocol read values `-1` and `now` remain ordinary client read offsets and are rejected as causal positions. `coverage()` returns `proven`, `not-yet`, or `incomparable`; identities must match before positions are compared lexicographically.

## Binding

Import the binding API from `@streamsy/streams/binding`.

`bindStream()` creates an inert `{ identity, client, streamId }` value. The binding is not another transport handle, registry, or address resolver. Transport operations stay on the fixed `StreamProtocolClient` handle, while Effect-owned orchestration consumes that Promise client through the capabilities below.

## Effect capabilities

Import the Effect-native `CreateStreams`, `ReadStreams`, and `AppendStreams` capabilities from the package root, and deterministic test-layer helpers from `@streamsy/streams/testing`. `DerivedRecovery` lives in `@streamsy/projection/mesh` with the derived-state orchestration that it serves.

These are finite capabilities; a binding remains a method argument rather than becoming a service tag. Live Layers adapt the existing fixed Promise client. Expected transport/session failures use schema-backed tagged errors, while protocol classifications such as missing, gone, duplicate, conflict, stale epoch, and producer gap remain values.

The package pins `effect@4.0.0-rc.112` exactly. Libraries return Effect descriptions and never create a runtime or call `runPromise` internally.
