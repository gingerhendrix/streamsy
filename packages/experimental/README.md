# @streamsy/experimental

Experimental Streamsy primitives are exposed through explicit subpath exports while they mature. There is no package-root API.

## Causal vocabulary

Import stream identity helpers from `@streamsy/experimental/stream-identity`. The broader pure causal API remains available from `@streamsy/experimental/causal`.

`streamIdentity()` constructs a structured, mesh-assigned identity independently of a stream URL or application stream id. `encodeStreamIdentity()` provides its versioned canonical durable-key encoding; the v1 encoding deliberately leaves lifetime/incarnation for a later encoding version.

`sourceAck()` and `sourceWatermark()` accept only real durable-stream positions. The protocol read values `-1` and `now` remain ordinary client read offsets and are rejected as causal positions. `coverage()` returns `proven`, `not-yet`, or `incomparable`; identities must match before positions are compared lexicographically.

## Binding

Import the binding API from `@streamsy/experimental/binding`.

`bindStream()` creates an inert `{ identity, client, streamId }` value. The binding is not another transport handle, registry, or address resolver. Transport operations stay on the fixed `StreamProtocolClient` handle, while Effect-owned orchestration consumes that Promise client through the capabilities below.

## Effect capabilities

Import the Effect-native `ReadStreams` and `AppendStreams` capabilities from `@streamsy/experimental/effect`, and deterministic test-layer helpers from `@streamsy/experimental/effect/testing`. `DerivedRecovery` is exported from `@streamsy/experimental/ivm-mesh` with the derived-state orchestration that it serves.

These are finite capabilities; a binding remains a method argument rather than becoming a service tag. Live Layers adapt the existing fixed Promise client. Expected transport/session failures use schema-backed tagged errors, while protocol classifications such as missing, gone, duplicate, conflict, stale epoch, and producer gap remain values.

The package pins `effect@4.0.0-rc.109` exactly. Libraries return Effect descriptions and never create a runtime or call `runPromise` internally.

## State projection facade

Import `StateProjection` from `@streamsy/experimental/state-projection` for the first application-facing bounded projection seam. `make()` declares stable identity, a service-free Effect `Schema` decoder, and pure JSON-item-to-State logic. `resource()` creates inert stream identity/id values, and `instance()` binds two resources to a declaration, generation, and fixed producer epoch without capturing a client. Provide `layerClient(client)` when running `catchUp()`; the facade resolves resources to compatibility bindings and derives the producer lane internally. Public progress omits recovered checkpoints and producer sequence.

This tracer supports one ordered JSON source and one Durable State target, runs a finite catch-up pass, and scans complete target history during recovery. The facade owns its fixed-client Effect Layer while `StreamBinding` remains a plain method argument and `ReadStreams` / `AppendStreams` adapt the Promise-native client. State change constructors, snapshots, resource address resolution, and long-lived supervision remain later API batches. Existing `@streamsy/experimental/ivm-mesh` exports remain available for regression compatibility.

## IVM mesh incubation

`@streamsy/experimental/ivm-mesh` is a framework-private incubation area, not a generic writer or processor API. Its first output generation uses one deterministic, bounded producer lane derived from processor id/version, generation, and canonical source/target identities. The epoch is fixed configuration for that immutable generation and is never claimed or bumped on restart; producer sequence counts append batches.

The reserved State collection is `__streamsy.mesh.lineage.v1`, with canonical row key `checkpoint` and format `streamsy.mesh.lineage.v1`. All `__streamsy.` State types are reserved; fact events using that prefix are rejected. The row records processor/version/generation, canonical identities, producer lane/epoch, incorporated source position, and the next batch sequence.

`appendDerivedStateBatch()` is Effect-native private framework machinery scoped to this subpath. It writes ordered fact events plus exactly one final lineage upsert through one `appendJsonBatch()` request with producer tuple and expected target offset. `recoverDerivedState()` obtains that target offset and checkpoint from durable history through `DerivedRecovery`; it never follows an append with `HEAD`. Unknown lineage is decoded with `Schema`. A duplicate is re-read and reported as `sequence-already-accepted` only when the durable lane, source-through, current transaction boundary, and next sequence reconcile. The boundary check detects trailing output; it is not evidence of transition or payload equality.

Compatibility note: `appendJsonBatch()` is now a required `StreamProtocolHandle` member. Third-party implementations of that interface must add the one-request ordered JSON operation when adopting this revision.

`catchUp()` is a named, bounded Effect program with explicit `recover → pull → pure step → commit` phases, not a follow loop, arbitrary Stream pipeline, or core processor interface. At startup it scans output State history to recover its authoritative lineage and producer sequence, so this incubation path is O(output history). It then resumes the source strictly after that domain-correct source position. Limits apply to complete source delivery boundaries: pages are delivered client batches, batches are committed State transactions, items are decoded source items, and bytes are the encoded source batch size. A boundary that only exceeds the invocation's remaining budget is left uncommitted for deterministic continuation. A single boundary larger than the configured item or byte maximum returns terminal `boundary-too-large`, so repeating the same configuration cannot silently livelock. Filtered boundaries still commit a metadata-only State transaction. Interruption remains interruption; interruption during an append leaves durability unknown until restart recovery decides whether the boundary committed.
