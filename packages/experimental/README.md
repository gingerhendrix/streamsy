# @streamsy/experimental

Experimental Streamsy primitives are exposed through explicit subpath exports while they mature. There is no package-root API.

## Causal vocabulary

Import the pure causal API from `@streamsy/experimental/causal`.

`streamIdentity()` constructs a structured, mesh-assigned identity independently of a stream URL or application stream id. `encodeStreamIdentity()` provides its versioned canonical durable-key encoding; the v1 encoding deliberately leaves lifetime/incarnation for a later encoding version.

`sourceAck()` and `sourceWatermark()` accept only real durable-stream positions. The protocol read values `-1` and `now` remain ordinary client read offsets and are rejected as causal positions. `coverage()` returns `proven`, `not-yet`, or `incomparable`; identities must match before positions are compared lexicographically.

## Binding

Import the binding API from `@streamsy/experimental/binding`.

`bindStream()` creates an inert `{ identity, client, streamId }` value. `readBoundStream()` and `appendBoundStream()` delegate through the existing fixed `StreamProtocolClient` handle; the binding is not another transport handle, registry, or address resolver. Read offsets, including `-1` and `now`, pass through unchanged.

`appendBoundStream()` adds a `SourceAck` only to a new `appended` result carrying its exact response offset. Producer `duplicate` remains a distinct result without an acknowledgement and does not assert payload equality. The binding never follows an append with `HEAD` or infers identity from the stream id or URL.

## Materializer

Import the materializer API from `@streamsy/experimental/materializer`.

It provides pure catch-up folds and checkpoint storage over the transport-neutral Streamsy client. Both direct and remote clients can be supplied through the same `StreamProtocolClient` seam.

`materialize()` reads from an optional after-exclusive client cursor, folds every currently available content-aware batch, inspects the session terminal result, and returns the last completely consumed batch cursor. Its decoder maps each batch to zero or more domain events. It rejects on source/read/session, decode, or evolve failure. The pure fold commits nothing, so re-running it after a failure is safe.

Checkpoint snapshots and the checkpoint store's `State` must be JSON-serializable. By default, each view is stored at `__streamsy/views/${encodeURIComponent(viewId)}/checkpoint`; callers may supply a custom stream-id function.

Checkpoint loading is fail-fast when the latest record is malformed and uses last-write-wins, not max-cursor-wins, semantics. Re-appending a stale checkpoint can regress its cursor, but that is safe for level-triggered, idempotent consumers because it only causes batches to be read again. Loading currently reads the full checkpoint stream and costs O(saves); planned stream compaction will bound that cost.

## IVM mesh incubation

`@streamsy/experimental/ivm-mesh` is a framework-private incubation area, not a generic writer or processor API. Its first output generation uses one deterministic, bounded producer lane derived from processor id/version, generation, and canonical source/target identities. The epoch is fixed configuration for that immutable generation and is never claimed or bumped on restart; producer sequence counts append batches.

The reserved State collection is `__streamsy.mesh.lineage.v1`, with canonical row key `checkpoint` and format `streamsy.mesh.lineage.v1`. All `__streamsy.` State types are reserved; fact events using that prefix are rejected. The row records processor/version/generation, canonical identities, producer lane/epoch, incorporated source position, and the next batch sequence.

`appendDerivedStateBatch()` is private framework machinery scoped to this subpath. It writes ordered fact events plus exactly one final lineage upsert through one `appendJsonBatch()` request with producer tuple and expected target offset. `recoverDerivedState()` obtains that target offset and checkpoint from durable history; it never follows an append with `HEAD`. A duplicate is re-read and reported as `sequence-already-accepted` only when the durable lane, source-through, target tail, and next sequence reconcile exactly. This is deliberately not payload verification.
