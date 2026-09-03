# @streamsy/projection

Bounded derived state for Streamsy durable streams. The package root is the application-facing `StateProjection` facade; `@streamsy/projection/mesh` is the framework-private incubation area beneath it.

| Subpath                     | Module                    | Contents                 | Audience          |
| --------------------------- | ------------------------- | ------------------------ | ----------------- |
| `@streamsy/projection`      | `src/state-projection.ts` | `StateProjection` facade | application       |
| `@streamsy/projection/mesh` | `src/mesh.ts`             | IVM mesh incubation      | framework private |

`src/mesh.ts` is a re-export entry module over `src/mesh/*`. It is the single sanctioned exception to the repository's no-barrel rule, granted because `/mesh` is a private incubation seam whose module split is expected to change. Do not treat it as a pattern for other packages, and do not import it from application code you intend to keep.

## State projection facade

Import `StateProjection` from `@streamsy/projection` for the first application-facing bounded projection seam. `make()` declares stable identity, a service-free Effect `Schema` decoder, and pure JSON-item-to-State logic. `resource()` creates inert stream identity/id values, and `instance()` binds two resources to a declaration, generation, and fixed producer epoch without capturing a client. Provide `layerClient(client)` when running `catchUp()`; the facade resolves resources to compatibility bindings and derives the producer lane internally. Public progress omits recovered checkpoints and producer sequence.

This tracer supports one ordered JSON source and one Durable State target, runs a finite catch-up pass, and scans complete target history during recovery. The facade owns its fixed-client Effect Layer while `StreamBinding` remains a plain method argument and `ReadStreams` / `AppendStreams` from `@streamsy/streams` adapt the Promise-native client. State change constructors, snapshots, resource address resolution, and long-lived supervision remain later API batches. The `@streamsy/projection/mesh` exports remain available for regression compatibility.

## IVM mesh incubation

`@streamsy/projection/mesh` is a framework-private incubation area, not a generic writer or processor API. Its first output generation uses one deterministic, bounded producer lane derived from processor id/version, generation, and canonical source/target identities. The epoch is fixed configuration for that immutable generation and is never claimed or bumped on restart; producer sequence counts append batches.

The reserved State collection is `__streamsy.mesh.lineage.v1`, with canonical row key `checkpoint` and format `streamsy.mesh.lineage.v1`. All `__streamsy.` State types are reserved; fact events using that prefix are rejected. The row records processor/version/generation, canonical identities, producer lane/epoch, incorporated source position, and the next batch sequence.

`appendDerivedStateBatch()` is Effect-native private framework machinery scoped to this subpath. It writes ordered fact events plus exactly one final lineage upsert through one `appendJsonBatch()` request with producer tuple and expected target offset. `recoverDerivedState()` obtains that target offset and checkpoint from durable history through `DerivedRecovery`; it never follows an append with `HEAD`. Unknown lineage is decoded with `Schema`. A duplicate is re-read and reported as `sequence-already-accepted` only when the durable lane, source-through, current transaction boundary, and next sequence reconcile. The boundary check detects trailing output; it is not evidence of transition or payload equality.

Compatibility note: `appendJsonBatch()` is now a required `StreamProtocolHandle` member. Third-party implementations of that interface must add the one-request ordered JSON operation when adopting this revision.

`catchUp()` is a named, bounded Effect program with explicit `recover → pull → pure step → commit` phases, not a follow loop, arbitrary Stream pipeline, or core processor interface. At startup it scans output State history to recover its authoritative lineage and producer sequence, so this incubation path is O(output history). It then resumes the source strictly after that domain-correct source position. Limits apply to complete source delivery boundaries: pages are delivered client batches, batches are committed State transactions, items are decoded source items, and bytes are the encoded source batch size. A boundary that only exceeds the invocation's remaining budget is left uncommitted for deterministic continuation. A single boundary larger than the configured item or byte maximum returns terminal `boundary-too-large`, so repeating the same configuration cannot silently livelock. Filtered boundaries still commit a metadata-only State transaction. Interruption remains interruption; interruption during an append leaves durability unknown until restart recovery decides whether the boundary committed.

The package pins `effect@4.0.0-rc.112` exactly.
