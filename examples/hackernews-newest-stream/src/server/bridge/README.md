# Private State projection bridge

This is private, example-local code for the Hacker News demo. Step 5 replaces it
with `@streamsy/derive`; it is not a supported library API.

One process runtime owns one memory store. The bridge consumes `StreamsReader`
and `StreamsWriter` from that store. It scans the complete target history,
recovers the last fact's `headers.offset`, then projects whole source read pages.
Each source page produces one target append under the recovered target tail's
`expectedOffset` CAS. The browser receives the same State upsert/delete facts,
including `offset` and `txid` headers. A conflict leaves progress unchanged.
Source pages, batches, decoded items and encoded JSON bytes count toward the
limits. Recovery is O(target history) and is not included in these limits.
An oversized source page is rejected whole, never split to fit an item bound.

This bridge requires at least one fact for each source boundary, with every
fact carrying that boundary's offset. Hacker News always emits one per input.
It cannot durably track progress for zero-output transforms without checkpoints.

Deferred to Step 5: target producer-epoch fencing, in-band lineage rows,
checkpoints/snapshots, generations, replay-safe pending writes, owner fencing,
Source/Sink abstractions, and mesh behavior. `generation` and `producerEpoch`
remain declaration fields only. The `stale-epoch`, `producer-gap`, and
`invalid-epoch-seq` outcome types remain for shape compatibility and are never
produced. Expected-offset conflicts are the only competing-output protection.
Memory state lasts only for the owning Layer's lifetime.
