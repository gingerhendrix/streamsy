# Storage contract

`Storage` is an Effect service. `Protocol.layer(options)` requires it and supplies
`StreamsReader` and `StreamsWriter`. `Streams.layerMemory` composes those layers.
The contract and memory constructor are public through `@streamsy/core/storage`;
schemas and `StorageFault` are also available from the root.

## Reads, mutations and failures

`record(id)`, `messages(id, window)` and `producer(id, producerId)` return Effects
with typed `StorageFault`. Records use `Option`; message windows have exclusive
`after`, inclusive `until` and an optional limit. Returned records and message
bytes must be independent of caller inputs and stored state.

`mutate({ operations })` takes a nonempty ordered set of `Create`, `Append` or
`Delete` operations. Internal values use PascalCase `_tag` discriminants.
`MutationApplied` is the success-only `{ _tag: "Applied", results }` schema and
supplies ordered operation results. Expected rejection fails with the schema-backed
`MutationRejected` error, carrying the failing operation index, a reason and an
`Option<StreamRecord>` current record. Reasons remain `offset`, `closed`, `producer`,
`exists`, `not-found`, `gone`, `fork-source-gone` and `expiry-mismatch`.
I/O failure remains `StorageFault`, with `retryable` indicating policy eligibility
rather than proof that an opaque write is safe to retry. `MutationRejected` never
qualifies for SQL-fault retry. Protocol append instead rereads and replans up to
eight times; duplicate producer acknowledgement still wins.

Validate every precondition against pre-mutation state before writing. Rejection
must change nothing, including producer state, lineage and expiry metadata.
Duplicate operations for one stream and unsupported multi-stream atomicity are
programmer defects.

A rejection escaping `MemoryCommitBoundary.withTransaction` or the SQL
`CommitBoundary.withTransaction` rolls back earlier application writes and pending
invalidations too. Catch `MutationRejected` outside the boundary to recover after
rollback. Catching it inside the outer body deliberately allows that body’s other
writes to commit; nested calls join the owner and do not create savepoints.

`RecordPatch` is trusted protocol input: arbitrary lifecycle or lineage patches
are not a safe public command language. Operation results can describe intermediate
state (a soft-deleted parent can be purged by a later operation in the same mutation);
inspect the final record for final existence.

The protocol masks storage mutation calls against interruption. Memory also masks
its bounded synchronous mutation region and semaphore acquisition. The existing
acquisition-mask and trusted-input review observations remain follow-up items;
this package swap does not claim to repair them.

## Capabilities and memory

| Capability    | Default memory | Constrained memory |
| ------------- | -------------- | ------------------ |
| `fork`        | `chain`        | `copy`             |
| `atomicScope` | `store`        | `stream`           |
| `wake`        | `push`         | `poll`             |
| `expiryIndex` | `indexed`      | `lazy`             |

Fork may also be `none` for other implementations. Chain reads compose inherited
prefixes and child messages. Copy mode makes the child independent of its source.
Memory mutation is guarded by one semaphore inside one acquired store; no
cross-process or disk persistence guarantee is implied.

`changes(id)` is a scoped level-triggered Stream. Subscribe before the first
snapshot so writes cannot fall between snapshot and registration. Snapshots carry
presence, offset, closure and soft deletion. Consumers compare the complete state,
including a lower offset after purge/recreate. Unchanged wakes are valid. Default
memory retains at most one wake per subscriber and rereads authoritative state;
constrained memory polls under the Effect clock (25 ms by default). Interrupting
a consumer or closing the owner must release its subscription.

`nextExpiry` returns the earliest indexed deadline or `None`; lazy stores return
`None`. Expiry deletion uses an expected-deadline precondition. After a stale
deletion rejection, the next indexed observation must advance or remove stale
work so a host sweep cannot spin. The Step 1 Bun host relies on expiry on access.

## Contract tests

```ts
import { Memory } from "@streamsy/core/storage";
import { StorageContract } from "@streamsy/core/testing";

StorageContract.run({
  name: "memory chain",
  layer: Memory.layer(),
  expected: { fork: "chain", atomicScope: "store", wake: "push", expiryIndex: "indexed" },
});
StorageContract.run({
  name: "memory copy",
  layer: Memory.layer({ constrained: true }),
  expected: { fork: "copy", atomicScope: "stream", wake: "poll", expiryIndex: "lazy" },
});
```

Run with Bun. The kit builds a fresh scoped Layer for every case and supplies
`TestClock`; options accept a Layer value, not a factory. Named capability skips
remain visible across the two configurations. The additional `faultyStorage`
decorator fails before a selected mutate call or after its successful application
for ambiguity tests. A rejection propagates unchanged through an after-fault
injection; it consumes the selected attempted call without injecting an I/O fault.
`layerTest` provides real protocol tags plus subscriber/snapshot observations.

## Retired backend evidence

The filesystem protocol backend was removed in Batch 6 with a confirmed, unfixed
multi-writer CAS defect: two writers can both acknowledge the same expected tail.
Removal is not a repair. Historical failure logs and the forced schedule survive
in the implementation stream's `filesystem-cas-diagnosis.md` and its registered
scratch evidence. Persistent protocol storage is later work; the separate views
and serve SQLite stores retain their existing role and are not protocol backends.
