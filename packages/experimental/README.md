# @streamsy/experimental

Experimental Streamsy primitives are exposed through explicit subpath exports while they mature. There is no package-root API.

## Command logs and derived streams

`@streamsy/experimental/command` provides `createCommandLog`, an event-sourced
command boundary that owns canonical folding, stream-anchored command-id
deduplication, payload-reuse rejection, expected-offset retry, and optional
durable acknowledgement caching.

`@streamsy/experimental/derived` provides `catchUpDerived` for replay-safe
one-source-to-many fan-out using producer sequences, plus cursor-based
`readDerived` with optional long polling.

## Materializer

Import the materializer API from `@streamsy/experimental/materializer`.

It provides pure catch-up folds and checkpoint storage over the transport-neutral Streamsy client. Both direct and remote clients can be supplied through the same `StreamProtocolClient` seam.

`materialize()` reads from an optional after-exclusive client cursor, folds every currently available content-aware batch, inspects the session terminal result, and returns the last completely consumed batch cursor. Its decoder maps each batch to zero or more domain events. It rejects on source/read/session, decode, or evolve failure. The pure fold commits nothing, so re-running it after a failure is safe.

Checkpoint snapshots and the checkpoint store's `State` must be JSON-serializable. By default, each view is stored at `__streamsy/views/${encodeURIComponent(viewId)}/checkpoint`; callers may supply a custom stream-id function.

Checkpoint loading is fail-fast when the latest record is malformed and uses last-write-wins, not max-cursor-wins, semantics. Re-appending a stale checkpoint can regress its cursor, but that is safe for level-triggered, idempotent consumers because it only causes batches to be read again. Loading currently reads the full checkpoint stream and costs O(saves); planned stream compaction will bound that cost.

## Projection runtime

Import the projection runtime from `@streamsy/experimental/projection`.

`ProjectionRuntime` materializes a canonical **source** stream into a _separate_
**projection** stream where every output transition atomically embeds the source
offset it is valid through. The canonical stream stays the source of truth; the
projection is a rebuildable, causally-watermarked materialization. Unlike the
materializer's `streamCheckpointStore`, the watermark is **not** a detached
checkpoint — it is co-committed with the board changes in the same append, so no
board state can commit without its source-through offset (and vice versa).

The runtime owns everything replay-safety needs; the domain supplies only the
pure pieces through a `ProjectionAdapter<State, Event>`:

- `decodeSourceMessage(data)` → one domain event per source message;
- `reduce(state, event, meta)` — pure reducer; **throw** to mark a poison event;
- `encodeTransition({ prev, next, event, meta })` → the JSON items appended
  atomically as one output batch (must embed the watermark + a resume snapshot);
- `decodeCheckpoint(messages)` → recover the latest `{ state, sourceThroughOffset,
sourceSeq }` from the projection stream, or `null` when empty.

For Durable State outputs, `durableStateProjectionAdapter` builds that adapter
from a Durable State schema, a pure reducer, and row extraction. It owns row
diffing and the co-committed checkpoint/watermark row.

### Atomicity

A transition's items are appended as a single JSON array in one
`ProtocolStream.append`. Streamsy frames a JSON-array body into one message per
item committed in a single storage transaction (`AppendPlan`), so board changes
and their watermark can never commit apart.

### Replay-safe identity and concurrency

Each transition appends under producer identity
`producerId = <processorId>::<generation>::<sourceStreamId>` and
`producerSeq = <0-based source ordinal>`.

- **Ambiguous append retry** → re-appending an already-committed transition
  returns `duplicate` (no second write).
- **Concurrent writers with the same identity** → the loser's re-append is
  classified `duplicate`; no transition is written twice.
- **Concurrent writers with distinct identities** → arbitrated by `expectedOffset`
  CAS on the projection tail; the loser gets `conflict`/`expected-offset`,
  reloads, and converges.

### Recovery model

State lives entirely in the projection stream. On construct + `load()` (or the
first `catchUp()`), the runtime rebuilds `{ state, watermark, output tail }` from
the projection and resumes reading the source **strictly after** the durable
watermark. Because the watermark is co-committed with the board, a crash
immediately after an output commit is recovered by resuming past it — the event
is never re-applied. A reducer that throws halts the projection visibly at the
prior watermark (`status().lastError`) rather than skipping the event. Wakes
(`follow()`) are hints only; the durable watermark remains the source of truth,
so duplicate or spurious wakes are harmless.

### Status

`await runtime.status()` reports `running`, `stopped`, `caughtUp`, `sourceHead`,
`sourceThroughOffset`, `sourceSeq`, `outputTail`, and `lastError`. A numeric lag
is not reported because offsets are opaque; `caughtUp` (watermark has reached the
source head) is exposed instead.
