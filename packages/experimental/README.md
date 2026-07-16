# @streamsy/experimental

Experimental Streamsy primitives are exposed through explicit subpath exports while they mature. There is no package-root API.

## Materializer

Import the materializer API from `@streamsy/experimental/materializer`.

It provides pure catch-up folds and checkpoint storage over the transport-neutral Streamsy client. Both direct and remote clients can be supplied through the same `StreamProtocolClient` seam.

`materialize()` reads from an optional after-exclusive client cursor, folds every currently available content-aware batch, inspects the session terminal result, and returns the last completely consumed batch cursor. Its decoder maps each batch to zero or more domain events. It rejects on source/read/session, decode, or evolve failure. The pure fold commits nothing, so re-running it after a failure is safe.

Checkpoint snapshots and the checkpoint store's `State` must be JSON-serializable. By default, each view is stored at `__streamsy/views/${encodeURIComponent(viewId)}/checkpoint`; callers may supply a custom stream-id function.

Checkpoint loading is fail-fast when the latest record is malformed and uses last-write-wins, not max-cursor-wins, semantics. Re-appending a stale checkpoint can regress its cursor, but that is safe for level-triggered, idempotent consumers because it only causes batches to be read again. Loading currently reads the full checkpoint stream and costs O(saves); planned stream compaction will bound that cost.
