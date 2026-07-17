# @streamsy/experimental

Experimental Streamsy primitives are exposed through explicit subpath exports while they mature.

## Materializer

Import the materializer API from `@streamsy/experimental/materializer`. It provides pure, bounded folds and checkpoint storage for Streamsy streams.

`materialize()` folds records in the after-exclusive range `(from, to]`. It
rejects on the first source, read, decode, or evolve failure. The pure fold
commits nothing, so re-running it after a failure is safe.

Checkpoint snapshots and the checkpoint store's `State` must be
JSON-serializable. By default, each view is stored at
`__streamsy/views/${encodeURIComponent(viewId)}/checkpoint`; callers may supply
a custom stream-id function.

Checkpoint loading is fail-fast when the latest record is malformed and uses
last-write-wins, not max-cursor-wins, semantics. Re-appending a stale checkpoint
can regress its cursor, but that is safe for level-triggered, idempotent
consumers because it only causes records to be read again. Loading currently
reads the full checkpoint stream and costs O(saves); planned stream compaction
will bound that cost.
