---
packages:
  "group:streamsy": patch
---

## Long checkpoint keys save on MySQL

`CheckpointStore.layer` keeps each checkpoint under a key that holds the projection id, version, generation and params JSON. MySQL stores that key in a `VARCHAR(512)` column. Before this release, a projection whose params made the key longer than 512 characters failed every checkpoint save on MySQL with `ER_DATA_TOO_LONG`, so it never advanced.

- On MySQL, SQL storage now hashes an owner record key over 512 characters to the first 447 characters, `#` and the SHA-256 hex of the full key. Two long keys that share their first 512 characters stay apart.
- A key of at most 512 characters does not change, so stored checkpoints stay readable.
- SQLite, Durable Object storage and Postgres store the key as `TEXT` and keep every key as it is, so checkpoints they already hold under long keys stay readable.
- `recordKey` from `@streamsy/projection/checkpoint` is unchanged. A custom Commit that stores records elsewhere still sees the full key and must fit it into its own store's key limit.
