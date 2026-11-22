---
packages:
  "@streamsy/core": minor
  "@streamsy/storage": minor
---

## List owner records by key prefix

**Breaking change:** the `OwnerRecords` service from `@streamsy/core/internal` now requires `list(prefix)`. It returns the owner's `[key, encoded]` entries whose keys start with the prefix. `@streamsy/projection` uses it to find the bindings to replay when you rebuild a projection.

The memory storage and the SQLite and PostgreSQL storage in `@streamsy/storage` implement `list`. MySQL storage fails it with a `StorageFault` whose `operation` is `OWNER_RECORDS_LIST_UNSUPPORTED`, because MySQL may have shortened long keys.

You need to act only if you provide `OwnerRecords` from your own storage host. Add `list`, or fail with that operation when your store cannot list. [Memory storage internal services](https://streamsy.dev/docs/reference/core/memory-storage#types-with-no-exported-name)
