---
packages:
  "group:streamsy": patch
---

## Long projection producer ids are hashed

A `Commit.producer` projection, such as `Projection.wake`, appends under the producer id `<id>/v<version>/<params JSON>`. Storage refuses a producer id over 255 code points. Before this release, a binding with long params, such as a hex folder id over 113 UTF-8 bytes, failed every append with a `mutate.producer-id` storage fault and never wrote its output.

- A producer id of at most 255 code points does not change. Deployed outputs keep their producer state under it.
- A longer producer id becomes `<id>/v<version>/#<sha256>`, where the SHA-256 is the hex digest of the full id. When the projection id itself is long, the part before `#` is cut so the whole id is 255 code points. The digest uses WebCrypto (`crypto.subtle`), which Bun, Node, workerd and browsers in a secure context provide.
- A retry of a pinned unit sends the same hashed id, so the output still answers `Duplicate`.
- The checkpoint key is unchanged. MySQL stores it in a `VARCHAR(512)` column, so a checkpoint save on MySQL can still fail when the params make that key longer than 512 characters. SQLite, Durable Object storage and Postgres store it as `TEXT`.
