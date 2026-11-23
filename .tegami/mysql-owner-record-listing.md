---
packages:
  "@streamsy/storage": minor
  "@streamsy/projection": minor
---

## List projection bindings and manage routed outputs on MySQL

`Projection.bindings` now works on MySQL. `Projection.retire` and `Projection.forget` now also work there for bindings with routed outputs.

**Breaking change:** Opening a MySQL database adds `full_key` to `streamsy_owner_records`, adds `full_params` to `streamsy_projection_pending`, and moves storage to version 4. Streamsy 0.13.0 cannot open a MySQL database at version 4. SQLite and Postgres databases stay at version 3. See the [SQL migration rules](https://streamsy.dev/docs/reference/storage/sql-storage#sqlstoragelayer).

Stop every 0.13.0 process before you open a MySQL database with this release. A 0.13.0 process that stays attached can read the fitted form of a long pending mark, fail to decode it, and delete the mark without running the binding. The binding then waits for its next commit to be marked again.

A MySQL owner key longer than 512 code points that 0.13.0 wrote lists again after its next write. Until then, listing a range containing the key fails.

Durable pending marks on MySQL now support params longer than 512 code points. Both attach hosts reject MySQL projection IDs longer than 255 code points when durability is enabled.

An attached projection on MySQL now discovers its stored bindings at startup.
