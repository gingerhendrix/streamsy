---
packages:
  "@streamsy/storage": minor
---

## List projection bindings and manage routed outputs on MySQL

`Projection.bindings`, `Projection.retire`, and `Projection.forget` now work on MySQL for bindings with routed outputs.

**Breaking change:** Opening a MySQL database adds `full_key` to `streamsy_owner_records`, adds `full_params` to `streamsy_projection_pending`, and moves storage to version 4. Streamsy 0.13.0 cannot open a MySQL database at version 4. SQLite and Postgres databases stay at version 3. See the [SQL migration rules](https://streamsy.dev/docs/reference/storage/sql-storage#sqlstoragelayer).

A MySQL owner key longer than 512 code points that 0.13.0 wrote lists again after its next write. Until then, listing a range containing the key fails.

Durable pending marks on MySQL now support params longer than 512 code points. Both attach hosts reject MySQL projection IDs longer than 255 code points at startup.

An attached projection on MySQL now discovers its stored bindings at startup.
