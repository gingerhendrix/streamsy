# SQLite migration to 0.4

Streamsy 0.4 introduces a new Effect SQL storage format. Existing experimental
0.3 SQLite databases are not imported because their offset and record formats
are incompatible with the 0.4 storage contract.

Open a fresh database path for 0.4. When `@streamsy/storage` detects the legacy
`streamsy_schema_version` table or legacy protocol tables without the new-format
version marker, layer acquisition fails before making a schema change. The same
safe rejection applies to schema versions newer than this package supports.
Streamsy never deletes, resets, or rewrites an unsupported database automatically.

New-format migrations are recorded in `streamsy_storage_schema_version` and run
in one lock-protected transaction.
Fresh creation, later supported upgrades, and the version record commit together;
failure or interruption rolls the migration back.
