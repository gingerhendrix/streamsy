# Frozen HTTP wire expectations

Captured at repository base `9c86871a631fdcdeffb95dec1aed1d94380d8856`, before the
package swap, by running the existing side-by-side HTTP tests successfully.
`wire.json` stores every status, status text, complete header map and exact byte
array. It is test data and is excluded from package build entries.

The private/public sequences contain 57 responses each. Fifteen expiry cases add
PUT and HEAD pairs; two open SSE cases include data, initial control and actual
timeout control frames. Fixed wall time is 1780000000000; timers remain live.
HEAD expectations use bodyless wire semantics. Lowercase ISO and RFC UTC expiry
rejection rows are frozen from the new Effect edge, the explicitly accepted
behavioral difference; other expected rows come from the comparison implementation.

The recorder and successful comparison log are retained in the stream's registered
`2026-09-06-streamsy-batch-6` scratch directory. Tests have no regeneration mode:
a changed expectation must be reviewed as a wire-contract change. New ingress,
StorageFault, interruption, subscription cleanup, host shutdown and rebind tests
remain separate and unchanged by fixture capture.
