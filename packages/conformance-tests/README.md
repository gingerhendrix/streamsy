# Conformance fixtures

Build the repository packages first with `bun run build`, then use
`bun run --cwd packages/conformance-tests build:worker` to create the single
`dist/worker/worker.js` artifact and its adjacent `bundle-report.json`.

`test:memory`, `test:sqlite`, and `test:workerd` register the official suite.
The official local workerd runner uses `Placement.byKey(() => "conformance")`,
placing all suite streams in one Durable Object while retaining distinct stream
IDs. It exercises same-object chain semantics, including source retention and
cascade collection. Cross-object copy behavior is verified separately by the
accepted real-workerd host tests. Default `Placement.byStream()` copies have no
source retention edge and do not satisfy the official suite's nine
chain-lifecycle assertions.

The workerd fixture is local Miniflare with Durable Object SQLite, compatibility
date `2026-07-30`, `nodejs_compat`, loopback networking, and a 1,500 ms test
long-poll override. Its result is local conformance, not hosted evidence. The
single artifact and its bundle report are attributed to this named profile; the
profile cannot establish distinct first-object activations or the proposed
first-object latency p95. Fake measurement tests preserve the historical
all-first-then-warm ordering and name the arithmetic `firstMinusWarm`; the
candidate first PUT body is `"x"`, whereas Step 0 used an empty PUT, so no
hosted comparison is implied.

The worker has no probes and imports only built public package exports. Hosted
deployment, metadata, remote measurement, and arbitrary external targets remain
deferred.
