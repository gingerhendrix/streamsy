# Hosted preparation

This standalone private package typechecks the pinned Alchemy v2 stack and tests
the small Effect workflow with explicit fake Layers. `bun run typecheck` checks
the stack declarations; `bun test test` exercises validation, polling,
measurement protocol, cleanup, reports, and the disabled executable.

Alchemy deployment for Step 3 is authorized. Hosted execution remains disabled
in this local package pending independently reviewed live adapters and a
reconciled run plan, including destroy/cleanup and required query scope. Hosted
acceptance still requires hosted evidence, the uploaded-compressed-byte/startup-
CPU policy, and Gareth's budget/topology decision. The accepted Batch B local
signal is 81,574 B gzip against the unchanged 27,160 B proposal. The 542.85 ms
first-object p95 proposal remains unmeasured.

`bun run evidence` prints that status and exits 2 in this local range, including
when fake credentials or permission values are present. There are no live
deploy/destroy, Cloudflare metadata, process, HTTP, external conformance, or
celld adapters here. A later batch must add and independently review those
capabilities against the reconciled run plan.

The official local workerd runner uses `Placement.byKey(() => "conformance")`,
placing all suite streams in one Durable Object while retaining distinct stream
IDs. It exercises same-object chain semantics, including source retention and
cascade collection. Cross-object copy behavior is verified separately by the
accepted real-workerd host tests. Default `Placement.byStream()` copies have no
source retention edge and do not satisfy the official suite's nine
chain-lifecycle assertions. The worker uses a 1,500 ms test long-poll override;
its single-object artifact cannot establish distinct first-object activations.
Fake measurement tests use the historical all-first-then-warm ordering and
report `firstMinusWarm` (the candidate PUT body is `"x"`; Step 0 used an empty
PUT), so their arithmetic is not a hosted comparison.
