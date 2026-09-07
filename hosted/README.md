# Hosted preparation

This standalone private package typechecks the pinned Alchemy v2 stack and tests
the small Effect workflow with explicit fake Layers. `bun run typecheck` checks
the stack declarations; `bun test test` exercises validation, polling,
measurement protocol, cleanup, reports, and the blocked executable.

`bun run evidence` always prints the fixed blocked status and exits 2 in this
local range, including when fake credentials or permission values are present.
There are no live deploy/destroy, Cloudflare metadata, process, HTTP, external
conformance, or celld adapters here. A later permission-gated batch must add and
independently review those capabilities.

The official local workerd runner uses `Placement.byKey(() => "conformance")`,
placing all suite streams in one Durable Object while retaining distinct stream
IDs. It exercises same-object chain semantics, including source retention and
cascade collection. Cross-object copy behavior is verified separately by the
accepted real-workerd host tests. Default `Placement.byStream()` copies have no
source retention edge and do not satisfy the official suite's nine
chain-lifecycle assertions. The worker uses a 1,500 ms test long-poll override;
its single-object artifact cannot establish distinct first-object activations.
