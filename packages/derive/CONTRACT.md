# Step 5 contract

Selected before backend implementation. `Projection.make({ id, version, source,
sink, initial, stateSchema, step })` is inert. `step(state, input, context)` is pure
and returns `{ state, outputs }`. Context has identity and the boundary position.
`Source<A>` has stable `identity`, `initialPosition`, `pull(after, { items, bytes })`
and `wait(after)`. Pull returns an ordered replayable boundary or history unavailable.
`Sink<A>` has identity, initialPosition, its exact Commit owner, and
`write(outputs, previousPosition)`; it must join the host's transaction.

`Commit` is a Layer-provided service holding CheckpointStore, StateStore and
`withTransaction(body)`. Both stores persist encoded records; persisted envelopes
and application state are Schema decoded. The kernel restores outside commit,
pulls and steps, then validates the prior checkpoint again inside commit before
writing sink, state and checkpoint. No retries, remote delivery or owner fencing.

`Projection.pass` accepts one boundary; `catchUp` bounds boundaries, items and
reported bytes, and returns caught-up, source-closed or limit-reached plus counts
and checkpoint. Typed DeriveFault reasons cover history, identity, corrupt state,
sink conflict, storage, unsupported composition and invalid source/limits.
`follow` returns a caller-scoped fiber; each cycle catches up then waits with a
bounded repair timeout. Terminal source close and typed failures remain observable
through the fiber. Defaults: 100 boundaries, 1000 items, repair every 1000 ms.

Identity is { id, version: String(version), generation: `v${version}`, source, sink }.
Stores key by projection id, so changing identity stops rather than implicitly
resetting. A new id and dedicated sink are required for a fresh lane. One host
owner and one runner per id are supported. Source history and stream identity
must be retained; deletion/recreation under the same id is unsupported.
