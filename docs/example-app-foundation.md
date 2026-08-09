# Example application foundation

This branch develops Streamsy through complete applications. Each application tests the library under product constraints. Library changes follow evidence from an application.

The foundation starts at `feat/alchemy-projections-demo@455c598`. It is an experimental integration line and does not replace `main`.

## Working cycle

For each application:

1. Create a short application branch from the latest accepted foundation.
2. Build one complete product slice with explicit durability and failure claims.
3. Record awkward application code, unclear concepts, and missing library seams.
4. Review the product behavior, processing semantics, API, and tests.
5. Refactor the smallest contract that the application evidence supports on a focused branch.
6. Update the conceptual guide and a runnable example. Treat difficult documentation as API feedback.
7. Integrate the accepted application, library, and documentation commits into this foundation.

The next application starts from the updated foundation.

## Application acceptance gates

An application is ready for foundation review when it meets these gates:

- **Product:** A user can run a useful end-to-end slice. The interface exposes durable progress and meaningful failure outcomes.
- **Truth:** The source streams, derived state, causal lineage, and ownership boundaries are documented. Code never compares positions from different streams.
- **Recovery:** Automated evidence covers restart and replay. Duplicate delivery, interruption, stale output, and repair behavior are tested where the slice can encounter them.
- **Durability:** Output and its lineage commit together. Wake delivery only reduces latency, while repair rereads durable truth.
- **Architecture:** Effect owns capabilities, resource scope, cancellation, and executable-edge runtime wiring. Pure domain transitions stay independent of transport.
- **Documentation:** A small runnable path explains the public concepts without exposing framework internals. Any concept that remains hard to explain is recorded for review.
- **Verification:** Targeted tests, type checks, formatting, lint, builds, and applicable smoke checks pass. Environment-dependent checks and skipped deployment evidence are listed.

## Branch policy

`experimental/example-apps-base` is the shared integration branch for this programme. Application branches and focused refactor branches start from its latest accepted commit. Keep one writer in each worktree. Preserve source application branches until their useful contracts and tests exist on the foundation.

Integrate only reviewable commits that meet the applicable gates. Do not merge old application histories to import a small primitive. Port the required behavior with focused tests. Publication, deployment, and integration into `main` remain separate decisions.

## First portfolio

### Hex Domination

Hex Domination is the deepest product proof. Its existing Risk branches provide evidence for event-sourced commands, replay-safe board state, generation rebuilds, private action streams, browser materialization, timers, external agent play, and one-game-per-Durable-Object placement. Port narrow product slices and generic recovery laws. Keep its historic branch lineage separate.

### Issue tracker

`examples/issue-tracker-projections` is the first application on this foundation. It proves issue detail projection, dynamic project fan-in, chained causal coverage, browser reconstruction, optimistic display, repair, and an Alchemy v2 topology. Use it as the current executable reference while the application-facing API is simplified.

### Hacker News

The older Hacker News branch supplies a fixture, interface, reactive query, browser mirror, and offline smoke-test evidence. Rebuild that product slice on this foundation. Use it to test restart behavior, alternate generations, browser integration, and the documented State projection surface.

## Current evidence limits

Recovery of derived State still scans complete output history. Snapshot and checkpoint compaction remain future work.

The issue tracker has local, SQLite, fetch-client, build, and structural architecture evidence. Its live Cloudflare deployment has not been verified. The older Hex Domination and Hacker News applications use earlier library surfaces and cannot serve as direct compatibility proof.

Generic stateful-recovery fixes found on `risk-demo-primitives` are not present on this foundation. They include validation of recovered application state before source reads, validation after duplicate reconciliation, and fold-before-commit behavior. Port these laws into the current Effect-native State projection path before treating it as the application facade.
