# Streamsy Risk Demo — game kernel (Batch 1)

A **deterministic, headless** event-sourced kernel for the Streamsy Risk demo. It is the
executable specification the later batches build on: canonical domain events are the source of
truth, and both the command-side decision model and the read-side board projection are pure folds
over those events.

There is **no persistence, materializer, REST, capability, or UI** here yet — those arrive in
Batches 2–5. This package deliberately stays a pure domain library so the projection/materializer
machinery can be driven against a stable, tested specification.

## What it provides

| Piece | Module | Role |
|---|---|---|
| Fixed map + ruleset | `map.ts` | Six-territory `demo-map-v1`, `risk-demo-v1` limits |
| Canonical events | `events.ts` | Immutable domain facts (the source of truth) |
| Commands | `commands.ts` | Command envelopes + stable `RiskErrorCode`s |
| Injected RNG | `rng.ts` | `Rng` seam + deterministic `mulberry32` seed |
| Dice | `dice.ts` | Standard single-throw combat resolution |
| Aggregate fold | `aggregate.ts` | Authoritative decision model (`foldAggregate`) |
| Decision/validation | `decide.ts` | Pure `decide(state, command, rng)` |
| Command driver | `engine.ts` | Fold → dedupe → decide → append → acknowledge |
| Board projection | `projection.ts` | Independent query-shaped read model + equivalence |

## Core guarantees (all covered by tests)

- **Deterministic replay** — folding the same events (including recorded dice) always yields the
  same state; the projection and aggregate agree at every source offset.
- **Randomness recorded once** — dice, the territory deal, and turn order are resolved by the
  command side and written into events. Replay never touches an `Rng`.
- **`commandId` idempotency** — a retried command returns its original outcome (including the
  original dice) without re-deciding or re-rolling.
- **`turnId` precondition** — a stale observed turn is rejected (`STALE_TURN` / `NOT_YOUR_TURN`)
  before it can affect a later turn.

## Rules (`risk-demo-v1`, intentionally bounded)

- 2–4 players, sequential turns on a fixed six-territory map.
- Setup is resolved deterministically at `start-game`: territories are dealt round-robin (one army
  each) and turn order is shuffled via the injected RNG.
- Each turn: **reinforce** (place `max(3, floor(owned/3))` armies, no continent bonuses) →
  **attack** (single-throw dice, ties to defender) → optional single **fortify** → **end-turn**.
- Elimination on losing your last territory; last player standing wins.
- No cards, missions, alliances, or timers.

## Deliberate deviations from the design docs

- **No `occurredAt` timestamps** on events. Nothing in the folded state depends on wall-clock time,
  and omitting it keeps `decide` pure without a clock injection. Timestamps and the real stream
  offset become the command/persistence layer's concern in Batch 3; here "source offset" is the
  positional index of an event in the log.
- **Setup collapsed into `GameStarted`.** There is no separate interactive claim phase in v1.

## Running

```bash
bun run --cwd examples/risk-demo test        # vitest suite
bun run --cwd examples/risk-demo typecheck   # tsc --noEmit
```
