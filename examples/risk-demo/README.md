# Streamsy Risk Demo — kernel + projection materializer (Batches 1–2)

A **deterministic, headless** event-sourced kernel for the Streamsy Risk demo. It is the
executable specification the later batches build on: canonical domain events are the source of
truth, and both the command-side decision model and the read-side board projection are pure folds
over those events.

There is **no REST, capability, turn stream, or UI** here yet — those arrive in Batches 3–5.
Batch 2 adds the replay-safe **board projection materializer** on top of the pure kernel.

## What it provides

| Piece               | Module                             | Role                                               |
| ------------------- | ---------------------------------- | -------------------------------------------------- |
| Fixed map + ruleset | `map.ts`                           | Six-territory `demo-map-v1`, `risk-demo-v1` limits |
| Canonical events    | `events.ts`                        | Immutable domain facts (the source of truth)       |
| Commands            | `commands.ts`                      | Command envelopes + stable `RiskErrorCode`s        |
| Injected RNG        | `rng.ts`                           | `Rng` seam + deterministic `mulberry32` seed       |
| Dice                | `dice.ts`                          | Standard single-throw combat resolution            |
| Aggregate fold      | `aggregate.ts`                     | Authoritative decision model (`foldAggregate`)     |
| Decision/validation | `decide.ts`                        | Pure `decide(state, command, rng)`                 |
| Command driver      | `engine.ts`                        | Fold → dedupe → decide → append → acknowledge      |
| Board projection    | `projection.ts`                    | Independent query-shaped read model + equivalence  |
| Board materializer  | `materializer/board-projection.ts` | Streamsy-backed projection adapter (Batch 2)       |

## Batch 2 — replay-safe board materializer

`materializer/board-projection.ts` drives the pure `projectEvent` reducer through the reusable
[`@streamsy/experimental/projection`](../../packages/experimental/README.md#projection-runtime)
runtime to materialize a **separate, causally-watermarked Durable State projection** from the
canonical event stream:

- canonical events live in a source stream (`games/<id>/events`); the board projection is a
  **separate** stream (`games/<id>/projections/board/<generation>`) — events are never collapsed
  into the board;
- each source event becomes **one atomic output transaction** of Durable-State change messages
  (`game`/`player`/`territory` upserts) plus a `projectionMeta` row embedding the canonical
  `sourceThroughOffset` and a resume snapshot — board changes and watermark commit together;
- the materializer resumes strictly after the durable watermark, so a crash right after an output
  commit never double-applies; replay-safe producer identity + `expectedOffset` CAS keep concurrent
  or ambiguous writers from double-applying;
- a reducer failure halts the projection visibly at the prior offset;
- a fresh **generation** replays the same canonical log into a new stream and reaches an equivalent
  board (rebuild).

Tests in `materializer/board-projection.test.ts` run this against a real in-memory Streamsy
protocol (`createMemoryStorageAdapter`) and assert projection⇄aggregate equivalence, crash-after-
commit recovery, incremental catch-up, and generation rebuild. The replay-safety primitives
themselves (atomic watermark, duplicate/CAS classification, poison halt) are proven in
`packages/experimental/src/projection/runtime.test.ts`.

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
  and omitting it keeps `decide` pure without a clock injection. Timestamps become the
  command/persistence layer's concern in Batch 3. In the pure kernel the "source offset" is the
  positional index of an event; once materialized off a Streamsy stream (Batch 2) it is the real
  stream offset, so the projection's embedded `sourceThroughOffset` is a genuine watermark.
- **Setup collapsed into `GameStarted`.** There is no separate interactive claim phase in v1.

## Running

```bash
bun run build                                # build @streamsy/* dists (needed by the materializer)
bun run --cwd examples/risk-demo test        # vitest suite (kernel + materializer)
bun run --cwd examples/risk-demo typecheck   # tsc --noEmit
```
