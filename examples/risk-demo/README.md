# Streamsy Risk Demo — kernel, materializer, and durable command API (Batches 1–3)

A **deterministic, headless** event-sourced kernel for the Streamsy Risk demo. It is the
executable specification the later batches build on: canonical domain events are the source of
truth, and both the command-side decision model and the read-side board projection are pure folds
over those events.

There is **no turn-notification stream, agent harness, or UI** here yet — those arrive in
Batches 4–5. Batch 2 adds the replay-safe **board projection materializer**; Batch 3 adds the
durable **command & capability REST API**.

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
| Legal actions       | `legal-actions.ts`, `decision.ts`  | Structured agent affordances + decision context    |
| Command API         | `server/*.ts`                      | Durable REST command/capability API (Batch 3)      |

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

## Batch 3 — durable command & capability API

`server/` is a headless HTTP API (a web-standard `buildApp(deps)` fetch handler, hosted by
`server/index.ts` under Bun). Endpoints:

| Method | Path                                  | Auth   | Purpose                                                        |
| ------ | ------------------------------------- | ------ | -------------------------------------------------------------- |
| `POST` | `/v1/games`                           | —      | Create a game; returns host player + one-time host capability. |
| `POST` | `/v1/games/{id}/players`              | —      | Join; returns player + one-time player capability.             |
| `POST` | `/v1/games/{id}/start`                | host   | Start the game.                                                |
| `GET`  | `/v1/games/{id}`                      | —      | Metadata & status.                                             |
| `GET`  | `/v1/games/{id}/board`                | —      | Projected board + canonical `sourceThroughOffset`.             |
| `GET`  | `/v1/games/{id}/decision`             | player | Fresh turn/board/legal-action context.                         |
| `POST` | `/v1/games/{id}/commands`             | player | Submit a typed command (idempotent by `commandId`).            |
| `GET`  | `/v1/games/{id}/commands/{commandId}` | player | Recover an accepted/rejected result.                           |
| `GET`  | `/openapi.json`                       | —      | OpenAPI 3.1 + JSON Schemas for commands/acks/decision/errors.  |

Guarantees:

- **Authoritative decision loop** — every mutation authenticates a bearer capability into
  `{gameId, playerId, role}`, then folds canonical Streamsy history to its exact head, dedupes the
  `commandId` before rolling dice, validates with the Batch 1 kernel, resolves randomness once, and
  CAS-appends the event batch at the folded head (`expectedOffset`), refolding/retrying on conflict.
  Acks carry the real canonical `sourceStreamId` + committed `sourceOffset` (not positional indexes).
- **Idempotency** — a retried `commandId` returns the original events/offset/dice with no second
  append; idempotency is anchored in the canonical stream, so it holds even after a crash that lost
  the command-log row. A `commandId` reused with a different payload is rejected `COMMAND_ID_REUSED`.
- **Capability security** — a token is `rsk_<tokenId>_<secret>`; only the tokenId and a SHA-256
  verifier hash are stored, never the raw token, and verification is constant-time. Tokens never
  enter canonical events, the projection, or responses after issuance. A capability is scoped to one
  `{gameId, playerId}`; the acting player id is always derived from the token.
- **Durability** — one SQLite database (`@streamsy/storage-sqlite`) holds the canonical event
  streams, the board projection streams, and the capability/game/command tables together, so
  everything survives restart.

Tests: `src/api.test.ts` (vitest, storage-agnostic) covers authz isolation, happy path,
idempotent retry + recovery, `COMMAND_ID_REUSED`, stale-turn/illegal-phase codes, board catch-up to
the ack offset, a concurrent-command CAS race, OpenAPI discovery, and raw-token secrecy.
`server/persistence.test.ts` (`bun test`, real temp SQLite file) proves events, projection, command
recovery, and capability verifiers all survive a restart. `scripts/http-smoke.ts` drives the real
server end-to-end over HTTP including a kill/respawn against the same database file.

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
bun run build                                # build @streamsy/* dists (needed by materializer/API)
bun run --cwd examples/risk-demo test        # vitest suite (kernel + materializer + API)
bun run --cwd examples/risk-demo test:sqlite # bun test: SQLite durability + restart proof
bun run --cwd examples/risk-demo smoke:http  # spawn real server, drive HTTP, restart
bun run --cwd examples/risk-demo typecheck   # tsc --noEmit

# Serve (SQLite file for durability; :memory: by default):
DB_PATH=./risk.sqlite PORT=1339 bun run --cwd examples/risk-demo start
```

> `src/**` tests run under vitest and stay storage-agnostic (in-memory Streamsy + stores).
> `bun:sqlite`-backed tests live under `server/**` and run with `bun test`.
