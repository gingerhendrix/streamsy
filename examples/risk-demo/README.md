# Streamsy Risk Demo — kernel, materializer, command API, turn streams, clients & rebuild (Batches 1–5)

A **deterministic, headless** event-sourced kernel for the Streamsy Risk demo. It is the
executable specification the later batches build on: canonical domain events are the source of
truth, and both the command-side decision model and the read-side board projection are pure folds
over those events.

Batch 2 adds the replay-safe **board projection materializer**; Batch 3 adds the durable
**command & capability REST API**; Batch 4 adds **per-player turn-notification streams**, a
**coding-agent harness**, and a **React board**. Batch 5 adds **projection generation
rebuild/cutover**, a causal **`syncedThrough(ack)`** helper, and a deterministic **signature-demo
proof** that emits machine-readable traces for the accompanying article.

## What it provides

| Piece               | Module                                         | Role                                               |
| ------------------- | ---------------------------------------------- | -------------------------------------------------- |
| Fixed map + ruleset | `map.ts`                                       | Six-territory `demo-map-v1`, `risk-demo-v1` limits |
| Canonical events    | `events.ts`                                    | Immutable domain facts (the source of truth)       |
| Commands            | `commands.ts`                                  | Command envelopes + stable `RiskErrorCode`s        |
| Injected RNG        | `rng.ts`                                       | `Rng` seam + deterministic `mulberry32` seed       |
| Dice                | `dice.ts`                                      | Standard single-throw combat resolution            |
| Aggregate fold      | `aggregate.ts`                                 | Authoritative decision model (`foldAggregate`)     |
| Decision/validation | `decide.ts`                                    | Pure `decide(state, command, rng)`                 |
| Command binding     | `server/command-service.ts`                    | Risk fold/decide bound to Streamsy `command-log`   |
| Kernel test driver  | `testkit.ts`                                   | In-memory helpers used only by tests and proofs    |
| Board projection    | `projection.ts`                                | Independent query-shaped read model + equivalence  |
| Board materializer  | `materializer/board-projection.ts`             | Streamsy-backed projection adapter (Batch 2)       |
| Legal actions       | `legal-actions.ts`, `decision.ts`              | Structured agent affordances + decision context    |
| Command API         | `server/*.ts`                                  | Durable REST command/capability API (Batch 3)      |
| Turn notifications  | `server/turn-notifier.ts`                      | Per-player durable wake streams (Batch 4)          |
| Agent harness       | `server/agent.ts`, `scripts/agent.ts`          | HTTP-only coding-agent player (Batch 4)            |
| React board         | `public/`, `src/ui/*`                          | Playable/spectator board from the projection (B4)  |
| Generation rebuild  | `server/rebuild.ts`, `scripts/rebuild.ts`      | Rebuild + durable cutover of the board (B5)        |
| Causal wait         | `server/board-sync.ts`                         | `syncedThrough(ack)` board reconciliation (B5)     |
| Signature proof     | `server/signature-demo.ts`, `scripts/proof.ts` | Deterministic end-to-end demo + traces (B5)        |

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

## Batch 4 — turn streams, agent harness, and board

**Per-player turn notifications** (`server/turn-notifier.ts`): a derived, rebuildable fan-out of
canonical history into one durable Streamsy stream per player,
`games/<id>/players/<pid>/turns`, read at `GET /v1/games/{id}/players/me/turns` (player capability,
`?offset=<cursor>&wait=<ms>` for durable-cursor resume + long-poll). A `TurnAvailable` wake is
produced exactly when control passes to a player (`GameStarted` → first player; each `TurnEnded` →
next player), carrying `turnId`, `round`, and the canonical `causedBySourceOffset`. Each player's
stream is appended under producer identity `risk-turns:<gameId>:<playerId>` with `producerSeq` = the
player's wake ordinal, so a crash/restart/rebuild re-derives the same wakes and Streamsy classifies
re-appends `duplicate` — no second notification. Wakes are hints only: a delayed/duplicate/stale
wake is safe because acting on a stale `turnId` is rejected (`STALE_TURN`/`NOT_YOUR_TURN`) and every
command is revalidated against canonical history. `/me/turns` derives the player from the token, so
one capability can never read another player's stream. This uses the append/producer/CAS primitives
directly rather than `ProjectionRuntime` because wakes are sparse and fan out per player.

**Coding-agent harness** (`server/agent.ts`, runnable via `scripts/agent.ts`): follows its turn
stream from a persisted cursor, on wake fetches fresh `/decision`, chooses from structured
`legalActions` with a deterministic strategy (reinforce a frontier → attack forward → end turn), and
submits stable-`commandId` commands until control passes. `commandId` is derived from the observed
board state (`playerId:turnId:<fingerprint>`), so a resume from the saved cursor re-derives the same
id for an un-committed action (idempotent) and a fresh id once the board changes — only the cursor
needs to persist. It plays using ONLY the HTTP resources + turn stream, never the kernel.

**React board** (`public/index.html`, `src/ui/*`): a modest playable/spectator board served by the
Bun server (`GET /` → bundled SPA; API under `/v1/*`). It renders the fixed six-territory map with
owner colours + army counts and game/turn/phase, driven only by `GET /board` (polled), with
join/start/action controls generated from `/decision`. Two tabs converge by polling.

Tests: `src/turns.test.ts` (wake targeting, cursor resume, replay idempotency, stale-wake safety),
`src/agent.test.ts` (a complete agent-only game over HTTP, restart-from-cursor resume, duplicate-wake
tolerance), plus `server/persistence.test.ts` (turn-stream cursor resume + rebuild across a real
SQLite restart) and `scripts/http-smoke.ts` (turn-stream wake + SPA render over the real server).

## Batch 5 — generation rebuild/cutover, causal wait, and the signature proof

**Projection generations + durable cutover** (`server/rebuild.ts`, `scripts/rebuild.ts`). Each game
has a durable **active board-generation pointer** (`risk_games.generation`, mirrored in a
`risk_generations` catalogue). A generation is a _separate, rebuildable_ Durable State stream
(`games/<id>/projections/board/<generation>`); board reads always resolve the active generation.
`rebuildBoardGeneration(deps, gameId)`:

- creates a fresh generation and **replays the whole canonical log** into it through the same
  replay-safe `ProjectionRuntime`, catching up to the canonical head;
- **verifies** the rebuilt board against the authoritative aggregate fold — logical board
  equivalence _and_ an identical canonical `sourceThroughOffset`;
- only then **atomically cuts over** the active pointer (retire old → activate new → repoint the
  game, one SQLite transaction);
- on verification failure it **leaves the old generation active and usable** and marks the new one
  `failed`; **old generations are retained, never deleted**, so a cutover is reversible.

It is invokable host-side (not in product UI) via the CLI:
`DB_PATH=./risk.sqlite bun run scripts/rebuild.ts <gameId> [targetGeneration]` — exits `0` on
cutover, non-zero if verification fails (active generation unchanged).

**Causal wait** (`server/board-sync.ts`). A command ack names the _canonical source_ stream/offset;
the board projection may lag. `syncedThrough(call, gameId, ack)` / `waitForBoardThrough(readBoard,
ack)` polls the board until `ProjectionMeta.sourceStreamId` matches the ack's stream **and**
`sourceThroughOffset >= ack.sourceOffset` under `compareOffsets`. It compares offsets **only within
the same stream** — a mismatched source stream is a hard `BoardSyncError("wrong-stream")`, never a
meaningless cross-stream comparison — and supports immediate/delayed/timeout/abort behaviour.

**Signature proof** (`server/signature-demo.ts`, `scripts/proof.ts`). One deterministic scenario
over real Streamsy storage + the HTTP command/turn resources that emits structured **JSONL trace**
events and a machine-readable summary, then verifies the demo invariants and exits `0`. It
demonstrates, as one coherent run: a durable two-agent HTTP-only game with turn notifications; an
agent notification cursor persisted, reloaded, and resumed; an accepted attack retained with its
recorded dice + canonical ack; causal `syncedThrough`; an idempotent retry (`duplicate`); a
`STALE_TURN` rejection; a crash injected **immediately after a projection output commit** proven not
to double-apply on recovery; a fresh-generation rebuild verified + cut over with the old generation
retained; and the agent-only game finishing with a winner + final watermark. Trace output is never
committed — it prints to stdout (or `TRACE_FILE`).

**How the no-double-apply claim is proven.** The crashed generation is decoded from its committed
bytes (`analyzeProjectionOutput`): every transition writes exactly one `projectionMeta` row carrying
its applied `sourceSeq`, so the proof compares the recovered stream against a **clean control
generation** built from the same canonical log and reports the discriminating fields —
`canonicalEvents`, `expectedTransitions`/`actualTransitions`,
`expectedOutputMessages`/`actualOutputMessages`, `duplicateSourceSeqs`, and `watermarkEqual`.
`doubleApplied` is `detectDoubleApply(actual, control)`: true if any `sourceSeq` repeats or the
transition/message counts exceed the control. A representative `crash-recovery` trace line:

```json
{
  "step": "crash-recovery",
  "crashAtSeq": 19,
  "canonicalEvents": 39,
  "committedAtCrash": 64,
  "expectedTransitions": 39,
  "actualTransitions": 39,
  "expectedOutputMessages": 115,
  "actualOutputMessages": 115,
  "duplicateSourceSeqs": [],
  "doubleApplied": false,
  "boardEqual": true,
  "watermarkEqual": true
}
```

```bash
bun run --cwd examples/risk-demo proof                     # memory Streamsy storage, prints JSONL + summary
DB_PATH=./proof.sqlite bun run --cwd examples/risk-demo proof   # real SQLite durability
```

Tests: `src/rebuild.test.ts` (equivalence, cutover, rollback-on-failure, retention, chained
rebuilds), `server/rebuild-persistence.test.ts` (`bun test`, cutover survives a SQLite restart),
`src/board-sync.test.ts` (immediate/delayed/null/wrong-stream/timeout/abort), `src/long-poll.test.ts`
(deterministic long-poll wake), and `src/signature-demo.test.ts` (the full sequence, determinism, and
a **negative control** that genuinely double-applies a transition and asserts `detectDoubleApply`
flags it — so `doubleApplied=false` is a real claim, not a tautology).

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
bun run --cwd examples/risk-demo test        # vitest suite (kernel, materializer, API, turns, agent, rebuild, proof)
bun run --cwd examples/risk-demo test:sqlite # bun test: SQLite durability + restart + cutover proofs
bun run --cwd examples/risk-demo smoke:http  # spawn real server, drive HTTP + SPA, restart
bun run --cwd examples/risk-demo proof       # deterministic signature demo (JSONL trace + summary, exits 0)
bun run --cwd examples/risk-demo typecheck   # tsc --noEmit

# Serve the API + React board (SQLite file for durability; :memory: by default):
DB_PATH=./risk.sqlite PORT=1339 bun run --cwd examples/risk-demo start

# Run a standalone coding agent against a live server (file-persisted cursor):
GAME_ID=game_xxx PLAYER_ID=p_xxx PLAYER_TOKEN=rsk_... CURSOR_FILE=./p1.cursor \
  bun run --cwd examples/risk-demo agent

# Two agents against one live server (each with its own persisted cursor):
DB_PATH=./risk.sqlite PORT=1339 bun run --cwd examples/risk-demo start &   # terminal 1
#   POST /v1/games + /players to obtain gameId + two player capabilities, then:
GAME_ID=game_xxx PLAYER_ID=p_a PLAYER_TOKEN=rsk_a... CURSOR_FILE=./a.cursor bun run --cwd examples/risk-demo agent &
GAME_ID=game_xxx PLAYER_ID=p_b PLAYER_TOKEN=rsk_b... CURSOR_FILE=./b.cursor bun run --cwd examples/risk-demo agent &

# Rebuild a game's board into a fresh generation and cut over (host/admin CLI):
DB_PATH=./risk.sqlite bun run --cwd examples/risk-demo rebuild <gameId> [targetGeneration]
#   exit 0 → cut over (prints from→to generation + retained list); non-zero → verification failed,
#   active generation unchanged. Old generations are retained.

# Signature proof against real SQLite, optionally writing the trace to a file (never committed):
DB_PATH=./proof.sqlite TRACE_FILE=./trace.jsonl bun run --cwd examples/risk-demo proof
```

> `src/**` tests run under vitest and stay storage-agnostic (in-memory Streamsy + stores).
> `bun:sqlite`-backed tests live under `server/**` and run with `bun test`.

## Acceptance matrix (concept tests → concrete evidence)

| Concept acceptance test                                    | Where it is proven                                                                                                                                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Canonical deterministic replay                             | `src/kernel.test.ts`; `src/materializer/board-projection.test.ts`                                                                                                        |
| Projection ⇄ aggregate equivalence                         | `src/materializer/board-projection.test.ts`; `src/kernel.test.ts`                                                                                                        |
| Atomic state + watermark                                   | `src/materializer/board-projection.test.ts` (self-contained reload); `packages/experimental/src/projection/runtime.test.ts`                                              |
| Crash immediately after output commit (no double-apply)    | `src/signature-demo.test.ts` (control-build comparison + duplicate-transition negative control); `scripts/proof.ts` (`crash-recovery` trace); `board-projection.test.ts` |
| Ambiguous append retry classified `duplicate`              | `packages/experimental/src/projection/runtime.test.ts`; `server/turn-notifier.ts` rebuild path (`src/turns.test.ts`)                                                     |
| Concurrent materializers (single transition)               | `packages/experimental/src/projection/runtime.test.ts` (CAS/epoch)                                                                                                       |
| Poison event halts visibly                                 | `packages/experimental/src/projection/runtime.test.ts`                                                                                                                   |
| Projection catch-up gap-free                               | `src/materializer/board-projection.test.ts`                                                                                                                              |
| Generation rebuild equivalence + watermark                 | `src/rebuild.test.ts`; `server/rebuild-persistence.test.ts`; `scripts/proof.ts` (`generation-rebuild`)                                                                   |
| Durable cutover + rollback-on-failure + retention          | `src/rebuild.test.ts`; `server/rebuild-persistence.test.ts` (restart)                                                                                                    |
| Causal `syncedThrough(ack)`                                | `src/board-sync.test.ts`; `src/signature-demo.test.ts` (`causalWait`)                                                                                                    |
| Command CAS race                                           | `src/api.test.ts`                                                                                                                                                        |
| Command idempotency (no double append/roll)                | `src/api.test.ts`; `src/signature-demo.test.ts` (`idempotentRetry`)                                                                                                      |
| Authorization isolation                                    | `src/api.test.ts`; `scripts/http-smoke.ts`                                                                                                                               |
| Restart (events/projections/watermarks/tokens/generations) | `server/persistence.test.ts`; `server/rebuild-persistence.test.ts`; `scripts/http-smoke.ts`                                                                              |
| Agent-only complete game                                   | `src/agent.test.ts`; `src/signature-demo.test.ts`                                                                                                                        |
| Turn-stream cursor resume                                  | `src/agent.test.ts`; `server/persistence.test.ts`; `signature-demo` (`agent-cursor-restart`)                                                                             |
| Stale/duplicate wake safety                                | `src/turns.test.ts`; `src/signature-demo.test.ts` (`STALE_TURN`)                                                                                                         |
| Long-poll wake                                             | `src/long-poll.test.ts`                                                                                                                                                  |
| Notification replay idempotency                            | `src/turns.test.ts`; `server/persistence.test.ts`                                                                                                                        |
| Capability isolation on turn streams                       | `src/turns.test.ts`                                                                                                                                                      |

## Remaining limitations (Batch 5)

- **Cutover fencing is by generation identity, not epoch.** Each generation is a distinct stream
  with a distinct producer id, so a stale writer for the old generation cannot corrupt the new one;
  within a generation, output CAS (`expectedOffset`) plus producer-seq already prevent
  double-apply. An explicit `producerEpoch` bump on cutover is available in `ProjectionRuntime` but
  not wired, since the generation boundary makes it unnecessary here.
- **Board reads are catch-up-on-read, not a long-lived background follower.** `GET /board` and the
  rebuild advance the projection synchronously; there is no always-on materializer daemon. The
  `follow()` loop exists in the runtime and is exercised by long-poll, but the demo does not run it
  as a service.
- **The signature proof's crash-injection generation is a dedicated `crash-demo` stream** built from
  the completed canonical log, so the fault window is deterministic and isolated from the live board;
  the HTTP app itself still has no fault-injection seam (fault hooks live only on `ProjectionRuntime`).
- **The two-agent runner is scripted, not a single supervisor process.** The automated agent-only
  game runs in-process (`src/agent.test.ts`, `signature-demo`); the standalone runner is two
  `scripts/agent.ts` processes against a live server (documented above), not one orchestrator.
- **The React board has typecheck + served-HTML smoke coverage, not a browser interaction test.**
  (Unchanged from Batch 4.)
- **`syncedThrough` polls; it does not yet subscribe** to a board long-poll. Correctness is
  unaffected (it reads the durable watermark), but a follow-based variant would remove the poll
  interval.
