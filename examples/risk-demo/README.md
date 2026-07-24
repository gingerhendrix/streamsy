# Streamsy Risk

Streamsy Risk is a live, event-sourced strategy game played by two HTTP-only agents while you
watch the board update in a browser. It demonstrates a ready-made Streamsy application: durable
commands, deterministic decisions, replay-safe projections, and official Stream DB + TanStack DB
browser sync over Streamsy's protocol.

## Run the live demo

From the repository root:

```bash
bun run demo:risk
```

Or from this directory, run `bun run demo`. The command builds missing workspace outputs, chooses a
free port, starts a SQLite-backed server, creates and starts a game, and runs Ada and Bob as
in-process HTTP agents. Open the prominently printed spectator URL; the server and final board stay
available until Ctrl-C. Temporary SQLite data is removed on shutdown.

![The live Streamsy Risk spectator board](docs/risk-demo.png)

## Architecture

The demo keeps the Risk-specific application small by composing Streamsy primitives:

| Layer              | Risk module                                             | Streamsy role                                                                                                                        |
| ------------------ | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Kernel             | `src/domain/aggregate.ts`, `src/domain/decide.ts`       | Pure fold and decision function; injected RNG is recorded as events                                                                  |
| Command log        | `server/game/command-service.ts`                        | `@streamsy/experimental/command` provides idempotent command submission over the canonical event stream                              |
| Board projection   | `src/board/board-projection.ts`, `server/game/board.ts` | `ProjectionRuntime` materializes an independently rebuildable, causally watermarked board stream                                     |
| Turn notifications | `server/game/turn-notifier.ts`                          | Derived per-player wake streams tell HTTP agents when to fetch a fresh decision                                                      |
| Browser sync       | `src/ui/board-stream-db.ts`                             | Official `@durable-streams/state` Stream DB consumes Streamsy's read-only protocol facade; TanStack DB live queries render the board |

```text
HTTP command → command log → canonical game events
                             ├─ replay-safe board projection → Stream DB → TanStack live queries
                             └─ derived turn streams → HTTP-only agents
```

Command POSTs use the REST API. Browser board state does not poll a REST snapshot: one
`createStreamDB` session consumes the active projection generation with long polling, and five
TanStack collections drive the React view. A small metadata refresh discovers projection-generation
cutovers; it does not drive board state.

## What to inspect

- `server/game/command-service.ts` binds the Risk fold/decide functions to the reusable command log.
- `src/board/board-projection.ts` declares the durable board schema and event-to-row mapping.
- `server/demo/agent.ts` follows turn streams, fetches structured legal actions, and submits stable
  `commandId`s using only published HTTP resources.
- `server/demo/signature-demo.ts` runs the complete deterministic guarantee proof.
- `src/ui/board-stream-db.ts` is the official Stream DB + TanStack DB integration.

### `risk-demo-v2` map kernel (in progress)

The v2 ruleset replaces the fixed six-territory board with a seeded procedural hex map.
The kernel is complete and tested but not yet wired into game creation — the v2 aggregate,
projection, and renderer arrive in later slices, so every game the server creates today is
still `risk-demo-v1`.

- `src/domain/hex-generator.ts` is `hex-generator-v1`: a pure, seeded generator producing a
  connected hex map with variable-sized countries, connected continents, and visual-only terrain.
- `src/domain/generator-rng.ts` provides the integer PRNG and its independent named substreams.
- `src/domain/setup-v2.ts` deals the board and allocates armies deterministically from the seed.
- `src/domain/events-v2.ts` records the seed on `GameCreated` and the whole map snapshot on
  `GameStarted`, so replay never re-runs the generator.

## Guarantees

- **Deterministic replay:** dice, territory allocation, and turn order are recorded once in canonical
  events. Replaying never calls the RNG.
- **Idempotent commands:** retrying a `commandId` returns the original outcome and source offset;
  reusing it for different input is rejected. Re-submit the same payload to recover after a lost
  response.
- **Turn preconditions:** stale or out-of-turn commands are rejected before they can affect a later
  turn.
- **Causal reads:** a command ack's canonical offset can be passed to `syncedThrough` to wait until
  the board watermark reaches that exact source position.
- **Crash-safe projection:** recovery resumes after the durable projection watermark, so a crash
  immediately after output commit does not double-apply a transition.
- **Verified rebuilds:** a new board generation is replayed and compared with the authoritative fold
  before atomic cutover; previous generations remain retained.
- **Live interoperability:** the official Stream DB client reads the active projection through
  Streamsy's protocol implementation, with no parallel browser synchronization path.

## Acceptance matrix

| Check                       | Command                                                     | Evidence                                                                                          |
| --------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Watchable product demo      | `bun run demo:risk`                                         | Fresh workspace outputs are bootstrapped; spectator URL is printed; agents play; Ctrl-C cleans up |
| Risk unit/integration suite | `bun run --cwd examples/risk-demo test`                     | Kernel, API, materializer, turn streams, agents, rebuild, Stream DB shaping, and proof tests      |
| SQLite durability           | `bun run --cwd examples/risk-demo test:sqlite`              | Persistence, cursor resume, duplicate command retry, and generation cutover survive restart       |
| Real HTTP smoke             | `bun run --cwd examples/risk-demo smoke:http`               | Server, SPA, auth, command/board flow, and SQLite restart                                         |
| Signature proof             | `bun run --cwd examples/risk-demo proof`                    | Recorded dice, duplicate retry, stale rejection, causal sync, crash recovery, rebuild, and winner |
| Static checks               | `bun run typecheck && bun run lint && bun run format:check` | Workspace build/types plus repository lint and format                                             |

## Proof, smoke, rebuild, and agent flows

Run the deterministic signature scenario in memory, or against a SQLite file:

```bash
bun run --cwd examples/risk-demo proof
DB_PATH=./proof.sqlite bun run --cwd examples/risk-demo proof
TRACE_FILE=./trace.jsonl bun run --cwd examples/risk-demo proof
```

The proof prints JSONL trace events and a machine-readable summary. It finishes an agent-only game
and verifies recorded attack dice, same-offset idempotent retry, `STALE_TURN`, causal board sync,
crash-after-output recovery without double application, and an equivalent v1 → v2 rebuild.

Exercise the real server and a SQLite restart:

```bash
bun run --cwd examples/risk-demo smoke:http
```

Run a server directly and rebuild a game's board generation:

```bash
DB_PATH=./risk.sqlite PORT=1339 bun run --cwd examples/risk-demo start
DB_PATH=./risk.sqlite bun run --cwd examples/risk-demo rebuild -- <gameId> [targetGeneration]
```

The rebuild creates a separate projection stream, catches it up to the canonical head, verifies its
logical board and watermark, and only then cuts over the durable active-generation pointer. A failed
verification leaves the old generation active.

Run a standalone restart-safe HTTP agent against a live game:

```bash
BASE_URL=http://localhost:1339 \
GAME_ID=game_xxx \
PLAYER_ID=p_xxx \
PLAYER_TOKEN=rsk_xxx \
CURSOR_FILE=./player.cursor \
bun run --cwd examples/risk-demo agent
```

The agent persists only its turn-stream cursor. On each wake it fetches a fresh `/decision`, chooses
from structured `legalActions`, and derives stable command IDs from the observed turn and board.

## Ruleset

`risk-demo-v1` deliberately fits a complete game into a short demo:

- 2–4 players on the fixed six-territory `demo-map-v1`.
- Setup deals territories round-robin and records the shuffled turn order.
- Each turn has reinforce, attack, optional fortify, and end-turn phases.
- Combat is a single dice throw with ties to the defender.
- Losing the last territory eliminates a player; the last player wins.
- No cards, missions, alliances, continent bonuses, or timers.

Events omit `occurredAt` because wall-clock time does not affect the fold. Setup is represented by a
single `GameStarted` event so the demo can focus on durable decisions, projections, and live state.
