# Streamsy Hex Domination demo

An event-sourced territory game built to demonstrate Streamsy’s durable command,
projection, and per-player action-stream patterns. A seeded generator creates a
connected hex map for two to four players; the canonical `GameStarted` event
records the complete generated map so replay never depends on running the
generator again.

## Run it

From the repository root:

```sh
bun install
bun run build
bun run --cwd examples/risk-demo demo
```

For local development:

```sh
bun run --cwd examples/risk-demo dev
```

The one-command demo starts the server, creates a seeded bot-versus-bot game,
prints the spectator URL, and plays through to a winner.

## Product model

- The front page is an invitation and one command. Creating a game asks for
  nothing: the server issues a provisional seat name, and every naming decision
  is made in the lobby, where the roster is visible while you make it.
- The lobby is where the roster is assembled. Anyone can share the invite link;
  a visitor holding no seat can join; a person holding one can rename it or give
  it up, behind a confirmation; the creator can invite agents and start.
- Renaming and leaving are canonical, lobby-only commands (`PlayerRenamed`,
  `PlayerLeft`) — the muster roll, the move feed, and an agent's own briefing all
  read the recorded name, so none of them can be a client-side label.
- Hosting is a capability, not a seat. A creator who gives up its seat keeps the
  lobby it opened and watches the game from there, which is how an
  agent-versus-agent game is set up: invite two agents, then leave.
- Two to four players on a seeded procedural hex map.
- Reinforcement is submitted as one complete allocation.
- Each attack is a declaration followed by a defender roll.
- Human and bot defenders receive a timed action; external-agent defence is
  resolved by the server so an agent never has to make a dice-only decision.
- A capture opens a mandatory occupation before play can continue.
- Fortification can move through any connected path of owned countries.
- Terrain is visual character only; it does not affect the rules.

`procedural-hex-v1` and `hex-generator-v2` are replay provenance identifiers,
not alternate game modes. Changing the generator in a way that changes
output requires a new generator identifier because generated maps are durable
facts. `hex-generator-v1` remains a valid identifier for games generated
before continent sizing was skewed; those games replay against their own
recorded snapshot and keep the bonuses they were dealt.

## Architecture

The canonical stream is `games/<gameId>/events`. Commands fold that history,
validate against the resulting aggregate, and append accepted events with an
expected-head precondition. Stable `commandId` values make retries idempotent.

The browser reads an independent board projection at:

```text
games/<gameId>/projections/board/<generation>
```

The initial generation is `board1`. Every projection transaction embeds its
canonical `sourceThroughOffset`, so the UI and decision resource can name the
exact history prefix they represent.

Per-player action-required messages are derived into private streams at:

```text
games/<gameId>/players/<playerId>/actions
```

They are exposed only through the bearer-authenticated actions endpoint. The
public stream facade exposes the active board generation, never canonical events,
command logs, retired generations, or private action streams.

The main source areas are:

- `src/domain`: commands, events, aggregate, map generation, and decision rules.
- `src/board`: independent board projection and causal transaction identifiers.
- `src/application`: player-relative decisions and API types.
- `server/game`: command service, projection materialization, action derivation,
  defence timers, and rebuild verification.
- `server/http`: capability-scoped routes and the OpenAPI contract.
- `src/ui`: the lobby and live Hex Domination board.

## Projection rebuilds

Projection generations are an intentional durability feature. The rebuild
command replays canonical history into a fresh generation, verifies both logical
board equality and the canonical watermark, and only then atomically changes the
active generation pointer. A failed verification leaves the current generation
active. Previous generations remain recorded for inspection.

```sh
bun run --cwd examples/risk-demo rebuild -- <game-id>
```

## Lobby API

Beyond create, join, and start, the roster is edited with two capability-scoped
routes, both lobby-only:

```text
PATCH  /v1/games/:gameId/players/:playerId   { "name": "..." }
DELETE /v1/games/:gameId/players/me
```

The rename admits exactly two callers — the seat's own capability, and the host
for an agent seat it opened — and never a host on another person's seat. The
leave is scoped to `me` rather than a seat id, so no shape of the request removes
somebody else, and it refuses a seat an agent is playing.

## Agent API

The host opens an agent seat with:

```text
POST /v1/games/:gameId/agent-seats
```

The public request vocabulary uses `controller: "agent"`; canonical events use
`controller: "external-agent"`. This is the deliberate API-to-domain boundary.
The returned one-time bootstrap includes the four playing resources:

- `GET /v1/games/:gameId/map`
- `GET /v1/games/:gameId/players/me/actions?offset=&wait=`
- `GET /v1/games/:gameId/decision`
- `POST /v1/games/:gameId/commands`

See [docs/agent-api.md](docs/agent-api.md) and
[external-agent/README.md](external-agent/README.md).

## Checks

```sh
bun run --cwd examples/risk-demo typecheck
bun run --cwd examples/risk-demo test
bun run --cwd examples/risk-demo test:sqlite
bun run --cwd examples/risk-demo build:cloudflare
bun run --cwd examples/risk-demo smoke:http
```

When driving the UI from a browser automation harness, run it against
`bun run --cwd examples/risk-demo start` rather than `dev`: Bun's dev-mode
`<bun-hmr>` overlay is a full-viewport fixed element at the top of the stacking
order, so `elementFromPoint` returns it everywhere and synthesized clicks land on
it silently. Against the dev server, remove that element from the DOM first.

The Vitest suite covers the kernel, generated-map invariants, projection
equivalence, HTTP contract, action streams, defence timing, scripted bots,
external-agent launcher, UI presentation, and rebuild cutover. The Bun SQLite
suite proves command, capability, projection, action cursor, and generation
durability across process restarts.
