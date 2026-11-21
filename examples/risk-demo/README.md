# Hex Domination

Hex Domination is an event-sourced territory game for two to four players. Play in a browser or watch two scripted bots use the same command and board routes. The server records the generated hex map in `GameStarted`, so replay uses the map the players saw.

## Play on Bun

From the repository root, install and build the workspace packages. Use a new SQLite file to keep your games across restarts:

```bash
bun install --frozen-lockfile
bun run build
DB_PATH="$HOME/hex-domination.sqlite" bun run --cwd examples/risk-demo start
```

Open <http://127.0.0.1:1339>. Create a game and share its invite link with another browser profile. Join from that profile, then start the game from the host's lobby. To reinforce, select your territories until you have placed the whole pool, then select **Finish reinforcements**.

The server listens on loopback. Set `PORT` to change the port. If you omit `DB_PATH`, the database is in memory and stops with the server. Use `dev` for Bun hot reload and `start` when driving the UI with browser automation.

To watch scripted bots, run:

```bash
bun run --cwd examples/risk-demo demo
```

Open the printed spectator URL. The demo creates a disposable database and starts the bots after a short lead-in. Stop it with Ctrl-C.

## Command and stream contracts

- `games/<gameId>/events` is the canonical log. Each item is a command batch. The service folds the log, checks `commandId`, and appends with `expectedOffset`. After an offset conflict, it reads the new head and applies the rules again. The payload hash in an accepted batch lets a retry recover the same acknowledgement even if the server stopped before saving its metadata row.
- `games/<gameId>/projections/board/<generation>` holds Durable State rows. The initial generation is `board1`. `Projection.mapAccum` and `Commit.producer` persist board state and its input cursor. The projector emits the command's `txid` on the final `projectionMeta` row, after the changed board rows. `/decision` reads only the canonical prefix named by the board's `sourceThroughOffset`.
- `games/<gameId>/players/<playerId>/actions` holds the player's derived action messages. The notifier checks the durable prefix after a producer duplicate or a conflict. Only the bearer-scoped `/v1/games/<gameId>/players/me/actions` endpoint serves these messages. It defaults to SSE and accepts `Accept: application/json` for an immediate page. The browser's read-only `/streams` endpoint exposes only the active board generation.

See [the agent API](docs/agent-api.md) for the seat bootstrap and command shapes. The [standalone seat launcher](external-agent/README.md) is optional. The local bot demo does not need a model provider.

## Rebuild a board generation

Stop the game server first. With the same file that the server used, run:

```bash
DB_PATH="$HOME/hex-domination.sqlite" bun run --cwd examples/risk-demo rebuild <game-id>
```

The command replays canonical history into a fresh generation. It compares the candidate's materialized rows and source watermark against the projection checkpoint and the authoritative aggregate. Only then does it commit the active generation pointer in SQLite. A failed candidate stays recorded as failed. The old generation stays active and its stream is retained. Restart the server to serve the verified generation.

`hex-domination:board-4` identifies the current output format. This release uses current Streamsy SQLite storage, private command batches, and new board checkpoints. It does not open or migrate the old app's SQLite databases, actions, or board streams. Tests replay the historical event fixtures from `examples/hex-board` after batching events by command. That example remains independently runnable.

## Verify the app

```bash
bun run --cwd examples/risk-demo typecheck
bun run --cwd examples/risk-demo test
bun run --cwd examples/risk-demo test:sqlite
bun run --cwd examples/risk-demo smoke:http
```

The HTTP smoke starts the real Bun server on loopback with disposable SQLite state, submits commands, reads the streams, and restarts the process. The Worker and Durable Object host are outside this Bun port.
