# Workerd host

The Worker host runs the same game code as the Bun server. `buildApp`, the stream declarations, the board projection, the command service, and the defence timers are shared. Only storage, routing, and the timer differ. This host is verified on local workerd through Miniflare. The public deployment runs at <https://hex-domination.streamsy.dev>.

## Run it

```bash
bun run build
bun run --cwd examples/risk-demo dev:worker
```

`dev:worker` builds the SPA into `dist/web` and the Worker into `dist/worker`, then serves both on <http://127.0.0.1:8791>. Set `PORT` to change the port. Durable Object state is kept in `WORKER_STATE`. Its default is `dist/worker-state`, which the next build deletes. Set `DEFENSE_TIMEOUT_MS` to shorten the 15-second defence window for local testing.

## Deploy

`alchemy.run.ts` declares the `hex-domination` stack: the `hex-domination-<stage>` Worker, the `GAME` Durable Object namespace for `GameDurableObject`, and `dist/web` as its assets. `deploy` builds the Worker bundle and runs Alchemy on it as it is (`bundle: false`).

```bash
bun run --cwd examples/risk-demo deploy --stage prod
```

Only the `prod` stage attaches `hex-domination.streamsy.dev` and turns off `workers.dev`. State is local (`Alchemy.localState()`), so run deploys from the worktree that owns `examples/risk-demo/.alchemy/`. The deployed Worker uses the 15-second defence window. It does not set `DEFENSE_TIMEOUT_MS`.

## Routing

| Path                                                              | Handled by                                                                          |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `POST /v1/games`                                                  | The edge mints `game_<24 hex>` and sends the request to that game's object.         |
| `/v1/games/<gameId>/...`                                          | The object named by `gameId`. Malformed ids answer 404 at the edge.                 |
| `/streams/games/<gameId>/projections/board/<generation>`          | The same object. It serves only the active generation.                              |
| `/healthz`, `/openapi.json`                                       | The edge.                                                                           |
| Other `/v1/`, `/streams/`, `/agent/`, `/agent-seat/`, `/__` paths | 404 at the edge.                                                                    |
| Everything else                                                   | The SPA from `dist/web`, with the SPA fallback for routes such as `/game/<gameId>`. |

The edge sets `x-risk-game-id` and replaces any client value. The object answers 409 when the header names another game. An object that was never created answers 404 for every route except create, and writes nothing.

## The game object

`GameDurableObject` composes `DurableObjectStorage` and `CheckpointStore` over the object's SQLite database. That database holds every stream of the game (canonical events, private action streams, and board generations) and the app tables (`risk_capabilities`, `risk_games`, `risk_commands`, and `risk_generations`). The tables match the Bun store. Rows are decoded with Schema. Generation activation is one `transactionSync`. A one-row `risk_object` table binds the object to its game id.

Capabilities, CAS appends, command receipts, and duplicate-payload reconciliation are unchanged from Bun. Another game's capability does not exist in this object, so it fails with 401 `UNAUTHORIZED`. The single-process Bun host answers 403 `WRONG_GAME` for the same request.

## Defence deadlines

Bun keeps a process timer for each pending defence. The game object uses its storage alarm:

1. A command that opens a defence moves the alarm to the canonical `defenseDeadlineAt`.
2. `alarm()` refolds canonical history with `DefenseTimers.recover`. It submits `resolve-defense-timeout` with the stable command id `timeout-defense:<attackId>` through the same command service, then publishes the board and action streams. An early or stale alarm finds nothing due.
3. Before a POST, PATCH, or DELETE reaches the app, the object arms an alarm one defence window ahead if none is set. If the object fails after a canonical append but before the command moves the alarm or publishes, this alarm still wakes it. A defence deadline is never earlier than that alarm, so the command's own deadline replaces it.
4. When an object is recreated, its constructor runs the same recovery inside `blockConcurrencyWhile`. It resolves a deadline that passed while the object was away, re-arms a future one, and repairs publication.

A human defence and the alarm both go through the CAS append. The first one commits. The other one gets `ATTACK_ALREADY_RESOLVED`, or `DEFENSE_DEADLINE_EXPIRED` for a roll at or after the deadline. If `alarm()` throws, the runtime retries it, and the stable command id makes the retry a duplicate.

## Limits

- **No data migration.** The deployment starts empty. It does not take over the archived hosted game at `hexdomination.gandrew.com`.
- **No Worker rebuild.** There is no admin or rebuild route. The offline `rebuild` command works only on a Bun SQLite file. A game object always serves `board1`. A future board format change would need an object-local maintenance path.
- **No data import.** The host does not read Bun SQLite files, the hosted game's Durable Objects, or the archived storage tables (`risk_streams` and `risk_messages`). Games start fresh.
- **Dice.** The object uses the same seeded default RNG as the Bun server. A recreated object restarts that sequence. This is unchanged game behaviour from both earlier hosts.
- **Agents.** The Worker serves the external-agent HTTP protocol. It does not run model inference, bots, or the seat launcher. Run those outside the Worker against its URL.
- **Recovery alarms.** A write with no pending alarm schedules one recovery wake a defence window later. With nothing due, that wake refolds history and finds the board current.
