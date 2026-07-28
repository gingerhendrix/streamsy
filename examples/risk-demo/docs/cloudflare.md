# Cloudflare deployment

The Cloudflare runtime is an additional deployment target; the existing Bun/SQLite server remains
the local product workflow.

## Isolation model

The edge Worker creates a 96-bit `game_<hex>` id and selects
`GAME.get(GAME.idFromName(gameId))`. Every game-scoped API request, active public board-stream read,
and external-agent request is routed by that same id.

`GameDurableObject` owns one SQLite database containing:

- every Streamsy stream for that game (canonical events, active and retired board projections, and
  player action-required streams);
- capability verifier hashes (never plaintext tokens), command idempotency records, game metadata,
  and projection-generation records;
- the canonical pending-defence state and the Durable Object alarm used to recover its deadline.

The game-local Streamsy adapter does not call a Durable Object namespace and does not route by
stream id. `@streamsy/storage-durable-object` is intentionally not used here because its model is
one object per stream.

Agent requests are game-scoped and carry the capability only in the bearer header:

```text
/v1/games/:gameId/map
/v1/games/:gameId/players/me/actions
/v1/games/:gameId/decision
/v1/games/:gameId/commands
```

This lets the edge select the game object without a global token registry. Token-bearing URL paths,
the old personalized state/wait resources, and the seat-bootstrap URL are removed in both runtimes.

## Local Workers verification

```bash
bun run build
bun run --cwd examples/risk-demo dev:cloudflare:local -- --port 8791

# in another shell
BASE_URL=http://127.0.0.1:8791 \
  bun run --cwd examples/risk-demo smoke:cloudflare
```

The local command uses `wrangler.jsonc` only as a deterministic Miniflare/Workers test harness.
Alchemy remains the deployment authority.

## Deploy with Alchemy

Alchemy uses app `streamsy-risk`, the selected Alchemy stage (the local username by default), a
SQLite `game` Durable Object namespace, and the `demo` Website/Worker with bundled SPA assets.

```bash
bun run --cwd examples/risk-demo deploy:cloudflare
```

Then verify the printed URL:

```bash
BASE_URL=https://<printed-worker-url> \
  bun run --cwd examples/risk-demo smoke:cloudflare
```

Deploying requires a configured Alchemy Cloudflare profile or a valid Cloudflare login/token in the
environment. Alchemy owns the generated Worker name and resource state. The package scripts execute
the Alchemy TypeScript program with Node because Bun 1.3.14 crashes while evaluating Alchemy 0.82.1
in this repository; this does not change the Alchemy resources or state backend.

## Teardown

Destroy the same stage that was deployed:

```bash
bun run --cwd examples/risk-demo destroy:cloudflare
```

If a non-default stage was selected, pass the same `--stage <stage>` argument to deploy and destroy.
Destroying removes the demo resources and their Durable Object namespace data.
