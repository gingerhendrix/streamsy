# External seat launcher

`risk-seat.mjs` is a copyable, dependency-free Node launcher for one private
external-agent seat. It imports no repository or bot code. The launcher enforces
the HTTP protocol; Claude Code or Codex chooses one strategic action from each
fresh `legalActions`.

Initialize protected state from the complete private URL:

```bash
node risk-seat.mjs init --seat-url '<private-seat-url>' --state ./seat-state
```

Run a bounded control loop:

```bash
node risk-seat.mjs run \
  --state ./seat-state \
  --harness claude \
  --max-commands 1 \
  --max-decisions 8 \
  --max-posts-per-command 2 \
  --wall-ms 120000 \
  --model-timeout-ms 45000 \
  --wait-ms 3000 \
  --max-budget-usd 1
```

Use `--harness codex` for Codex. Both profiles use the same observation,
freshness, validation, retry, evidence, and cancellation path. Claude runs with
no tools, no session persistence, safe mode, `dontAsk`, and a spend bound. Codex
runs ephemerally, read-only, with approvals disabled and no repository
requirement. Neither model working directory contains the capability.

Pass `--cancel-file <path>` to make creation of that file abort an active wait or
strategy subprocess. `SIGINT` and `SIGTERM` use the same cleanup path. Every HTTP
wait, model call, command retry, decision count, command count, and total run has
an explicit bound.

The state directory contains the bearer capability and must remain private. The
launcher creates it as `0700` and its state/evidence files as `0600`. Evidence
contains hashes, action types, statuses, and counts—not capabilities,
authorization headers, private URLs, or game/player/territory/attack IDs.
