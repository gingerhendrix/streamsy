# fold-agent

A [Fold Core](https://www.npmjs.com/package/@humanlayer/fold-core) agent whose
log lives in Streamsy. Fold owns the agent loop, tool settlement, and
conversation projections. Streamsy stores each session's event log, so a
session can resume from durable history after a crash.

## Run it

```text
bun run --cwd examples/fold-agent start   <prompt>
bun run --cwd examples/fold-agent resume  <stream-id> <prompt>
bun run --cwd examples/fold-agent inspect <stream-id>
```

`start` opens a new session. `resume` continues one from the stored log.
`inspect` renders a session and needs no model credentials. Add `--help` to
any command for its usage.

| Variable                              | Meaning                                                       |
| ------------------------------------- | ------------------------------------------------------------- |
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` | Provider credentials                                          |
| `FOLD_AGENT_MODEL`                    | Model selection                                               |
| `FOLD_AGENT_DB`                       | SQLite file, default `examples/fold-agent/.data/agent.sqlite` |
| `FOLD_AGENT_SCRIPT`                   | Scripted JSON turns for provider-free runs and tests          |

## How it works

A session uses one stream, `fold/sessions/<session>/events`, declared with
`Streamsy.json`. The template parameter is Fold's `SessionId`, so the CLI
rejects a stream id that does not name a session. Opening the writer
reads that log, validates Fold's sequence, and uses its length as the next
sequence. Each append names the observed tail with `expectedOffset`. If
another process advances the log first, the append fails and the stale writer
stays fenced until the caller opens a new writer from durable history.

If the process crashes after a model call but before its append commits, the
model call runs again on the next turn. This is the accepted cost of keeping
the example to one stream and one write per entry.

`memoryLayer()` and `sqliteLayer(filename)` give the same `Sessions` service
on the memory host and on Bun SQLite. The CLI reads its settings with Effect
`Config` and provides the store Layer for each command. The commands are
defined with `effect/unstable/cli` and run by `BunRuntime.runMain`, so Ctrl-C
interrupts the command and closes the store.

## Verify

```bash
bun run --cwd examples/fold-agent typecheck
bun run --cwd examples/fold-agent test:unit
```

The tests run the store contract on memory and SQLite, drive real CLI
subprocesses with the scripted fixture, and prove `expectedOffset` fencing
across separate writer processes. This demo has no HTTP surface, so its three
SQLite process tests are its smoke suite.

## Limits

Full-history recovery is O(log). Snapshots, retention, and recovery of an
in-flight provider request are not provided. Non-recomputable payloads need
the persisted tuple contract described under
[Producer restarts](../../packages/core/README.md#producer-restarts). Effect is
pinned to `4.0.0-rc.115` through the workspace override;
`@humanlayer/fold-core` declares older peers, and the tests cover that
combination.
