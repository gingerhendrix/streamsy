# fold-agent

A [Fold Core](https://www.npmjs.com/package/@humanlayer/fold-core) agent whose
log lives in Streamsy. Fold owns the agent loop, tool settlement, and
conversation projections. Streamsy stores each session's event log and an
append journal, so a session can resume after a crash or be taken over by a
new process without duplicating entries.

## Run it

```text
bun run --cwd examples/fold-agent start   <prompt>
bun run --cwd examples/fold-agent resume  <stream-id> [--epoch <n>] [--takeover] <prompt>
bun run --cwd examples/fold-agent inspect <stream-id>
```

`start` opens a new session. `resume` continues one: without a flag it uses
the last journal epoch, `--epoch` requires an exact match, and `--takeover`
fences the old writer and starts a new epoch. `inspect` renders a session and
needs no model credentials.

| Variable                              | Meaning                                                       |
| ------------------------------------- | ------------------------------------------------------------- |
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` | Provider credentials                                          |
| `FOLD_AGENT_MODEL`                    | Model selection                                               |
| `FOLD_AGENT_DB`                       | SQLite file, default `examples/fold-agent/.data/agent.sqlite` |
| `FOLD_AGENT_SCRIPT`                   | Scripted JSON turns for provider-free runs and tests          |

## How it works

A session uses the streams `fold/sessions/<id>/events` and
`fold/sessions/<id>/journal` with producer id `fold-session:<id>`. Each append
journals the exact entry and producer tuple first, then appends under that
tuple with `Producer.append`. Because the tuple is stable, a retry or a
delayed append from an old process settles as `Duplicate` instead of
duplicating the entry. Recovery replays the log, settles the last pending
entry from the journal, and then either resumes the same epoch or takes over
with a new one.

`openMemoryStore()` and `openStore({ filename })` give the same store on the
memory Layer and on Bun SQLite.

## Verify

```bash
bun run --cwd examples/fold-agent typecheck
bun run --cwd examples/fold-agent test:unit
```

The tests run the store contract on memory and SQLite, drive real CLI
subprocesses with the scripted fixture, and prove crash recovery and takeover
fencing across separate writer processes.

## Limits

Full-history recovery is O(log + journal). Snapshots, retention, and recovery
of an in-flight provider request are not provided. Effect is pinned to
`4.0.0-rc.115` through the workspace override; `@humanlayer/fold-core`
declares older peers, and the tests cover that combination.
