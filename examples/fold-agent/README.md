# fold-agent

A Fold Core agent backed by Streamsy's Effect services. Fold owns the agent
loop, tool settlement and conversation projections. Streamsy stores the Fold
log and its append journal through `@streamsy/core`.

`openMemoryStore()` acquires the memory Layer once. `openStore({ filename })`
acquires the official Bun SQLite protocol Layer and creates the parent directory.
Both return one context and idempotent `close`; failed acquisition and CLI exit
close the scope. Memory disappears at close, while SQLite survives processes.

## Data flow and ownership

A session uses `fold/sessions/<id>/events`, `fold/sessions/<id>/journal`, and
producer id `fold-session:<id>`. Fold entry sequences and producer sequences are
separate; producer sequence starts at zero in each epoch.

Under an adapter semaphore, each append mints and encodes one Fold entry, writes
its exact `Pending` payload and tuple to the journal under `expectedOffset` CAS,
then calls `Producer.append`. Retryable storage failures get five total attempts
with the same tuple and bytes. Both `appended` and `duplicate` acknowledge intent.
No acknowledgement row is necessary: the log itself proves acknowledgement.

Recovery validates contiguous Fold entries, the initial `session_started`, and
all journal sequence/payload bindings. It settles the last unacknowledged Pending
before appending a same-epoch resume marker or a new takeover epoch. That marker's
CAS transfers journal ownership; an old adapter cannot journal new intent.
A delayed old log append carries only its already-journaled tuple. It can settle
that intent before takeover's first new append, or receive `stale-epoch` after it.
This is not an activation lease or transparent continuation of a live agent.

After an exhausted failure or interruption during append, that adapter refuses
new payloads; reconstruct through resume to settle uncertain intent first.
Journal conflicts fail fenced. Invalid producer sequence outcomes are corrupt
journal defects. Closed/missing streams and operational failures remain errors.
Entries and subscriptions decode through Fold's own v1 contract.

## CLI and persistence boundary

The CLI's `start`, `resume`, and `inspect` use retained-file SQLite by default.

```text
start   <prompt>
resume  <stream-id> [--epoch <n>] [--takeover] <prompt>
inspect <stream-id>
```

Without an epoch flag, resume uses the last journal epoch. `--epoch` requires an
exact match; `--takeover` increments it. The flags are mutually exclusive.
`FOLD_AGENT_DB` retains its default `examples/fold-agent/.data/agent.sqlite`.
`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, and `FOLD_AGENT_MODEL` retain their provider
selection meanings. Inspect rendering requires no model credentials.
`FOLD_AGENT_SCRIPT` accepts deterministic JSON `text` and `tool` turns for the
provider-free verification fixture; normal users should select a provider.

## Verification and limits

```bash
bun run --cwd examples/fold-agent typecheck
bun run --cwd examples/fold-agent test:unit
```

The same EventLog contract runs on memory and SQLite. Separate writer processes
prove retained producer identity, exact crash-retained tuple and payload recovery,
intervening unrelated activity, takeover fencing, and a log/journal read race.
Real CLI subprocesses run start, resume, and inspect with the scripted fixture;
inspect separately proves it needs no provider credentials. Full-history recovery
is O(log + journal); snapshots, retention,
in-flight provider request recovery, and persistent deployment are not provided.

`@humanlayer/fold-core@0.1.4` declares rc.109 peers. Effect stays exactly rc.112
through the workspace override; existing `@effect/ai-*` packages stay rc.109.
The tool-turn, resume and own-wire decoder tests check this combination. Fold v1
requires JSON omission of optional undefined usage fields: `fromJsonString` over
Fold's schema preserves this, while `toCodecJson` would convert them to null.
