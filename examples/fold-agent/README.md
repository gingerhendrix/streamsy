# fold-agent

A Fold Core agent backed by Streamsy's Effect services. Fold owns the agent
loop, tool settlement and conversation projections. Streamsy stores the Fold
log and its append journal through `@streamsy/core-next`.

This release step proves recovery **inside one process and one memory Layer**.
`openMemoryStore()` acquires the Layer once and returns its context and `close`.
New Fold session scopes can reuse that context. Closing the store loses its data.
File-backed `openStore` fails with `StorageNotAvailable`: SQLite arrives in Step 2.

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

The CLI compiles, but default `start`, `resume`, and `inspect` use a file path and
cannot run until Step 2 SQLite support. Their persistence smoke remains explicitly
skipped with its seed/close/child-inspect body retained in the tests.

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

## Verification and limits

```bash
bun run --cwd examples/fold-agent typecheck
bun run --cwd examples/fold-agent test:unit
```

Tests drive a real scripted Fold tool turn and reconstruct it in a fresh scope,
exercise faults before/after commit, recover journal-only intent, and fence an old
owner during takeover. These prove memory-Layer behavior, not cross-process
persistence. Full-history recovery is O(log + journal); snapshots, retention,
in-flight provider request recovery, and persistent deployment are not provided.

`@humanlayer/fold-core@0.1.4` declares rc.109 peers. Effect stays exactly rc.112
through the workspace override; existing `@effect/ai-*` packages stay rc.109.
The tool-turn, resume and own-wire decoder tests check this combination. Fold v1
requires JSON omission of optional undefined usage fields: `fromJsonString` over
Fold's schema preserves this, while `toCodecJson` would convert them to null.
