# fold-agent

A [Fold Core](https://www.npmjs.com/package/@humanlayer/fold-core) agent whose
append-only event log is one Streamsy durable stream, written Effect-native
against the `@streamsy/streams` capabilities.

Fold runs unchanged: it keeps the agent loop, tool settlement, projections,
interruption, and resume semantics. Streamsy owns durability through Fold's
`eventLogSource` seam. The demo message is:

> Durable Streams are the data primitive for the agent loop, while Fold owns
> the loop semantics.

## Effect-native shape

The adapter consumes the experimental Effect capabilities instead of bridging
the Promise client by hand:

- `CreateStreams.create` creates a session log through a typed write capability.
- `ReadStreams.open` acquires scoped read sessions. One catch-up session backs
  `entries()`; one long-poll session backs `subscribe()`, so backlog and live
  tail are the same call and there is no catch-up/live boundary to lose an
  entry across.
- `AppendStreams.appendJsonBatch` commits each Fold entry under an
  exact-offset precondition (`expectedOffset`).
- `StreamCreateError` / `StreamReadError` / `StreamAppendError` carry the failure classification;
  the adapter maps them to Fold's typed `EventLog` errors.
- Capabilities are a `Layer`, so tests can swap in
  `TestStreamsLayer(...)` from `@streamsy/streams/testing` and
  script capability behaviour with no transport at all.

Stream creation, reads, and appends all cross the same injectable Effect
capability boundary. The Live layers own Promise-client adaptation, while tests
can replace the complete stream interface without constructing a transport.

## Commands

```bash
# Start a session, run one prompt, and print the resumable stream id.
OPENAI_API_KEY=... bun run --cwd examples/fold-agent start \
  "Use the text_stats tool on 'hello from Streamsy'"

# Simulate a new process and continue the same Fold session.
OPENAI_API_KEY=... bun run --cwd examples/fold-agent resume \
  <stream-id> "What did the tool return earlier?"

# Inspect the durable Fold facts without starting the model.
bun run --cwd examples/fold-agent inspect <stream-id>
```

Environment:

| Variable            | Meaning                                                        |
| ------------------- | -------------------------------------------------------------- |
| `FOLD_AGENT_DB`     | SQLite path (default `examples/fold-agent/.data/agent.sqlite`) |
| `OPENAI_API_KEY`    | Use an OpenAI-compatible provider (first choice)               |
| `ANTHROPIC_API_KEY` | Use Anthropic when no OpenAI key is set                        |
| `FOLD_AGENT_MODEL`  | Override the provider model id                                 |

`inspect` needs no credentials. Reading durable state never requires an API
key.

## Ownership model

One Fold session is one Streamsy stream: `fold/sessions/<session-id>/events`.

| Concern                                         | Authority                                                  |
| ----------------------------------------------- | ---------------------------------------------------------- |
| Session history, tool results, completion facts | Fold log entries stored in the Streamsy stream             |
| Durable ordering and read checkpoint            | Streamsy offset                                            |
| Agent semantic order                            | Fold `seq`                                                 |
| Conversation state and model-visible messages   | Fold projections over replayed entries                     |
| Live text/reasoning deltas                      | Fold ephemeral session events (not persisted)              |
| SQLite file                                     | Storage implementation detail behind the Streamsy protocol |

Streamsy offsets are opaque transport checkpoints. Fold `seq` is the
agent-domain sequence carried inside each message. Neither is derived from the
other.

Appends are committed under an exact-offset compare-and-swap before anything
becomes observable. A competing writer is fenced with a typed error, never
silently re-sequenced: re-sequencing would graft an entry from a live Fold
runtime onto history that runtime has never seen.

## Non-claims

- **Single-writer only.** The CAS detects a competing writer; it does not
  elect one. No activation lease, no fencing token, no transparent
  continuation.
- **No in-flight model-call resume.** Fold's durable markers describe a
  mid-turn process loss; the upstream provider request is gone.
- **Full-history replay.** `entries()` re-reads durable storage each call.
  Fine for an example; a service needs snapshots, retention, and pagination.
- Fold context compaction writes semantic summary entries; it does not remove
  old durable facts, and this example never compacts the Streamsy log.

## Tests

```bash
bun run --cwd examples/fold-agent test:unit
```

The suite runs without provider credentials. A scripted `LanguageModel` drives
Fold through a real tool turn, a SQLite close/reopen restart, and a resume
with prior context. Adapter contract tests run over both the memory and SQLite
storage backends, plus a transport-free capability-injection test through
`TestStreamsLayer`.

## Version pins

`@humanlayer/fold-core@0.1.4` pins `effect@4.0.0-rc.109` and the matching
`@effect/ai-*` packages as exact peers. The example pins the same versions,
and the repository override keeps one `effect` instance across the workspace.
Treat a Fold or Effect upgrade as a separate reviewed change.
