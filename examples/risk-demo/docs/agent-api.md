# Hex Domination agent API

An external agent receives one play-only seat from the human host:

```http
POST /v1/games/:gameId/agent-seats
Authorization: Bearer <host capability>
```

The one-time response contains a machine-readable `seat` descriptor and matching pasteable
`instructions`. The agent capability is scoped to that game/player and can observe or play only that
seat. It cannot create or join games, open seats, or start a game. Capabilities are sent only as
`Authorization: Bearer` headers and seat-scoped responses are `no-store`.

Omit `playerId` to open a new agent-controlled seat. Pass `playerId` to convert an existing seat into
an agent seat — and the only seat a host may convert is **its own**, so `playerId` must equal the
authenticated host capability's player. Any other value is `403 FORBIDDEN`. `controller: "agent"` (and
its internal spelling `external-agent`) is refused on the unauthenticated create and join routes with
`403 AGENT_SEAT_REQUIRES_HOST`; this route is the only place an agent capability is minted.

## Complete agent surface

| Endpoint                                           | Purpose                                                               |
| -------------------------------------------------- | --------------------------------------------------------------------- |
| `GET /v1/games/:gameId/map`                        | Public immutable territory names, neighbours, continents, and bonuses |
| `GET /v1/games/:gameId/players/me/actions?offset=` | Capability-scoped action-required stream (SSE)                        |
| `GET /v1/games/:gameId/decision`                   | Capability-scoped bootstrap/recovery snapshot                         |
| `POST /v1/games/:gameId/commands`                  | Capability-scoped idempotent command submission                       |

Omitting `offset` reads from the beginning of this player's stream. Offsets are opaque and meaningful
only within it.

## The actions stream

The resource is `text/event-stream`, and `offset` is its only parameter. `wait` is gone: a request
carrying it is refused with `400 BAD_REQUEST` rather than silently interpreted.

```text
event: data
data:[
data:{"type":"ActionRequired","messageId":"act:g:p:1", …}
data:]

event: control
data:{"nextOffset":"…","upToDate":true}
```

A `data` frame carries a JSON array of messages, split one element per `data:` line; the `control`
frame that follows names the offset those messages were read through. Advance the cursor only on a
`control` frame, and never past a message the same batch did not deliver. The terminal control frame
— the one following `GameOver` — adds `"closed": true`.

One connection carries the backlog immediately, then holds open until an action lands. The server
closes it after **30 seconds** (`ACTIONS_STREAM_TIMEOUT_MS` in `src/application/actions-stream.ts`,
which is also what the launcher and the first-party bot size themselves against). Reconnect with the
newest `nextOffset`; a bounded connection plus an exact durable offset is what makes resume
gap-free and duplicate-free.

Capabilities travel only in `Authorization: Bearer`, so clients use `fetch` and parse the stream
themselves — `EventSource` cannot set headers, and the token must never enter a URL. Responses are
`no-store` and `no-referrer`.

`Accept: application/json` returns one immediate, non-blocking `AgentActionsPage` instead
(`{messages, nextOffset, upToDate}`). That representation is for bootstrap, recovery and the
repository's own scripted consumers; it never blocks and is never the default.

## Control loop

Read the actions stream, save `nextOffset`, and act only on its newest message. `ActionRequired`
contains the current turn, reinforcement accounting, `legalMoves`, complete ownership/armies, and
canonical events since the player's previous message. Submit exactly one command using that
message's `turn.id` and one legal move's `submit` template. Treat `accepted` and `duplicate` as
success. After a rejection, read the stream again; use `/decision` only for recovery.

The ack is a receipt, not an outcome:

```json
{ "status": "accepted", "commandId": "…", "turnId": "round-2:p1", "eventOffset": "…" }
```

Dice, captures and phase changes are published on the actions stream, never in the ack. A browser
that needs to wait for the board projection derives the transaction id from `commandId` and
`eventOffset` (`boardProjectionTxId`).

Advance the persisted cursor only once the command answering a message has been acknowledged. An
`ActionRequired` is not re-announced: the server has already said everything it has to say about that
position, so a cursor persisted past an unanswered ask waits forever. Retain the exact request body
until then and replay it byte-identically after a restart; the server dedupes on `commandId`.

`GameOver` names the winner and terminates the loop. Canonical event/message types are PascalCase;
command action types are kebab-case.

Malformed command bodies return:

```json
{
  "status": "rejected",
  "error": {
    "code": "INVALID_ACTION",
    "message": "Command validation failed.",
    "details": [
      {
        "path": "action.placements[0].armies",
        "expected": "integer >= 1",
        "received": 0
      }
    ]
  }
}
```

The actions stream is derived replay-safely from `games/<gameId>/events` into the generation-suffixed
private stream `games/<gameId>/players/<playerId>/actions`. It is never exposed by the
public `/streams/` facade.
