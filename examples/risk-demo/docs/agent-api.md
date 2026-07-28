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

## Complete agent surface

| Endpoint                                                 | Purpose                                                               |
| -------------------------------------------------------- | --------------------------------------------------------------------- |
| `GET /v1/games/:gameId/map`                              | Public immutable territory names, neighbours, continents, and bonuses |
| `GET /v1/games/:gameId/players/me/actions?offset=&wait=` | Capability-scoped action-required stream                              |
| `GET /v1/games/:gameId/decision`                         | Capability-scoped bootstrap/recovery snapshot                         |
| `POST /v1/games/:gameId/commands`                        | Capability-scoped idempotent command submission                       |

`wait` is clamped to 0–30000 ms. Omitting `offset` reads from the beginning. Every response returns
`nextOffset`; offsets are opaque and meaningful only within this player's actions stream.

## Control loop

Read the actions stream, save `nextOffset`, and act only on its newest message. `ActionRequired`
contains the current turn, reinforcement accounting, `legalMoves`, complete ownership/armies, and
canonical events since the player's previous message. Submit exactly one command using that
message's `turn.id` and one legal move's `submit` template. Treat `accepted` and `duplicate` as
success. After a rejection, read the stream again; use `/decision` only for recovery.

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
private stream `games/<gameId>/players/<playerId>/actions/actions1`. It is never exposed by the
public `/streams/` facade.
