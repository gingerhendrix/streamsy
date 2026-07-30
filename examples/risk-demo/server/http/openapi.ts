/**
 * OpenAPI 3.1 document + JSON Schemas for the Risk command/decision API.
 *
 * Served at `GET /openapi.json`. A coding agent can discover every command
 * shape, ack, decision context, and error code from this document without
 * reading any UI or server source.
 *
 * The document describes the sole Hex Domination command vocabulary and the
 * causally-watermarked resources a player uses to act.
 */

import { RULES } from "../../src/domain/map.ts";

/**
 * Published from the rule rather than restated, so the documented bound cannot
 * drift from the one the decider actually enforces.
 */
const MAX_PLAYER_NAME_LENGTH = RULES.maxPlayerNameLength;

const commandAction = {
  oneOf: [
    {
      type: "object",
      required: ["type", "placements"],
      description:
        "The complete reinforcement-turn allocation. Placements must name distinct owned territories and their armies must sum to the current legal action's maxArmies.",
      properties: {
        type: { const: "reinforce" },
        placements: {
          type: "array",
          minItems: 1,
          items: {
            type: "object",
            required: ["territoryId", "armies"],
            properties: {
              territoryId: { type: "string" },
              armies: { type: "integer", minimum: 1 },
            },
          },
        },
      },
    },
    {
      type: "object",
      required: ["type", "from", "to", "attackerDice"],
      description: "One throw. Rolls the attacker's dice and opens a defence interrupt.",
      properties: {
        type: { const: "declare-attack" },
        from: { type: "string" },
        to: { type: "string" },
        attackerDice: { type: "integer", minimum: 1, maximum: 3 },
      },
    },
    {
      type: "object",
      required: ["type", "attackId"],
      description:
        "The defender authorizes the roll; the dice count was fixed at declaration and is not chosen here. Legal out of turn.",
      properties: {
        type: { const: "roll-defense" },
        attackId: { type: "string" },
      },
    },
    {
      type: "object",
      required: ["type", "attackId", "armies"],
      description: "Required after a capture, before any other command is legal.",
      properties: {
        type: { const: "occupy-territory" },
        attackId: { type: "string" },
        armies: { type: "integer", minimum: 1 },
      },
    },
    {
      type: "object",
      required: ["type", "from", "to", "armies"],
      description: "Moves through any path of owned countries, not just adjacent ones.",
      properties: {
        type: { const: "fortify" },
        from: { type: "string" },
        to: { type: "string" },
        armies: { type: "integer", minimum: 1 },
      },
    },
    {
      type: "object",
      required: ["type"],
      description: "Declines the optional fortification and ends the turn.",
      properties: { type: { const: "skip-fortifications" } },
    },
  ],
} as const;

const legalAction = {
  oneOf: [
    {
      type: "object",
      required: ["type", "territoryIds", "pool", "submit"],
      description:
        "Submit one reinforce command whose placements use distinct territoryIds and sum exactly to pool.",
      properties: {
        type: { const: "reinforce" },
        territoryIds: { type: "array", items: { type: "string" } },
        pool: { type: "integer", minimum: 1 },
        submit: { type: "object" },
      },
    },
    {
      type: "object",
      required: ["type", "choices", "submit"],
      properties: {
        type: { const: "declare-attack" },
        choices: {
          type: "array",
          items: {
            type: "object",
            required: ["from", "to", "maxAttackerDice"],
            properties: {
              from: { type: "string" },
              to: { type: "string" },
              maxAttackerDice: { type: "integer" },
            },
          },
        },
        submit: { type: "object" },
      },
    },
    {
      type: "object",
      required: ["type", "attackId", "dice", "deadlineAt", "submit"],
      properties: {
        type: { const: "roll-defense" },
        attackId: { type: "string" },
        dice: { type: "integer" },
        deadlineAt: {
          type: "integer",
          description: "Canonical epoch-ms defence deadline.",
        },
        submit: { type: "object" },
      },
    },
    {
      type: "object",
      required: ["type", "attackId", "from", "to", "minArmies", "maxArmies", "submit"],
      properties: {
        type: { const: "occupy-territory" },
        attackId: { type: "string" },
        from: { type: "string" },
        to: { type: "string" },
        minArmies: { type: "integer" },
        maxArmies: { type: "integer" },
        submit: { type: "object" },
      },
    },
    {
      type: "object",
      required: ["type", "choices", "submit"],
      properties: {
        type: { const: "fortify" },
        choices: {
          type: "array",
          items: {
            type: "object",
            required: ["from", "reachable"],
            properties: {
              from: { type: "string" },
              reachable: {
                type: "array",
                items: {
                  type: "object",
                  required: ["to", "maxArmies"],
                  properties: {
                    to: { type: "string" },
                    maxArmies: { type: "integer" },
                  },
                },
              },
            },
          },
        },
        submit: { type: "object" },
      },
    },
    {
      type: "object",
      required: ["type", "submit"],
      properties: {
        type: { const: "skip-fortifications" },
        submit: { type: "object" },
      },
    },
  ],
} as const;

const pendingInteraction = {
  oneOf: [
    {
      type: "object",
      required: [
        "type",
        "attackId",
        "turnId",
        "attackerId",
        "defenderId",
        "from",
        "to",
        "attackerDice",
        "attackerRolls",
        "defenderDice",
        "defenseDeadlineAt",
      ],
      properties: {
        type: { const: "defense" },
        attackId: { type: "string" },
        turnId: { type: "string" },
        attackerId: { type: "string" },
        defenderId: { type: "string" },
        from: { type: "string" },
        to: { type: "string" },
        attackerDice: { type: "integer" },
        attackerRolls: { type: "array", items: { type: "integer" } },
        defenderDice: { type: "integer" },
        declaredAt: { type: "integer" },
        defenseDeadlineAt: { type: "integer" },
      },
    },
    {
      type: "object",
      required: ["type", "attackId", "turnId", "playerId", "from", "to", "minArmies", "maxArmies"],
      properties: {
        type: { const: "occupation" },
        attackId: { type: "string" },
        turnId: { type: "string" },
        playerId: { type: "string" },
        from: { type: "string" },
        to: { type: "string" },
        minArmies: { type: "integer" },
        maxArmies: { type: "integer" },
      },
    },
  ],
} as const;

const reinforcement = {
  type: "object",
  required: ["base", "continents", "total", "remaining"],
  properties: {
    base: { type: "integer" },
    continents: {
      type: "array",
      items: {
        type: "object",
        required: ["continentId", "bonus"],
        properties: {
          continentId: { type: "string" },
          bonus: { type: "integer" },
        },
      },
    },
    total: { type: "integer" },
    remaining: { type: "integer" },
  },
} as const;

const errorCodes = [
  "NOT_YOUR_TURN",
  "STALE_TURN",
  "INVALID_PHASE",
  "ILLEGAL_ACTION",
  "INSUFFICIENT_ARMIES",
  "NOT_ADJACENT",
  "UNKNOWN_TERRITORY",
  "COMMAND_ID_REUSED",
  "GAME_FINISHED",
  "GAME_NOT_FOUND",
  "GAME_NOT_STARTED",
  "GAME_ALREADY_STARTED",
  "NOT_ENOUGH_PLAYERS",
  "TOO_MANY_PLAYERS",
  "PLAYER_ID_TAKEN",
  "MAP_GENERATION_FAILED",
  // Hex Domination two-stage combat
  "PENDING_DEFENSE",
  "PENDING_OCCUPATION",
  "NOT_DEFENDING_PLAYER",
  "ATTACK_ID_MISMATCH",
  "ATTACK_ALREADY_RESOLVED",
  "DEFENSE_DEADLINE_EXPIRED",
  "INVALID_OCCUPATION",
  "NO_FRIENDLY_PATH",
  "UNAUTHORIZED",
  "FORBIDDEN",
  "WRONG_GAME",
  "NOT_FOUND",
  "BAD_REQUEST",
  "INVALID_ACTION",
  "AGENT_SEAT_REQUIRES_HOST",
  "PROJECTION_UNAVAILABLE",
  "INTERNAL",
] as const;

const schemas = {
  SeatControllerInput: {
    type: "string",
    enum: ["human", "bot", "agent"],
    description:
      "`agent` reserves the seat for an external coding-agent harness using its private seat URL. Canonical events record that controller as `external-agent`; `bot` is the repository's deterministic scripted policy.",
  },
  CreateGameRequest: {
    type: "object",
    properties: {
      name: { type: "string" },
      color: {
        type: "string",
        description:
          "Optional colour request. The server assigns colours conflict-safely: a free requested colour is honoured; an absent or taken one is replaced by the first available palette colour. The response's `player.color` is the colour actually issued.",
      },
      commandId: { type: "string" },
      controller: { $ref: "#/components/schemas/SeatControllerInput" },
      mapSeed: { type: "string" },
    },
  },
  JoinGameRequest: {
    type: "object",
    properties: {
      name: { type: "string" },
      color: {
        type: "string",
        description: "Optional colour request; assigned conflict-safely as on CreateGameRequest.",
      },
      commandId: { type: "string" },
      controller: { $ref: "#/components/schemas/SeatControllerInput" },
    },
  },
  GameCommand: {
    type: "object",
    description:
      "Hex Domination play command. `roll-defense` is the one legal out-of-turn action; the internal timeout resolver is never exposed here.",
    required: ["commandId", "turnId", "action"],
    properties: {
      commandId: {
        type: "string",
        description:
          "Stable idempotency key. For a declaration it also becomes the attackId; a defence retry must preserve its original commandId and payload.",
      },
      turnId: {
        type: "string",
        description: "Observed turn precondition, e.g. round-2:p1.",
      },
      action: commandAction,
    },
  },
  CommandAck: {
    type: "object",
    description:
      "Receipt only: the command was recorded, at this canonical offset. It is not the outcome — dice, captures and phase changes arrive on the player's actions stream. `accepted` and `duplicate` are both success.",
    required: ["status", "commandId", "eventOffset"],
    additionalProperties: false,
    properties: {
      status: { enum: ["accepted", "duplicate"] },
      commandId: { type: "string" },
      turnId: {
        type: "string",
        description: "Echo of the submitted turn precondition; absent on lobby commands.",
      },
      eventOffset: {
        type: "string",
        description: "Committed final canonical offset of the batch.",
      },
    },
  },
  ErrorResponse: {
    type: "object",
    required: ["status", "error"],
    properties: {
      status: { const: "rejected" },
      error: {
        type: "object",
        required: ["code", "message"],
        properties: {
          code: { enum: errorCodes },
          message: { type: "string" },
          currentTurnId: { type: "string" },
          details: {
            type: "array",
            description:
              "Field-level validation failures accompanying INVALID_ACTION. Each entry names the offending path, what was expected there, and what arrived.",
            items: {
              type: "object",
              required: ["path", "expected", "received"],
              properties: {
                path: { type: "string" },
                expected: { type: "string" },
                received: {},
              },
            },
          },
        },
      },
    },
  },
  DecisionContext: {
    type: "object",
    description:
      "Hex Domination decision context. Player-relative: an out-of-turn defender gets `roll-defense` here. Derived from canonical history through `board.sourceThroughOffset`, which the named board generation has already materialized — so the decision is never ahead of its board snapshot.",
    required: ["gameId", "player", "mode", "turn", "board", "legalMoves"],
    properties: {
      gameId: { type: "string" },
      player: {
        type: "object",
        required: ["id", "name", "color", "controller"],
        properties: {
          id: { type: "string" },
          name: { type: "string" },
          color: { type: "string" },
          controller: { enum: ["human", "bot", "external-agent"] },
        },
      },
      mode: { enum: ["active-turn", "defense", "waiting", "finished"] },
      turn: {
        type: "object",
        required: ["id", "round", "phase", "reinforcement"],
        properties: {
          id: { type: "string" },
          round: { type: "integer" },
          activePlayerId: { type: "string" },
          phase: { enum: ["setup", "reinforce", "attack", "fortify"] },
          reinforcement,
        },
      },
      pendingInteraction,
      board: {
        type: "object",
        required: ["sourceStreamId", "generation", "map", "territories", "players"],
        properties: {
          sourceStreamId: { type: "string" },
          sourceThroughOffset: { type: ["string", "null"] },
          generation: { type: "string" },
          map: {
            type: "object",
            description:
              "A reference, not the snapshot. Static geometry is fetched once from GET /board (or followed on boardStreamId).",
            required: ["boardStreamId", "territoryCount", "continentCount"],
            properties: {
              mapVersion: { const: "procedural-hex-v1" },
              generatorVersion: { const: "hex-generator-v1" },
              seed: { type: "string" },
              boardStreamId: { type: "string" },
              territoryCount: { type: "integer" },
              continentCount: { type: "integer" },
            },
          },
          territories: {
            type: "array",
            items: {
              type: "object",
              required: ["id", "armies"],
              properties: {
                id: { type: "string" },
                ownerId: { type: "string" },
                armies: { type: "integer" },
              },
            },
          },
          players: {
            type: "array",
            items: {
              type: "object",
              required: ["id", "controller", "eliminated"],
              properties: {
                id: { type: "string" },
                controller: { enum: ["human", "bot", "external-agent"] },
                eliminated: { type: "boolean" },
              },
            },
          },
        },
      },
      legalMoves: { type: "array", items: legalAction },
    },
  },
  Board: {
    type: "object",
    description:
      "Hex Domination projected board. Static map rows (hexes/territories/continents) are served here once; `turn` and `combat` are zero-or-one current rows.",
    required: [
      "gameId",
      "sourceStreamId",
      "generation",
      "boardStreamId",
      "game",
      "players",
      "hexes",
      "territories",
      "continents",
      "turn",
      "combat",
      "moves",
    ],
    properties: {
      gameId: { type: "string" },
      sourceStreamId: { type: "string" },
      sourceThroughOffset: { type: ["string", "null"] },
      generation: { type: "string" },
      boardStreamId: {
        type: "string",
        description: "Durable State stream a browser follows live for this generation.",
      },
      reducerVersion: { type: "string" },
      game: { type: "object" },
      players: {
        type: "array",
        items: {
          type: "object",
          required: ["id", "name", "color", "controller", "eliminated"],
          properties: {
            id: { type: "string" },
            name: { type: "string" },
            color: { type: "string" },
            controller: { enum: ["human", "bot", "external-agent"] },
            eliminated: { type: "boolean" },
            territoryCount: { type: "integer" },
            armyCount: { type: "integer" },
          },
        },
      },
      hexes: {
        type: "array",
        items: {
          type: "object",
          required: ["id", "q", "r", "territoryId", "terrain"],
          properties: {
            id: { type: "string" },
            q: { type: "integer" },
            r: { type: "integer" },
            territoryId: { type: "string" },
            terrain: {
              enum: ["plains", "forest", "hills", "desert", "mountains"],
            },
          },
        },
      },
      territories: {
        type: "array",
        items: {
          type: "object",
          required: ["id", "name", "continentId", "armies", "hexIds", "adjacentTerritoryIds"],
          properties: {
            id: { type: "string" },
            name: { type: "string" },
            continentId: { type: "string" },
            ownerId: { type: "string" },
            armies: { type: "integer" },
            hexIds: { type: "array", items: { type: "string" } },
            adjacentTerritoryIds: { type: "array", items: { type: "string" } },
            labelAnchor: {
              type: "object",
              required: ["q", "r"],
              properties: { q: { type: "integer" }, r: { type: "integer" } },
            },
          },
        },
      },
      continents: {
        type: "array",
        items: {
          type: "object",
          required: ["id", "name", "territoryIds", "reinforcementBonus"],
          properties: {
            id: { type: "string" },
            name: { type: "string" },
            territoryIds: { type: "array", items: { type: "string" } },
            reinforcementBonus: { type: "integer" },
            controllerId: { type: "string" },
            palette: { type: "object" },
          },
        },
      },
      turn: {
        type: ["object", "null"],
        properties: {
          turnId: { type: "string" },
          round: { type: "integer" },
          playerId: { type: "string" },
          phase: { enum: ["reinforce", "attack", "fortify"] },
          reinforcement,
          reinforcementsPlaced: { type: "integer" },
          attacksDeclared: { type: "integer" },
          throwsResolved: { type: "integer" },
          captures: { type: "integer" },
          eliminations: { type: "integer" },
          latestDice: { type: "object" },
        },
      },
      combat: {
        type: ["object", "null"],
        description: "Zero or one pending combat. Cleared as soon as the interrupt closes.",
        properties: {
          attackId: { type: "string" },
          turnId: { type: "string" },
          status: { enum: ["awaiting-defense", "awaiting-occupation"] },
          attackerId: { type: "string" },
          defenderId: { type: "string" },
          from: { type: "string" },
          to: { type: "string" },
          attackerDice: { type: "integer" },
          attackerRolls: { type: "array", items: { type: "integer" } },
          defenderDice: { type: "integer" },
          defenderRolls: { type: "array", items: { type: "integer" } },
          attackerLosses: { type: "integer" },
          defenderLosses: { type: "integer" },
          territoryCaptured: { type: "boolean" },
          resolutionSource: { enum: ["human", "bot", "agent", "timeout"] },
          declaredAt: { type: "integer" },
          defenseDeadlineAt: { type: "integer" },
          minArmies: { type: "integer" },
          maxArmies: { type: "integer" },
        },
      },
      moves: { type: "array", items: { type: "object" } },
    },
  },
  RenamePlayerRequest: {
    type: "object",
    description:
      "The decider trims the name and bounds it to RULES.maxPlayerNameLength before recording it, so a longer string is accepted and truncated rather than refused. `minLength` is the one hard floor: a name that trims to nothing is rejected with INVALID_NAME.",
    additionalProperties: false,
    required: ["name"],
    properties: {
      name: {
        type: "string",
        minLength: 1,
        maxLength: MAX_PLAYER_NAME_LENGTH,
        description: `Trimmed, then truncated to ${MAX_PLAYER_NAME_LENGTH} characters. The recorded name is echoed in the response.`,
      },
      commandId: { type: "string" },
    },
  },
  RenamePlayerResponse: {
    type: "object",
    required: ["player", "ack"],
    properties: {
      player: {
        type: "object",
        description: "The seat as canonical history now records it, not as requested.",
        required: ["id", "name"],
        properties: { id: { type: "string" }, name: { type: "string" } },
      },
      ack: { $ref: "#/components/schemas/CommandAck" },
    },
  },
  LeaveGameResponse: {
    type: "object",
    required: ["playerId", "ack"],
    properties: {
      playerId: { type: "string" },
      ack: { $ref: "#/components/schemas/CommandAck" },
    },
  },
  AgentSeatRequest: {
    type: "object",
    description:
      "Open an agent seat. Omit `playerId` to join a new agent-controlled seat; pass the host's own player id to convert that existing seat into an agent seat. No other seat may be delegated.",
    additionalProperties: false,
    properties: {
      name: { type: "string" },
      color: {
        type: "string",
        description: "Optional colour request; assigned conflict-safely as on JoinGameRequest.",
      },
      playerId: {
        type: "string",
        description:
          "Must equal the authenticated host capability's own playerId. Any other value is rejected with FORBIDDEN.",
      },
      commandId: { type: "string" },
    },
  },
  AgentSeatResponse: {
    type: "object",
    description:
      "The one place an agent capability is minted. Returned once, `Cache-Control: no-store`; the token never appears in a URL path or query.",
    required: ["seat", "instructions"],
    properties: {
      seat: {
        type: "object",
        required: ["origin", "gameId", "playerId", "name", "color", "token", "urls"],
        properties: {
          origin: { type: "string" },
          gameId: { type: "string" },
          playerId: { type: "string" },
          name: { type: "string" },
          color: { type: "string" },
          token: { type: "string", description: "Bearer capability; header use only." },
          urls: {
            type: "object",
            description: "The complete four-endpoint agent surface, as origin-relative paths.",
            required: ["map", "actions", "decision", "commands"],
            properties: {
              map: { type: "string" },
              actions: { type: "string" },
              decision: { type: "string" },
              commands: { type: "string" },
            },
          },
        },
      },
      instructions: {
        type: "string",
        description: "Pasteable seat instructions generated from the same descriptor.",
      },
    },
  },
  AgentMessage: {
    description:
      "One message on a player's durable, replay-safe actions stream. `ActionRequired` always wants exactly one command in response and is self-sufficient; `GameOver` is terminal and wants none.",
    oneOf: [
      {
        type: "object",
        required: [
          "type",
          "messageId",
          "seq",
          "gameId",
          "playerId",
          "reason",
          "turn",
          "mode",
          "pendingInteraction",
          "legalMoves",
          "board",
          "since",
          "eventOffset",
        ],
        properties: {
          type: { const: "ActionRequired" },
          messageId: {
            type: "string",
            description: "`act:<gameId>:<playerId>:<seq>`; stable across a stream rebuild.",
          },
          seq: { type: "integer", minimum: 1, description: "Dense, 1-based, per player." },
          gameId: { type: "string" },
          playerId: { type: "string" },
          reason: {
            enum: [
              "turn-started",
              "phase-changed",
              "reinforcement-remaining",
              "attack-resolved",
              "occupation-required",
              "defense-required",
            ],
            description:
              "Why an action is needed now. A reinforcement that empties the pool reports `phase-changed`, not `reinforcement-remaining`.",
          },
          turn: {
            type: "object",
            required: ["id", "round", "phase", "activePlayerId", "reinforcement"],
            properties: {
              id: { type: "string" },
              round: { type: "integer" },
              phase: { enum: ["reinforce", "attack", "fortify"] },
              activePlayerId: { type: "string" },
              reinforcement,
            },
          },
          mode: { enum: ["active-turn", "defense"] },
          pendingInteraction: {
            oneOf: [pendingInteraction, { type: "null" }],
            description:
              "Null for an agent seat's own defence: that is server-resolved, and its outcome arrives in `since.events`.",
          },
          legalMoves: {
            type: "array",
            minItems: 1,
            items: legalAction,
            description: "Exactly the moves that may be submitted in response to this message.",
          },
          board: {
            type: "object",
            required: ["territories", "players"],
            properties: {
              territories: {
                type: "array",
                items: {
                  type: "object",
                  required: ["id", "ownerId", "armies"],
                  properties: {
                    id: { type: "string" },
                    ownerId: { type: ["string", "null"] },
                    armies: { type: "integer" },
                  },
                },
              },
              players: {
                type: "array",
                items: {
                  type: "object",
                  required: ["id", "eliminated"],
                  properties: {
                    id: { type: "string" },
                    eliminated: { type: "boolean" },
                  },
                },
              },
            },
          },
          since: {
            type: "object",
            description:
              "Every canonical event since this player's previous message. Chains without gaps: `fromEventOffset` is the predecessor's `eventOffset`, or null for the first message.",
            required: ["fromEventOffset", "events"],
            properties: {
              fromEventOffset: { type: ["string", "null"] },
              events: { type: "array", items: { type: "object" } },
            },
          },
          eventOffset: {
            type: "string",
            description:
              "Canonical offset this message was derived at. Not the stream cursor — use `nextOffset` for that.",
          },
        },
      },
      {
        type: "object",
        required: [
          "type",
          "messageId",
          "seq",
          "gameId",
          "playerId",
          "winner",
          "since",
          "eventOffset",
        ],
        properties: {
          type: { const: "GameOver" },
          messageId: { type: "string" },
          seq: { type: "integer", minimum: 1 },
          gameId: { type: "string" },
          playerId: { type: "string" },
          winner: {
            type: "object",
            required: ["id", "name"],
            properties: { id: { type: "string" }, name: { type: "string" } },
          },
          since: {
            type: "object",
            required: ["fromEventOffset", "events"],
            properties: {
              fromEventOffset: { type: ["string", "null"] },
              events: { type: "array", items: { type: "object" } },
            },
          },
          eventOffset: { type: "string" },
        },
      },
    ],
  },
  AgentActionsPage: {
    type: "object",
    required: ["messages", "nextOffset", "upToDate"],
    properties: {
      messages: { type: "array", items: { $ref: "#/components/schemas/AgentMessage" } },
      nextOffset: {
        type: "string",
        description:
          "Opaque cursor to send as `offset` on the next read. Always returned, including on an empty bounded wait, so resume after a crash is exact.",
      },
      upToDate: { type: "boolean" },
    },
  },
} as const;

function jsonResponse(schemaRef: string) {
  return {
    content: {
      "application/json": {
        schema: { $ref: `#/components/schemas/${schemaRef}` },
      },
    },
  };
}

function jsonRequest(schemaRef: string) {
  return {
    content: {
      "application/json": {
        schema: { $ref: `#/components/schemas/${schemaRef}` },
      },
    },
  };
}

/**
 * Path parameters, declared once and referenced by every path that templates
 * them. Every operation under a path item inherits the item's `parameters`, so a
 * route cannot document its body while leaving the identifiers in its own URL
 * undescribed.
 */
const parameters = {
  GameId: {
    name: "gameId",
    in: "path",
    required: true,
    description: "Game identifier, as returned by POST /v1/games.",
    schema: { type: "string" },
  },
  PlayerId: {
    name: "playerId",
    in: "path",
    required: true,
    description: "Seat identifier, as it appears in the projected board's players.",
    schema: { type: "string" },
  },
} as const;

const gameIdParameter = [{ $ref: "#/components/parameters/GameId" }] as const;
const gameAndPlayerParameters = [
  { $ref: "#/components/parameters/GameId" },
  { $ref: "#/components/parameters/PlayerId" },
] as const;

/** The rejection shape, described once per status a route actually returns. */
function errorResponse(description: string) {
  return { description, ...jsonResponse("ErrorResponse") };
}

export const openApiDocument = {
  openapi: "3.1.0",
  info: {
    title: "Streamsy Risk demo — command & capability API",
    version: "1.0.0",
    description:
      "Event-sourced Hex Domination. Commands validate against canonical history and CAS-append; the board is a separate causally-watermarked projection over a procedural hex map with two-stage combat and timed defence interrupts.",
  },
  tags: [
    { name: "agent", description: "The complete four-endpoint playing-agent surface." },
    { name: "lobby" },
    { name: "browser" },
  ],
  paths: {
    "/v1/games": {
      post: {
        tags: ["lobby"],
        summary: "Create a game; returns the host player and a one-time host capability.",
        requestBody: jsonRequest("CreateGameRequest"),
        responses: {
          "201": {
            description:
              "Game, host identity, one-time host capability, and the CommandAck for the create.",
          },
        },
      },
    },
    "/v1/games/{gameId}/players": {
      parameters: gameIdParameter,
      post: {
        tags: ["lobby"],
        summary: "Join a game; returns the player and a one-time player capability.",
        requestBody: jsonRequest("JoinGameRequest"),
        responses: {
          "201": {
            description:
              "Player identity, one-time player capability, and the CommandAck for the join.",
          },
        },
      },
    },
    "/v1/games/{gameId}/players/me": {
      parameters: gameIdParameter,
      delete: {
        tags: ["lobby"],
        summary:
          "Give up this capability's own seat (lobby only, human-controlled seats only). Scoped to `me`, so no request shape removes another player. A host that leaves keeps its host capability and the lobby it opened.",
        responses: {
          "200": {
            description: "The seat is gone from canonical history.",
            ...jsonResponse("LeaveGameResponse"),
          },
          "400": errorResponse("UNKNOWN_PLAYER — this capability's seat is already given up."),
          "401": errorResponse("UNAUTHORIZED — missing or invalid bearer capability."),
          "403": errorResponse(
            "FORBIDDEN for an agent capability; WRONG_GAME for a capability scoped to another game.",
          ),
          "404": errorResponse("GAME_NOT_FOUND — no such game."),
          "409": errorResponse(
            "GAME_ALREADY_STARTED once the game is under way; ILLEGAL_ACTION for a seat something other than a person plays.",
          ),
        },
      },
    },
    "/v1/games/{gameId}/players/{playerId}": {
      parameters: gameAndPlayerParameters,
      patch: {
        tags: ["lobby"],
        summary:
          "Rename a seat (lobby only). Permitted for the seat's own capability, and for the host on an agent seat it opened — never on another person's seat.",
        requestBody: jsonRequest("RenamePlayerRequest"),
        responses: {
          "200": {
            description: "The seat's name as canonical history now records it.",
            ...jsonResponse("RenamePlayerResponse"),
          },
          "400": errorResponse(
            "BAD_REQUEST when the body carries no `name`; INVALID_NAME when it trims to nothing; UNKNOWN_PLAYER when the caller's own seat is no longer in the game.",
          ),
          "401": errorResponse("UNAUTHORIZED — missing or invalid bearer capability."),
          "403": errorResponse(
            "FORBIDDEN for an agent capability, and for any caller renaming a seat that is neither its own nor an agent seat it hosts; WRONG_GAME for a capability scoped to another game.",
          ),
          "404": errorResponse("GAME_NOT_FOUND, or NOT_FOUND when the named seat is not in it."),
          "409": errorResponse("GAME_ALREADY_STARTED — names are fixed once the board is dealt."),
        },
      },
    },
    "/v1/games/{gameId}/agent-seats": {
      parameters: gameIdParameter,
      post: {
        tags: ["lobby"],
        summary:
          "Open a new agent seat, or convert the host's own seat into one (host capability required). This is the only place an agent capability is minted.",
        requestBody: jsonRequest("AgentSeatRequest"),
        responses: {
          "201": jsonResponse("AgentSeatResponse"),
          "403": jsonResponse("ErrorResponse"),
        },
      },
    },
    "/v1/games/{gameId}/start": {
      parameters: gameIdParameter,
      post: {
        summary: "Start the game (host capability required).",
        responses: { "200": jsonResponse("CommandAck") },
      },
    },
    "/v1/games/{gameId}": {
      parameters: gameIdParameter,
      get: {
        summary: "Game metadata and status.",
        responses: { "200": jsonResponse("Board") },
      },
    },
    "/v1/games/{gameId}/board": {
      parameters: gameIdParameter,
      get: {
        summary: "Projected board plus its canonical sourceThroughOffset watermark.",
        responses: { "200": jsonResponse("Board") },
      },
    },
    "/v1/games/{gameId}/map": {
      parameters: gameIdParameter,
      get: {
        tags: ["agent"],
        summary:
          "Immutable map geometry, names, adjacency, continents, and bonuses. Available once the game has started; 409 GAME_NOT_STARTED before that.",
        responses: {
          "200": { description: "Immutable map document." },
          "409": jsonResponse("ErrorResponse"),
        },
      },
    },
    "/v1/games/{gameId}/decision": {
      parameters: gameIdParameter,
      get: {
        tags: ["agent"],
        summary: "Full player-relative snapshot for bootstrap and recovery.",
        responses: {
          "200": jsonResponse("DecisionContext"),
        },
      },
    },
    "/v1/games/{gameId}/commands": {
      parameters: gameIdParameter,
      post: {
        tags: ["agent"],
        summary: "Submit a typed command (player capability). Idempotent by commandId.",
        requestBody: jsonRequest("GameCommand"),
        responses: {
          "200": jsonResponse("CommandAck"),
          "400": jsonResponse("ErrorResponse"),
          "409": jsonResponse("ErrorResponse"),
        },
      },
    },
    "/v1/games/{gameId}/players/me/actions": {
      parameters: gameIdParameter,
      get: {
        tags: ["agent"],
        summary: "Follow this player's durable self-sufficient action-required stream.",
        parameters: [
          {
            name: "offset",
            in: "query",
            required: false,
            description:
              "Opaque nextOffset returned by the previous actions response. Omit initially.",
            schema: { type: "string" },
          },
          {
            name: "wait",
            in: "query",
            required: false,
            description:
              "Maximum long-poll duration in milliseconds. Omit or use 0 for an immediate read.",
            schema: { type: "integer", minimum: 0, maximum: 30000 },
          },
        ],
        responses: {
          "200": jsonResponse("AgentActionsPage"),
          "400": jsonResponse("ErrorResponse"),
        },
      },
    },
  },
  components: { schemas, parameters },
} as const;

export const jsonSchemas = schemas;
