/**
 * OpenAPI 3.1 document + JSON Schemas for the Risk command/decision API.
 *
 * Served at `GET /openapi.json`. A coding agent can discover every command
 * shape, ack, decision context, and error code from this document without
 * reading any UI or server source.
 *
 * The two rulesets are published as **version-discriminated** types, never as a
 * single merged shape (design spec §11). `risk-demo-v1` `attack` is one
 * fight-and-occupy step; `risk-demo-v2` `declare-attack` is one throw that opens
 * a defence interrupt someone else must close. A client that guessed they were
 * the same command would be wrong about who moves next, so the schemas keep them
 * apart and the `ruleset` field on `GET /v1/games/{gameId}` says which applies.
 */

const territory = {
  type: "object",
  required: ["id", "armies"],
  properties: {
    id: { type: "string" },
    ownerId: { type: "string" },
    armies: { type: "integer" },
    adjacentTerritoryIds: { type: "array", items: { type: "string" } },
  },
} as const;

const legalAction = {
  oneOf: [
    {
      type: "object",
      required: ["type", "territoryIds", "minArmies", "maxArmies"],
      properties: {
        type: { const: "reinforce" },
        territoryIds: { type: "array", items: { type: "string" } },
        minArmies: { type: "integer" },
        maxArmies: { type: "integer" },
      },
    },
    {
      type: "object",
      required: ["type", "choices"],
      properties: {
        type: { const: "attack" },
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
      },
    },
    {
      type: "object",
      required: ["type", "choices"],
      properties: {
        type: { const: "fortify" },
        choices: {
          type: "array",
          items: {
            type: "object",
            required: ["from", "to", "maxArmies"],
            properties: {
              from: { type: "string" },
              to: { type: "string" },
              maxArmies: { type: "integer" },
            },
          },
        },
      },
    },
    {
      type: "object",
      required: ["type"],
      properties: { type: { const: "end-turn" } },
    },
  ],
} as const;

const commandAction = {
  oneOf: [
    {
      type: "object",
      required: ["type", "territoryId", "armies"],
      properties: {
        type: { const: "reinforce" },
        territoryId: { type: "string" },
        armies: { type: "integer", minimum: 1 },
      },
    },
    {
      type: "object",
      required: ["type", "from", "to", "attackerDice"],
      description: "risk-demo-v1 only: rolls both sides and, on a capture, occupies in one step.",
      properties: {
        type: { const: "attack" },
        from: { type: "string" },
        to: { type: "string" },
        attackerDice: { type: "integer", minimum: 1, maximum: 3 },
      },
    },
    {
      type: "object",
      required: ["type", "from", "to", "armies"],
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
      properties: { type: { const: "end-turn" } },
    },
  ],
} as const;

// ---------------------------------------------------------------------------
// risk-demo-v2
// ---------------------------------------------------------------------------

const commandActionV2 = {
  oneOf: [
    {
      type: "object",
      required: ["type", "territoryId", "armies"],
      properties: {
        type: { const: "reinforce" },
        territoryId: { type: "string" },
        armies: { type: "integer", minimum: 1 },
      },
    },
    {
      type: "object",
      required: ["type", "from", "to", "attackerDice"],
      description:
        "One throw. Rolls the attacker's dice and opens a defence interrupt; it is NOT the v1 `attack` action renamed.",
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
      properties: { type: { const: "end-turn" } },
    },
  ],
} as const;

const legalActionV2 = {
  oneOf: [
    {
      type: "object",
      required: ["type", "territoryIds", "minArmies", "maxArmies"],
      properties: {
        type: { const: "reinforce" },
        territoryIds: { type: "array", items: { type: "string" } },
        minArmies: { type: "integer" },
        maxArmies: { type: "integer" },
      },
    },
    {
      type: "object",
      required: ["type", "choices"],
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
      },
    },
    {
      type: "object",
      required: ["type", "attackId", "dice", "deadlineAt"],
      properties: {
        type: { const: "roll-defense" },
        attackId: { type: "string" },
        dice: { type: "integer" },
        deadlineAt: {
          type: "integer",
          description: "Canonical epoch-ms defence deadline.",
        },
      },
    },
    {
      type: "object",
      required: ["type", "attackId", "from", "to", "minArmies", "maxArmies"],
      properties: {
        type: { const: "occupy-territory" },
        attackId: { type: "string" },
        from: { type: "string" },
        to: { type: "string" },
        minArmies: { type: "integer" },
        maxArmies: { type: "integer" },
      },
    },
    {
      type: "object",
      required: ["type", "choices"],
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
      },
    },
    {
      type: "object",
      required: ["type"],
      properties: { type: { const: "end-turn" } },
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
  "COLOR_TAKEN",
  "MAP_GENERATION_FAILED",
  // risk-demo-v2 two-stage combat
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
  "PROJECTION_UNAVAILABLE",
  "INTERNAL",
] as const;

const schemas = {
  SeatControllerInput: {
    type: "string",
    enum: ["human", "bot", "agent"],
    description:
      "`agent` reserves the seat for an external coding-agent harness using its private seat URL. `bot` is the repository's deterministic scripted policy. Persisted pre-migration `agent` events are read as `bot`.",
  },
  CreateGameRequest: {
    type: "object",
    properties: {
      name: { type: "string" },
      color: { type: "string" },
      commandId: { type: "string" },
      ruleset: { enum: ["risk-demo-v1", "risk-demo-v2"] },
      controller: { $ref: "#/components/schemas/SeatControllerInput" },
      mapSeed: { type: "string" },
    },
  },
  JoinGameRequest: {
    type: "object",
    properties: {
      name: { type: "string" },
      color: { type: "string" },
      commandId: { type: "string" },
      controller: { $ref: "#/components/schemas/SeatControllerInput" },
    },
  },
  GameCommand: {
    type: "object",
    description: "risk-demo-v1 play command.",
    required: ["commandId", "turnId", "action"],
    properties: {
      commandId: {
        type: "string",
        description: "Stable idempotency key; reuse on transport retry.",
      },
      turnId: {
        type: "string",
        description: "Observed turn precondition, e.g. round-2:p1.",
      },
      action: commandAction,
    },
  },
  GameCommandV2: {
    type: "object",
    description:
      "risk-demo-v2 play command. `roll-defense` is the one legal out-of-turn action; the internal timeout resolver is never exposed here.",
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
      action: commandActionV2,
    },
  },
  CommandAck: {
    type: "object",
    required: ["status", "commandId", "sourceStreamId", "sourceOffset", "txid", "events"],
    properties: {
      status: { enum: ["accepted", "duplicate"] },
      commandId: { type: "string" },
      sourceStreamId: {
        type: "string",
        description: "Canonical event stream id.",
      },
      sourceOffset: {
        type: "string",
        description: "Committed final canonical offset of the batch.",
      },
      txid: {
        type: "string",
        description: "Identity of the command’s final board-projection transition.",
      },
      events: { type: "array", items: { type: "object" } },
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
        },
      },
    },
  },
  DecisionContext: {
    type: "object",
    description: "risk-demo-v1 decision context (active player only).",
    required: ["gameId", "player", "turn", "board", "legalActions"],
    properties: {
      gameId: { type: "string" },
      player: {
        type: "object",
        required: ["id", "name", "color"],
        properties: {
          id: { type: "string" },
          name: { type: "string" },
          color: { type: "string" },
        },
      },
      turn: {
        type: "object",
        required: ["id", "round", "phase"],
        properties: {
          id: { type: "string" },
          round: { type: "integer" },
          activePlayerId: { type: "string" },
          phase: { enum: ["setup", "reinforce", "attack", "fortify"] },
        },
      },
      board: {
        type: "object",
        required: ["sourceStreamId", "territories", "players"],
        properties: {
          sourceStreamId: { type: "string" },
          sourceThroughOffset: { type: ["string", "null"] },
          territories: { type: "array", items: territory },
          players: { type: "array", items: { type: "object" } },
        },
      },
      legalActions: { type: "array", items: legalAction },
    },
  },
  DecisionContextV2: {
    type: "object",
    description:
      "risk-demo-v2 decision context. Player-relative: an out-of-turn defender gets `roll-defense` here. Derived from canonical history through `board.sourceThroughOffset`, which the named board generation has already materialized — so the decision is never ahead of its board snapshot.",
    required: ["gameId", "ruleset", "player", "mode", "turn", "board", "legalActions"],
    properties: {
      gameId: { type: "string" },
      ruleset: { const: "risk-demo-v2" },
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
      legalActions: { type: "array", items: legalActionV2 },
    },
  },
  Board: {
    type: "object",
    description: "risk-demo-v1 projected board plus its causal watermark.",
    required: ["gameId", "sourceStreamId", "game", "players", "territories"],
    properties: {
      gameId: { type: "string" },
      ruleset: { const: "risk-demo-v1" },
      sourceStreamId: { type: "string" },
      sourceThroughOffset: { type: ["string", "null"] },
      generation: { type: "string" },
      game: { type: "object" },
      players: { type: "array", items: { type: "object" } },
      territories: { type: "array", items: territory },
    },
  },
  BoardV2: {
    type: "object",
    description:
      "risk-demo-v2 projected board. Static map rows (hexes/territories/continents) are served here once; `turn` and `combat` are zero-or-one current rows.",
    required: [
      "gameId",
      "ruleset",
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
      ruleset: { const: "risk-demo-v2" },
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
  PlayerActionNotification: {
    description:
      "Derived, rebuildable wake hints on the per-player action stream. Never a correctness channel: a missed wake is covered by polling and, for combat, by the canonical timeout.",
    oneOf: [
      {
        type: "object",
        required: ["type", "notificationId", "gameId", "playerId", "turnId", "round", "phase"],
        properties: {
          type: { const: "TurnAvailable" },
          notificationId: { type: "string" },
          gameId: { type: "string" },
          playerId: { type: "string" },
          turnId: { type: "string" },
          round: { type: "integer" },
          phase: { const: "reinforce" },
          causedBySourceOffset: { type: "string" },
          decisionUrl: { type: "string" },
        },
      },
      {
        type: "object",
        description: "risk-demo-v2 only: an out-of-turn defence is waiting on this player.",
        required: ["type", "notificationId", "gameId", "playerId", "turnId", "attackId"],
        properties: {
          type: { const: "DefenseAvailable" },
          notificationId: { type: "string" },
          gameId: { type: "string" },
          playerId: { type: "string" },
          turnId: { type: "string" },
          attackId: { type: "string" },
          deadlineAt: { type: "integer" },
          causedBySourceOffset: { type: "string" },
          decisionUrl: { type: "string" },
        },
      },
    ],
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

function eitherRuleset(v1: string, v2: string) {
  return {
    content: {
      "application/json": {
        schema: {
          oneOf: [{ $ref: `#/components/schemas/${v1}` }, { $ref: `#/components/schemas/${v2}` }],
        },
      },
    },
  };
}

export const openApiDocument = {
  openapi: "3.1.0",
  info: {
    title: "Streamsy Risk demo — command & capability API",
    version: "2.0.0",
    description:
      "Event-sourced Risk. Commands validate against canonical history and CAS-append; the board is a separate causally-watermarked projection. Two rulesets are published side by side: `risk-demo-v1` (fixed six-country map, single-step attack) and `risk-demo-v2` (procedural hex map, two-stage combat with a timed defence interrupt). `GET /v1/games/{gameId}` reports which one a game speaks; the v1 `attack` action is never reinterpreted as the v2 `declare-attack` action.",
  },
  paths: {
    "/agent/{token}/state": {
      get: {
        summary: "Compact personalized v2 state: named map, turn, and current legal moves.",
        parameters: [
          {
            name: "token",
            in: "path",
            required: true,
            description: "Player seat capability embedded in the personalized URL.",
            schema: { type: "string" },
          },
        ],
        responses: {
          "200": {
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  required: ["status", "player", "turn", "territories", "legalMoves"],
                },
              },
            },
          },
        },
      },
    },
    "/agent/{token}/wait": {
      get: {
        summary: "Cursor-free bounded wait; always refetch the personalized state after return.",
        parameters: [
          {
            name: "token",
            in: "path",
            required: true,
            schema: { type: "string" },
          },
          {
            name: "wait",
            in: "query",
            required: false,
            description: "Bounded wait in milliseconds, capped at 30000.",
            schema: { type: "integer", minimum: 0, maximum: 30000 },
          },
        ],
        responses: {
          "200": {
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  required: ["changed", "reason", "stateUrl"],
                },
              },
            },
          },
        },
      },
    },
    "/v1/games": {
      post: {
        summary:
          'Create a game; returns the host player and a one-time host capability. New games are `risk-demo-v2`; pass `ruleset: "risk-demo-v1"` for a legacy fixed-map game.',
        requestBody: jsonRequest("CreateGameRequest"),
        responses: { "201": jsonResponse("CommandAck") },
      },
    },
    "/v1/games/{gameId}/players": {
      post: {
        summary: "Join a game; returns the player and a one-time player capability.",
        requestBody: jsonRequest("JoinGameRequest"),
        responses: { "201": jsonResponse("CommandAck") },
      },
    },
    "/v1/games/{gameId}/start": {
      post: {
        summary: "Start the game (host capability required).",
        responses: { "200": jsonResponse("CommandAck") },
      },
    },
    "/v1/games/{gameId}": {
      get: {
        summary: "Game metadata, status, and the ruleset every other resource is shaped by.",
        responses: { "200": jsonResponse("Board") },
      },
    },
    "/v1/games/{gameId}/board": {
      get: {
        summary: "Projected board plus its canonical sourceThroughOffset watermark.",
        responses: { "200": eitherRuleset("Board", "BoardV2") },
      },
    },
    "/v1/games/{gameId}/decision": {
      get: {
        summary: "Fresh agent decision context and legal actions (player capability).",
        responses: {
          "200": eitherRuleset("DecisionContext", "DecisionContextV2"),
        },
      },
    },
    "/v1/games/{gameId}/commands": {
      post: {
        summary: "Submit a typed command (player capability). Idempotent by commandId.",
        requestBody: eitherRuleset("GameCommand", "GameCommandV2"),
        responses: {
          "200": jsonResponse("CommandAck"),
          "409": jsonResponse("ErrorResponse"),
        },
      },
    },
    "/v1/games/{gameId}/players/me/turns": {
      get: {
        summary: "Follow this player's durable action stream (player capability).",
        parameters: [
          {
            name: "offset",
            in: "query",
            required: false,
            description:
              "Opaque cursor returned by the previous turns response. Omit for the initial read.",
            schema: { type: "string" },
          },
          {
            name: "wait",
            in: "query",
            required: false,
            description:
              "Maximum long-poll duration in milliseconds. Omit or use 0 for an immediate read.",
            schema: { type: "integer", minimum: 0 },
          },
        ],
        responses: { "200": jsonResponse("PlayerActionNotification") },
      },
    },
  },
  components: { schemas },
} as const;

export const jsonSchemas = schemas;
