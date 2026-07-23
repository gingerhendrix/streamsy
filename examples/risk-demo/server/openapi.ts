/**
 * OpenAPI 3.1 document + JSON Schemas for the Risk command/decision API.
 *
 * Served at `GET /openapi.json`. A coding agent can discover every command
 * shape, ack, decision context, and error code from this document without
 * reading any UI or server source.
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
    { type: "object", required: ["type"], properties: { type: { const: "end-turn" } } },
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
    { type: "object", required: ["type"], properties: { type: { const: "end-turn" } } },
  ],
} as const;

const schemas = {
  GameCommand: {
    type: "object",
    required: ["commandId", "turnId", "action"],
    properties: {
      commandId: {
        type: "string",
        description: "Stable idempotency key; reuse on transport retry.",
      },
      turnId: { type: "string", description: "Observed turn precondition, e.g. round-2:p1." },
      action: commandAction,
    },
  },
  CommandAck: {
    type: "object",
    required: ["status", "commandId", "sourceStreamId", "sourceOffset", "txid", "events"],
    properties: {
      status: { enum: ["accepted", "duplicate"] },
      commandId: { type: "string" },
      sourceStreamId: { type: "string", description: "Canonical event stream id." },
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
          code: {
            enum: [
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
              "UNAUTHORIZED",
              "FORBIDDEN",
              "WRONG_GAME",
              "NOT_FOUND",
              "BAD_REQUEST",
              "PROJECTION_UNAVAILABLE",
              "INTERNAL",
            ],
          },
          message: { type: "string" },
          currentTurnId: { type: "string" },
        },
      },
    },
  },
  DecisionContext: {
    type: "object",
    required: ["gameId", "player", "turn", "board", "legalActions"],
    properties: {
      gameId: { type: "string" },
      player: {
        type: "object",
        required: ["id", "name", "color"],
        properties: { id: { type: "string" }, name: { type: "string" }, color: { type: "string" } },
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
  Board: {
    type: "object",
    required: ["gameId", "sourceStreamId", "game", "players", "territories"],
    properties: {
      gameId: { type: "string" },
      sourceStreamId: { type: "string" },
      sourceThroughOffset: { type: ["string", "null"] },
      game: { type: "object" },
      players: { type: "array", items: { type: "object" } },
      territories: { type: "array", items: territory },
    },
  },
} as const;

function jsonResponse(schemaRef: string) {
  return {
    content: { "application/json": { schema: { $ref: `#/components/schemas/${schemaRef}` } } },
  };
}

export const openApiDocument = {
  openapi: "3.1.0",
  info: {
    title: "Streamsy Risk demo — command & capability API",
    version: "1.0.0",
    description:
      "Event-sourced Risk. Commands validate against canonical history and CAS-append; the board is a separate causally-watermarked projection.",
  },
  paths: {
    "/v1/games": {
      post: {
        summary: "Create a game; returns the host player and a one-time host capability.",
        responses: { "201": jsonResponse("CommandAck") },
      },
    },
    "/v1/games/{gameId}/players": {
      post: {
        summary: "Join a game; returns the player and a one-time player capability.",
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
      get: { summary: "Game metadata and status.", responses: { "200": jsonResponse("Board") } },
    },
    "/v1/games/{gameId}/board": {
      get: {
        summary: "Projected board plus its canonical sourceThroughOffset watermark.",
        responses: { "200": jsonResponse("Board") },
      },
    },
    "/v1/games/{gameId}/decision": {
      get: {
        summary: "Fresh agent decision context and legal actions (player capability).",
        responses: { "200": jsonResponse("DecisionContext") },
      },
    },
    "/v1/games/{gameId}/commands": {
      post: {
        summary: "Submit a typed command (player capability). Idempotent by commandId.",
        requestBody: jsonResponse("GameCommand"),
        responses: {
          "200": jsonResponse("CommandAck"),
          "409": jsonResponse("ErrorResponse"),
        },
      },
    },
    "/v1/games/{gameId}/commands/{commandId}": {
      get: {
        summary: "Recover a previously accepted or rejected command result.",
        responses: { "200": jsonResponse("CommandAck") },
      },
    },
  },
  components: { schemas },
} as const;

export const jsonSchemas = schemas;
