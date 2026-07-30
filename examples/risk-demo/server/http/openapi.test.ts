/** Contract tests for the sole Hex Domination command vocabulary. */

import { describe, expect, it } from "vitest";

import { jsonSchemas, openApiDocument } from "./openapi.ts";
import type { RiskErrorCode } from "../../src/domain/commands.ts";
import { RULES } from "../../src/domain/map.ts";

function actionTypes(schema: {
  oneOf: ReadonlyArray<{ properties: { type: { const: string } } }>;
}) {
  return schema.oneOf.map((variant) => variant.properties.type.const);
}

describe("published OpenAPI contract", () => {
  it("is a 3.1 document with every route documented", () => {
    expect(openApiDocument.openapi).toMatch(/^3\.1/);
    expect(Object.keys(openApiDocument.paths).toSorted()).toEqual([
      "/v1/games",
      "/v1/games/{gameId}",
      "/v1/games/{gameId}/agent-seats",
      "/v1/games/{gameId}/board",
      "/v1/games/{gameId}/commands",
      "/v1/games/{gameId}/decision",
      "/v1/games/{gameId}/map",
      "/v1/games/{gameId}/players",
      "/v1/games/{gameId}/players/me",
      "/v1/games/{gameId}/players/me/actions",
      "/v1/games/{gameId}/players/{playerId}",
      "/v1/games/{gameId}/start",
    ]);
  });

  it("publishes the complete command vocabulary", () => {
    const current = actionTypes(jsonSchemas.GameCommand.properties.action);
    expect(current).toEqual([
      "reinforce",
      "declare-attack",
      "roll-defense",
      "occupy-territory",
      "fortify",
      "skip-fortifications",
    ]);
    expect(current).not.toContain("attack");
    expect(current).not.toContain("end-turn");
    // The internal timeout resolver is never a player action.
    expect(current).not.toContain("resolve-defense-timeout");
  });

  it("publishes current reinforcement as one complete allocation command", () => {
    const reinforce = jsonSchemas.GameCommand.properties.action.oneOf.find(
      (variant: any) => variant.properties.type.const === "reinforce",
    ) as any;
    expect(reinforce.required).toEqual(["type", "placements"]);
    expect(reinforce.properties.placements.minItems).toBe(1);
    expect(reinforce.properties.placements.items.required).toEqual(["territoryId", "armies"]);
    expect(reinforce.description).toContain("complete reinforcement-turn allocation");
  });

  it("publishes one board and decision shape", () => {
    for (const path of ["/v1/games/{gameId}/board", "/v1/games/{gameId}/decision"] as const) {
      const schema = (openApiDocument.paths[path].get.responses["200"] as any).content[
        "application/json"
      ].schema;
      expect(schema.$ref).toMatch(/^#\/components\/schemas\//);
    }
  });

  it("describes the current board's zero-or-one turn and combat rows", () => {
    expect(jsonSchemas.Board.properties.turn.type).toEqual(["object", "null"]);
    expect(jsonSchemas.Board.properties.combat.type).toEqual(["object", "null"]);
    expect(jsonSchemas.Board.properties.combat.properties.status.enum).toEqual([
      "awaiting-defense",
      "awaiting-occupation",
    ]);
    expect(jsonSchemas.Board.properties.combat.properties.resolutionSource.enum).toEqual([
      "human",
      "bot",
      "agent",
      "timeout",
    ]);
  });

  it("reserves the public agent controller for external harnesses and names bots explicitly", () => {
    expect(jsonSchemas.SeatControllerInput.enum).toEqual(["human", "bot", "agent"]);
    expect(jsonSchemas.SeatControllerInput.description).toContain("external coding-agent");
    expect(
      (openApiDocument.paths["/v1/games"].post.requestBody as any).content["application/json"]
        .schema.$ref,
    ).toBe("#/components/schemas/CreateGameRequest");
    expect(
      (openApiDocument.paths["/v1/games/{gameId}/players"].post.requestBody as any).content[
        "application/json"
      ].schema.$ref,
    ).toBe("#/components/schemas/JoinGameRequest");
  });

  it("documents `roll-defense` as an out-of-turn legal action with a deadline", () => {
    const rollDefense = jsonSchemas.DecisionContext.properties.legalMoves.items.oneOf.find(
      (variant: any) => variant.properties.type.const === "roll-defense",
    ) as any;
    expect(rollDefense.required).toEqual(["type", "attackId", "dice", "deadlineAt", "submit"]);
  });

  it("publishes every stable rejection code", () => {
    const published = new Set<string>(
      jsonSchemas.ErrorResponse.properties.error.properties.code.enum,
    );
    const coreCodes: RiskErrorCode[] = [
      "NOT_YOUR_TURN",
      "STALE_TURN",
      "INVALID_PHASE",
      "ILLEGAL_ACTION",
      "INSUFFICIENT_ARMIES",
      "NOT_ADJACENT",
      "UNKNOWN_TERRITORY",
      "COMMAND_ID_REUSED",
    ];
    const combatCodes: RiskErrorCode[] = [
      "PENDING_DEFENSE",
      "PENDING_OCCUPATION",
      "NOT_DEFENDING_PLAYER",
      "ATTACK_ID_MISMATCH",
      "ATTACK_ALREADY_RESOLVED",
      "DEFENSE_DEADLINE_EXPIRED",
      "INVALID_OCCUPATION",
      "NO_FRIENDLY_PATH",
      "MAP_GENERATION_FAILED",
    ];
    for (const code of [...coreCodes, ...combatCodes]) expect(published.has(code)).toBe(true);
  });

  it("publishes exactly four agent-tagged endpoints and no token paths", () => {
    const agentPaths = Object.entries(openApiDocument.paths)
      .filter(([, item]) =>
        Object.values(item).some(
          (operation: any) => Array.isArray(operation.tags) && operation.tags.includes("agent"),
        ),
      )
      .map(([path]) => path)
      .toSorted();
    expect(agentPaths).toEqual(
      [
        "/v1/games/{gameId}/commands",
        "/v1/games/{gameId}/decision",
        "/v1/games/{gameId}/map",
        "/v1/games/{gameId}/players/me/actions",
      ].toSorted(),
    );
    expect(Object.keys(openApiDocument.paths).every((path) => !path.includes("token"))).toBe(true);
  });

  it("declares a path parameter for every identifier it templates", () => {
    // A route that documented its body but not the ids in its own URL would be
    // undiscoverable from the document alone, which is the whole point of it.
    for (const [path, item] of Object.entries(openApiDocument.paths)) {
      const templated = [...path.matchAll(/\{(\w+)\}/g)].map((match) => match[1]);
      const declared = ((item as any).parameters ?? []).map((parameter: any) =>
        parameter.$ref.replace("#/components/parameters/", ""),
      );
      const resolved = declared.map(
        (name: string) => (openApiDocument.components.parameters as any)[name],
      );
      expect(resolved.map((parameter: any) => parameter.name)).toEqual(templated);
      for (const parameter of resolved) {
        expect(parameter).toMatchObject({ in: "path", required: true, schema: { type: "string" } });
      }
    }
  });

  it("bounds a seat rename by the rule the decider enforces", () => {
    const name = jsonSchemas.RenamePlayerRequest.properties.name;
    expect(jsonSchemas.RenamePlayerRequest.required).toEqual(["name"]);
    expect(name.minLength).toBe(1);
    // Published from RULES, so the document cannot drift from the enforced bound.
    expect(name.maxLength).toBe(RULES.maxPlayerNameLength);
    expect(jsonSchemas.RenamePlayerRequest.description).toContain("INVALID_NAME");
  });

  it("documents what the roster routes actually answer, not just their happy path", () => {
    const rename = openApiDocument.paths["/v1/games/{gameId}/players/{playerId}"].patch;
    const leave = openApiDocument.paths["/v1/games/{gameId}/players/me"].delete;
    expect(Object.keys(rename.responses)).toEqual(["200", "400", "401", "403", "404", "409"]);
    expect(Object.keys(leave.responses)).toEqual(["200", "400", "401", "403", "404", "409"]);

    for (const operation of [rename, leave]) {
      for (const [status, response] of Object.entries(operation.responses)) {
        // Every documented status names a schema and says something specific
        // about when it happens; a bare `{ "403": {} }` documents nothing.
        const schema = (response as any).content["application/json"].schema.$ref;
        if (status === "200") expect(schema).toMatch(/^#\/components\/schemas\/(Rename|Leave)/);
        else expect(schema).toBe("#/components/schemas/ErrorResponse");
        expect((response as any).description.length).toBeGreaterThan(20);
      }
    }
    // The specific rejections each route is documented to distinguish.
    expect((rename.responses["403"] as any).description).toContain("agent seat it hosts");
    expect((rename.responses["409"] as any).description).toContain("GAME_ALREADY_STARTED");
    expect((leave.responses["409"] as any).description).toContain("ILLEGAL_ACTION");
  });

  it("documents the actions offset and bounded wait query parameters", () => {
    const parameters =
      openApiDocument.paths["/v1/games/{gameId}/players/me/actions"].get.parameters;
    expect(parameters.map((parameter) => parameter.name)).toEqual(["offset", "wait"]);
    expect(parameters[0]).toMatchObject({ in: "query", required: false });
    expect(parameters[1]).toMatchObject({
      in: "query",
      required: false,
      schema: { type: "integer", minimum: 0, maximum: 30000 },
    });
  });
});
