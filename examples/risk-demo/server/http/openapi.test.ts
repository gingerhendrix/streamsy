/**
 * The published contract must stay version-discriminated (design spec §11).
 *
 * The failure this guards against is subtle and expensive: a client that reads
 * one merged command schema and assumes v1 `attack` and v2 `declare-attack` are
 * the same action would be wrong about who moves next, because a v2 declaration
 * hands control to the *defender*. So the two rulesets get separate schemas, and
 * every stable rejection code either ruleset can emit is published.
 */

import { describe, expect, it } from "vitest";

import { jsonSchemas, openApiDocument } from "./openapi.ts";
import type { RiskErrorCode } from "../../src/domain/commands.ts";
import type { RiskErrorCodeV2 } from "../../src/domain/commands-v2.ts";

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
      "/v1/games/{gameId}/players/me/actions",
      "/v1/games/{gameId}/start",
    ]);
  });

  it("keeps v1 `attack` and v2 `declare-attack` as distinct actions", () => {
    const v1 = actionTypes(jsonSchemas.GameCommand.properties.action);
    const v2 = actionTypes(jsonSchemas.GameCommandV2.properties.action);

    expect(v1).toEqual(["reinforce", "attack", "fortify", "end-turn"]);
    expect(v2).toEqual([
      "reinforce",
      "declare-attack",
      "roll-defense",
      "occupy-territory",
      "fortify",
      "skip-fortifications",
    ]);
    // Neither vocabulary leaks into the other.
    expect(v1).not.toContain("declare-attack");
    expect(v2).not.toContain("attack");
    expect(v2).not.toContain("end-turn");
    // The internal timeout resolver is never a player action.
    expect(v2).not.toContain("resolve-defense-timeout");
  });

  it("publishes v2 reinforcement as one complete allocation command", () => {
    const reinforce = jsonSchemas.GameCommandV2.properties.action.oneOf.find(
      (variant: any) => variant.properties.type.const === "reinforce",
    ) as any;
    expect(reinforce.required).toEqual(["type", "placements"]);
    expect(reinforce.properties.placements.minItems).toBe(1);
    expect(reinforce.properties.placements.items.required).toEqual(["territoryId", "armies"]);
    expect(reinforce.description).toContain("complete reinforcement-turn allocation");
  });

  it("publishes both board and decision shapes under a ruleset discriminator", () => {
    expect(jsonSchemas.Board.properties.ruleset.const).toBe("risk-demo-v1");
    expect(jsonSchemas.BoardV2.properties.ruleset.const).toBe("risk-demo-v2");
    expect(jsonSchemas.DecisionContextV2.properties.ruleset.const).toBe("risk-demo-v2");

    for (const path of ["/v1/games/{gameId}/board", "/v1/games/{gameId}/decision"] as const) {
      const schema = (openApiDocument.paths[path].get.responses["200"] as any).content[
        "application/json"
      ].schema;
      expect(schema.oneOf).toHaveLength(2);
    }
  });

  it("describes the v2 board's zero-or-one turn and combat rows", () => {
    expect(jsonSchemas.BoardV2.properties.turn.type).toEqual(["object", "null"]);
    expect(jsonSchemas.BoardV2.properties.combat.type).toEqual(["object", "null"]);
    expect(jsonSchemas.BoardV2.properties.combat.properties.status.enum).toEqual([
      "awaiting-defense",
      "awaiting-occupation",
    ]);
    expect(jsonSchemas.BoardV2.properties.combat.properties.resolutionSource.enum).toEqual([
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
    const rollDefense = jsonSchemas.DecisionContextV2.properties.legalMoves.items.oneOf.find(
      (variant: any) => variant.properties.type.const === "roll-defense",
    ) as any;
    expect(rollDefense.required).toEqual(["type", "attackId", "dice", "deadlineAt", "submit"]);
  });

  it("publishes every stable rejection code both rulesets can emit", () => {
    const published = new Set<string>(
      jsonSchemas.ErrorResponse.properties.error.properties.code.enum,
    );
    const v1Codes: RiskErrorCode[] = [
      "NOT_YOUR_TURN",
      "STALE_TURN",
      "INVALID_PHASE",
      "ILLEGAL_ACTION",
      "INSUFFICIENT_ARMIES",
      "NOT_ADJACENT",
      "UNKNOWN_TERRITORY",
      "COMMAND_ID_REUSED",
    ];
    const v2Codes: RiskErrorCodeV2[] = [
      "PENDING_DEFENSE",
      "PENDING_OCCUPATION",
      "NOT_DEFENDING_PLAYER",
      "ATTACK_ID_MISMATCH",
      "ATTACK_ALREADY_RESOLVED",
      "DEFENSE_DEADLINE_EXPIRED",
      "INVALID_OCCUPATION",
      "NO_FRIENDLY_PATH",
      "MAP_GENERATION_FAILED",
      "COLOR_TAKEN",
    ];
    for (const code of [...v1Codes, ...v2Codes]) expect(published.has(code)).toBe(true);
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
