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
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated OpenAPI literal fixes this schema branch; the test intentionally traverses its structural union.
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
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated OpenAPI literal fixes this response branch; the test intentionally traverses its structural union.
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
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated OpenAPI literal fixes this request-body branch; the test intentionally traverses its structural union.
      (openApiDocument.paths["/v1/games"].post.requestBody as any).content["application/json"]
        .schema.$ref,
    ).toBe("#/components/schemas/CreateGameRequest");
    expect(
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated OpenAPI literal fixes this request-body branch; the test intentionally traverses its structural union.
      (openApiDocument.paths["/v1/games/{gameId}/players"].post.requestBody as any).content[
        "application/json"
      ].schema.$ref,
    ).toBe("#/components/schemas/JoinGameRequest");
  });

  it("documents `roll-defense` as an out-of-turn legal action with a deadline", () => {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated OpenAPI literal fixes this decision schema branch; the test intentionally traverses its structural union.
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
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The route item comes from the locally generated OpenAPI document and is checked structurally by this test.
      const declared = ((item as any).parameters ?? []).map((parameter: any) =>
        parameter.$ref.replace("#/components/parameters/", ""),
      );
      const resolved = declared.map(
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The parameter reference resolves against the locally generated OpenAPI components table under test.
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
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- This response is a fixed locally generated OpenAPI response object whose schema reference is the assertion target.
        const schema = (response as any).content["application/json"].schema.$ref;
        if (status === "200") expect(schema).toMatch(/^#\/components\/schemas\/(Rename|Leave)/);
        else expect(schema).toBe("#/components/schemas/ErrorResponse");
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- This response is a fixed locally generated OpenAPI response object whose description is the assertion target.
        expect((response as any).description.length).toBeGreaterThan(20);
      }
    }
    // The specific rejections each route is documented to distinguish.
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated rename response table has a declared 403 response checked here.
    expect((rename.responses["403"] as any).description).toContain("agent seat it hosts");
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated rename response table has a declared 409 response checked here.
    expect((rename.responses["409"] as any).description).toContain("GAME_ALREADY_STARTED");
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated leave response table has a declared 409 response checked here.
    expect((leave.responses["409"] as any).description).toContain("ILLEGAL_ACTION");
  });

  it("documents the actions resource as an SSE stream resumed by offset alone", () => {
    const actions = openApiDocument.paths["/v1/games/{gameId}/players/me/actions"].get;
    // `offset` is the whole parameter surface: the long poll it replaced is gone.
    expect(actions.parameters.map((parameter) => parameter.name)).toEqual(["offset"]);
    expect(actions.parameters[0]).toMatchObject({ in: "query", required: false });

    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The locally generated actions response table has a declared 200 response checked here.
    const ok = actions.responses["200"] as any;
    expect(Object.keys(ok.content)).toEqual(["text/event-stream", "application/json"]);
    expect(ok.content["application/json"].schema.$ref).toBe(
      "#/components/schemas/AgentActionsPage",
    );
    expect(actions.description).toContain("Server-Sent Events");
    expect(actions.description).toContain("30 seconds");
    // The immediate page is documented as the negotiated exception, not the rule.
    expect(jsonSchemas.AgentActionsPage.description).toContain("Accept: application/json");
    expect(jsonSchemas.AgentActionsControl.required).toEqual(["nextOffset", "upToDate"]);
  });
});
