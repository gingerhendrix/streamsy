/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
import { describe, expect, it } from "vitest";
import { agentPlayInstructions } from "../../src/application/agent-play.ts";
import { call, riskHarness } from "../harness.ts";

describe("agent seat authority and contract", () => {
  it("rejects agent creation/join and mints one host-authorized seat descriptor", async () => {
    const h = riskHarness();
    const forbiddenCreate = await call(h.app, "POST", "/v1/games", {
      body: { name: "Agent", controller: "agent" },
    });
    expect(forbiddenCreate.status).toBe(403);
    expect(forbiddenCreate.body.error.code).toBe("AGENT_SEAT_REQUIRES_HOST");

    const created = await call(h.app, "POST", "/v1/games", { body: { name: "Host" } });
    const gameId = created.body.game.id;
    const opened = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: created.body.capability,
      body: { name: "Agent 1" },
    });
    expect(opened.status).toBe(201);
    expect(opened.body.seat).toMatchObject({
      gameId,
      name: "Agent 1",
      urls: {
        map: `/v1/games/${gameId}/map`,
        actions: `/v1/games/${gameId}/players/me/actions`,
        decision: `/v1/games/${gameId}/decision`,
        commands: `/v1/games/${gameId}/commands`,
      },
    });
    expect(opened.body.instructions).toContain(`Token: ${opened.body.seat.token}`);
    expect(opened.body.instructions).not.toContain("/agent/");

    const agentToken = opened.body.seat.token;
    for (const [path, body] of [
      ["/v1/games", { name: "Extra" }],
      [`/v1/games/${gameId}/players`, { name: "Extra" }],
      [`/v1/games/${gameId}/start`, {}],
      [`/v1/games/${gameId}/agent-seats`, { name: "Extra" }],
    ] as const) {
      const response = await call(h.app, "POST", path, { token: agentToken, body });
      expect(response.status).toBe(403);
    }
  });

  it("returns structured INVALID_ACTION details", async () => {
    const h = riskHarness();
    const created = await call(h.app, "POST", "/v1/games", { body: { name: "Host" } });
    const response = await call(h.app, "POST", `/v1/games/${created.body.game.id}/commands`, {
      token: created.body.capability,
      body: {
        commandId: "bad",
        turnId: "turn",
        action: { type: "reinforce", placements: [{ territoryId: "x", armies: "three" }] },
      },
    });
    expect(response.status).toBe(400);
    expect(response.body.error).toMatchObject({
      code: "INVALID_ACTION",
      details: [
        {
          path: "action.placements[0]",
          expected: 'Expected number, got "three"\n  at ["action"]["placements"][0]["armies"]',
        },
      ],
    });
  });

  it("describes the cursor loop, submit templates, and recovery endpoint", () => {
    const text = agentPlayInstructions({
      origin: "https://risk.test",
      gameId: "game",
      playerId: "player",
      name: "Agent",
      color: "#fff",
      token: "rsk_token_secret",
    });
    expect(text).toContain("nextOffset");
    expect(text).toContain("newest ActionRequired");
    expect(text).toContain("sum exactly to pool");
    expect(text).toContain("/decision");
    expect(text).toContain("until GameOver");
  });
});
