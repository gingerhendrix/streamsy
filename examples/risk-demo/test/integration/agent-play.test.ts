import { describe, expect, it } from "vitest";

import { agentPlayInstructions } from "../../src/application/agent-play.ts";
import { BASE, call, createV2Game, decisionFor, post, v2Harness } from "../v2-harness.ts";

describe("single-session agent play", () => {
  it("returns complete pasteable instructions when an agent hosts", async () => {
    const h = v2Harness();
    const created = await call(h.app, "POST", "/v1/games", {
      body: {
        name: "Agent 1",
        color: "purple",
        controller: "agent",
        mapSeed: "agent-host-seed",
      },
    });

    expect(created.status).toBe(201);
    const instructions = created.body.agentInstructions as string;
    expect(instructions).toContain(`Player ID: ${created.body.player.id}`);
    expect(instructions).toContain(`Token: ${created.body.capability}`);
    expect(instructions).toContain(`/agent/${created.body.capability}/state`);
    expect(instructions).toContain(`/v1/games/${created.body.game.id}/commands`);
  });

  it("returns complete pasteable instructions when an agent joins", async () => {
    const h = v2Harness();
    const created = await call(h.app, "POST", "/v1/games", {
      body: { name: "Human", color: "red", mapSeed: "agent-play-seed" },
    });
    const gameId = created.body.game.id as string;
    const joined = await call(h.app, "POST", `/v1/games/${gameId}/players`, {
      body: { name: "Conquest Agent", color: "blue", controller: "agent" },
    });

    expect(joined.status).toBe(201);
    const instructions = joined.body.agentInstructions as string;
    expect(instructions).toContain(`Token: ${joined.body.capability}`);
    expect(instructions).toContain(`/agent/${joined.body.capability}/wait?wait=30000`);
    expect(instructions).toContain(`/agent/${joined.body.capability}/state`);
    expect(instructions).toContain(`/v1/games/${gameId}/commands`);
    expect(instructions).toContain("Defence dice are rolled automatically");
    expect(instructions).not.toContain("urgent out-of-turn interrupt");
    expect(instructions).toContain("mandatory occupy-territory");
    expect(instructions).toContain("accepted and duplicate as success");
  });

  it("serves one compact personalized map, turn, and legal-move document", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["human", "agent"] });
    const playerId = game.players[1]!;
    const token = game.tokenByPlayer[playerId]!;
    const state = await call(h.app, "GET", `/agent/${token}/state`);

    expect(state.status).toBe(200);
    expect(state.body.player.id).toBe(playerId);
    expect(state.body.status).toBe("playing");
    expect(state.body.turn.id).toMatch(/^round-1:/);
    expect(state.body.territories).toHaveLength(16);
    expect(state.body.territories[0]).toMatchObject({
      id: expect.any(String),
      name: expect.any(String),
      neighbours: expect.any(Array),
      owner: { id: expect.any(String), name: expect.any(String) },
      armies: expect.any(Number),
    });
    expect(state.body.territories[0].neighbours[0]).toMatchObject({
      id: expect.any(String),
      name: expect.any(String),
    });
    expect(Array.isArray(state.body.legalMoves)).toBe(true);
    expect(JSON.stringify(state.body)).not.toContain("hexIds");

    const bad = await call(h.app, "GET", "/agent/rsk_invalid_token/state");
    expect(bad.status).toBe(401);
  });

  it("waits without a cursor and tells the agent only to refetch state", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["human", "agent"] });
    const activeDecision = await decisionFor(
      h.app,
      game,
      (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body.activePlayerId,
    );
    const activeToken = game.tokenByPlayer[activeDecision.player.id]!;
    const immediate = await call(h.app, "GET", `/agent/${activeToken}/wait?wait=0`);
    expect(immediate.body).toEqual({
      changed: true,
      reason: "actionable",
      stateUrl: `/agent/${activeToken}/state`,
    });

    const waitingPlayer = game.players.find((id) => id !== activeDecision.player.id)!;
    const waitingToken = game.tokenByPlayer[waitingPlayer]!;
    const timeout = await call(h.app, "GET", `/agent/${waitingToken}/wait?wait=0`);
    expect(timeout.body).toEqual({
      changed: false,
      reason: "timeout",
      stateUrl: `/agent/${waitingToken}/state`,
    });
    expect(timeout.body).not.toHaveProperty("cursor");
  });

  it("returns named rule errors and directs the agent to fresh legal moves", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const metadata = await call(h.app, "GET", `/v1/games/${game.gameId}`);
    const active = metadata.body.activePlayerId as string;
    const decision = await decisionFor(h.app, game, active);
    const reinforce = decision.legalActions.find((move: any) => move.type === "reinforce");
    const territoryId = reinforce.territoryIds[0] as string;
    const rejected = await post(h.app, game, active, {
      commandId: "too-many-reinforcements",
      turnId: decision.turn.id,
      action: {
        type: "reinforce",
        placements: [{ territoryId, armies: reinforce.maxArmies + 1 }],
      },
    });
    expect(rejected.status).toBe(409);
    expect(rejected.body.error.code).toBe("INSUFFICIENT_ARMIES");
    expect(rejected.body.error.message).toContain(
      `Place all ${reinforce.maxArmies} reinforcements in one command`,
    );
    expect(rejected.body.error.message).toContain(`${reinforce.maxArmies + 1}`);
    expect(rejected.body.error.message).toContain("personalized state URL");
    expect(rejected.body.error.message).toContain("legalMoves");
  });
});

// Keeps the generated text reviewable as a stable, harness-neutral contract.
describe("agent instruction template", () => {
  it("embeds all endpoints and the fresh-state/idempotency loop", () => {
    const text = agentPlayInstructions({
      origin: BASE,
      gameId: "game-1",
      playerId: "player-2",
      token: "rsk_token_secret",
    });
    expect(text).toContain("GET the state URL immediately before choosing");
    expect(text).toContain("retry the byte-identical body with the same commandId");
    expect(text).toContain("continue until the game is finished");
  });
});
