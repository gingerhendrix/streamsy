import { describe, expect, it } from "vitest";

import { normalizeGameEventV2 } from "./events-v2.ts";

describe("historical controller vocabulary", () => {
  it("normalizes persisted deterministic agent seats to bots without mutating stored input", () => {
    const created = {
      type: "GameCreated",
      gameId: "g",
      hostPlayerId: "p1",
      hostName: "Ada",
      hostColor: "red",
      hostController: "agent",
      ruleset: "risk-demo-v2",
      mapVersion: "procedural-hex-v1",
      generatorVersion: "hex-generator-v1",
      mapSeed: "seed",
      commandId: "create",
    } as const;
    const joined = {
      type: "PlayerJoined",
      playerId: "p2",
      name: "Bob",
      color: "blue",
      controller: "agent",
      commandId: "join",
    } as const;

    expect(normalizeGameEventV2(created)).toMatchObject({ hostController: "bot" });
    expect(normalizeGameEventV2(joined)).toMatchObject({ controller: "bot" });
    expect(created.hostController).toBe("agent");
    expect(joined.controller).toBe("agent");
  });

  it("normalizes historical agent-auto defence attribution to bot", () => {
    const resolved = {
      type: "AttackResolved",
      attackId: "attack",
      turnId: "turn",
      attackerId: "p1",
      defenderId: "p2",
      from: "a",
      to: "b",
      attackerRolls: [6],
      defenderRolls: [1],
      attackerLosses: 0,
      defenderLosses: 1,
      territoryCaptured: true,
      resolutionSource: "agent-auto",
      commandId: "defend",
    } as const;

    expect(normalizeGameEventV2(resolved)).toMatchObject({ resolutionSource: "bot" });
    expect(resolved.resolutionSource).toBe("agent-auto");
  });

  it("preserves the new external-agent and bot vocabulary", () => {
    const external = {
      type: "PlayerJoined",
      playerId: "p2",
      name: "Claude",
      color: "blue",
      controller: "external-agent",
      commandId: "join-agent",
    } as const;
    const bot = { ...external, controller: "bot", commandId: "join-bot" } as const;

    expect(normalizeGameEventV2(external)).toBe(external);
    expect(normalizeGameEventV2(bot)).toBe(bot);
  });
});
