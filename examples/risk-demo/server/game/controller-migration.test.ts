import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import { createJsonProtocol, type JsonCodec } from "@streamsy/json";

import { readCanonicalV2 } from "./command-service.ts";

const legacyCodec: JsonCodec<Record<string, unknown>> = {
  encode: (value) => value,
  decode: (value) => value as Record<string, unknown>,
};

describe("canonical controller migration boundary", () => {
  it("replays persisted agent events as deterministic bots", async () => {
    const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
    const stream = await createJsonProtocol(protocol, legacyCodec).getOrCreate(
      "risk/game-old/events",
    );
    await stream.append({
      type: "GameCreated",
      gameId: "game-old",
      hostPlayerId: "p1",
      hostName: "Ada",
      hostColor: "red",
      hostController: "agent",
      ruleset: "risk-demo-v2",
      mapVersion: "procedural-hex-v1",
      generatorVersion: "hex-generator-v1",
      mapSeed: "seed",
      commandId: "create",
    });
    await stream.append({
      type: "PlayerJoined",
      playerId: "p2",
      name: "Bob",
      color: "blue",
      controller: "agent",
      commandId: "join",
    });

    const history = await readCanonicalV2(protocol, "risk/game-old/events");

    expect(history.events[0]).toMatchObject({ type: "GameCreated", hostController: "bot" });
    expect(history.events[1]).toMatchObject({ type: "PlayerJoined", controller: "bot" });
  });
});
