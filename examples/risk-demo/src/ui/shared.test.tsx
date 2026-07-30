import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  MAX_SEAT_NAME,
  PlayerFields,
  agentSeatName,
  gameFromPath,
  gamePath,
  spectatingSeat,
  type Identity,
} from "./shared.tsx";

describe("player identity fields", () => {
  it("asks only for a name because the game assigns an available colour", () => {
    const html = renderToStaticMarkup(<PlayerFields name="Mina" onName={() => {}} />);

    expect(html).toContain("Your name");
    expect(html).toContain('value="Mina"');
    expect(html).not.toContain("Colour");
    expect(html).not.toContain("swatch");
  });
});

describe("game routes", () => {
  it("keeps the home page at the root path", () => {
    expect(gameFromPath("/")).toBe("");
    expect(gameFromPath("/?game=ignored")).toBe("");
  });

  it("reads and builds game paths", () => {
    expect(gameFromPath("/game/game_123")).toBe("game_123");
    expect(gameFromPath("/game/game%20with%20spaces/")).toBe("game with spaces");
    expect(gamePath("game with spaces")).toBe("/game/game%20with%20spaces");
  });

  it("does not treat nested or malformed paths as games", () => {
    expect(gameFromPath("/games/game_123")).toBe("");
    expect(gameFromPath("/game/game_123/board")).toBe("");
    expect(gameFromPath("/game/%E0%A4%A")).toBe("");
  });
});

describe("agent seat names", () => {
  it("keeps a chosen name, trimmed", () => {
    expect(agentSeatName("  Marshal Ney  ", 2)).toBe("Marshal Ney");
  });

  it("never sends an empty name, falling back to the seat's ordinal", () => {
    expect(agentSeatName("", 1)).toBe("Agent 1");
    expect(agentSeatName("   ", 3)).toBe("Agent 3");
    expect(agentSeatName(undefined, 4)).toBe("Agent 4");
  });

  it("bounds the name so the muster roll and move feed stay readable", () => {
    const long = agentSeatName("x".repeat(80), 1);
    expect(long).toHaveLength(MAX_SEAT_NAME);
  });
});

describe("spectating seats", () => {
  const host: Identity = { gameId: "g1", playerId: "p1", token: "t", role: "host" };

  it("spectates without an identity", () => {
    expect(spectatingSeat(null, undefined)).toBe(true);
  });

  it("spectates while the seat is not yet on the board", () => {
    expect(spectatingSeat(host, undefined)).toBe(true);
  });

  it("plays a human seat this identity holds", () => {
    expect(spectatingSeat(host, { controller: "human" })).toBe(false);
  });

  it("spectates a seat some other controller is playing", () => {
    expect(spectatingSeat(host, { controller: "external-agent" })).toBe(true);
    expect(spectatingSeat(host, { controller: "bot" })).toBe(true);
  });

  it("honours the persisted marker before the delegation is projected", () => {
    expect(spectatingSeat({ ...host, spectator: true }, { controller: "human" })).toBe(true);
  });
});
