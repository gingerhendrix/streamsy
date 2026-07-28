import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { PlayerFields, gameFromPath, gamePath } from "./shared.tsx";

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
    expect(gameFromPath("/?game=legacy")).toBe("");
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
