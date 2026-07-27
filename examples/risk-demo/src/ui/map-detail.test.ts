import { describe, expect, it } from "vitest";

import { detailTerritoryId } from "./game-v2.tsx";

describe("map territory summary", () => {
  it("prefers transient pointer hover and falls back to keyboard focus", () => {
    expect(detailTerritoryId("hovered", "focused")).toBe("hovered");
    expect(detailTerritoryId(null, "focused")).toBe("focused");
    expect(detailTerritoryId(null, null)).toBeNull();
  });
});
