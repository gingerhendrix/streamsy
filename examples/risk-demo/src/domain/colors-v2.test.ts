/**
 * Colour assignment is only safe if it is total and duplicate-free wherever the
 * decider can reach it: any roster smaller than the palette must always yield a
 * free colour, requests are honoured only while free, and comparison ignores the
 * case/whitespace variations real clients have already sent.
 */

import { describe, expect, it } from "vitest";

import { PLAYER_COLORS_V2, assignPlayerColorV2, normalizedPlayerColor } from "./colors-v2.ts";
import { RULES_V2 } from "./map-v2.ts";

describe("PLAYER_COLORS_V2", () => {
  it("issues one distinct colour per possible seat", () => {
    expect(PLAYER_COLORS_V2).toHaveLength(RULES_V2.maxPlayers);
    expect(new Set(PLAYER_COLORS_V2.map(normalizedPlayerColor)).size).toBe(PLAYER_COLORS_V2.length);
  });
});

describe("assignPlayerColorV2", () => {
  it("issues the first palette colour to an empty roster without a request", () => {
    expect(assignPlayerColorV2([])).toBe(PLAYER_COLORS_V2[0]);
  });

  it("honours a requested colour while it is free", () => {
    expect(assignPlayerColorV2([PLAYER_COLORS_V2[0]], "#123456")).toBe("#123456");
    expect(assignPlayerColorV2([], PLAYER_COLORS_V2[3])).toBe(PLAYER_COLORS_V2[3]);
  });

  it("replaces a taken request with the first free palette colour", () => {
    const taken = [PLAYER_COLORS_V2[0]];
    expect(assignPlayerColorV2(taken, PLAYER_COLORS_V2[0])).toBe(PLAYER_COLORS_V2[1]);
  });

  it("compares colours ignoring case and surrounding whitespace", () => {
    expect(assignPlayerColorV2(["#E05A47"], " #e05a47 ")).toBe(PLAYER_COLORS_V2[1]);
    expect(assignPlayerColorV2([" #3B82F6 "], "#3b82f6")).toBe(PLAYER_COLORS_V2[0]);
  });

  it("treats a blank request as no request", () => {
    expect(assignPlayerColorV2([PLAYER_COLORS_V2[0]], "   ")).toBe(PLAYER_COLORS_V2[1]);
  });

  it("never duplicates a colour for any reachable roster", () => {
    // Every subset of taken palette colours a lobby below the seat cap can hold,
    // with and without a conflicting request.
    for (let mask = 0; mask < 1 << PLAYER_COLORS_V2.length; mask += 1) {
      const taken = PLAYER_COLORS_V2.filter((_, index) => mask & (1 << index));
      if (taken.length >= RULES_V2.maxPlayers) continue;
      for (const requested of [undefined, ...taken]) {
        const assigned = assignPlayerColorV2(taken, requested);
        expect(taken.map(normalizedPlayerColor)).not.toContain(normalizedPlayerColor(assigned));
      }
    }
  });

  it("also assigns freely around non-palette colours already in the roster", () => {
    const taken = ["#22c1a5", PLAYER_COLORS_V2[0]];
    const assigned = assignPlayerColorV2(taken, "#22C1A5");
    expect(taken.map(normalizedPlayerColor)).not.toContain(normalizedPlayerColor(assigned));
  });
});
