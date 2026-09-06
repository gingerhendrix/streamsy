import { describe, expect, it } from "vitest";

import {
  DEMO_COMMAND_PACE_MS,
  DEMO_GUEST_REQUEST,
  DEMO_HOST_REQUEST,
  DEMO_LEAD_IN_MS,
  REQUIRED_WORKSPACE_DISTS,
  missingWorkspaceDists,
  spectatorUrl,
} from "../../scripts/demo.ts";
import { acknowledgementNotice } from "../../src/ui/App.tsx";

describe("one-command demo helpers", () => {
  it("reports exactly the workspace outputs that need building", () => {
    const present = new Set(REQUIRED_WORKSPACE_DISTS.slice(0, 2));
    const missing = missingWorkspaceDists("/repo", (path) =>
      [...present].some((entry) => path.endsWith(entry)),
    );

    expect(missing).toEqual(REQUIRED_WORKSPACE_DISTS.slice(2));
  });

  it("builds a spectator URL without exposing player credentials", () => {
    expect(spectatorUrl("http://127.0.0.1:4321", "game_a&b")).toBe(
      "http://127.0.0.1:4321/?game=game_a%26b",
    );
  });

  it("uses two scripted bots", () => {
    // Nobody is at the keyboard, so both seats must be able to answer a defence
    // interrupt as well as play their own turn.
    expect(DEMO_HOST_REQUEST.controller).toBe("bot");
    expect(DEMO_GUEST_REQUEST.controller).toBe("bot");
    expect(DEMO_GUEST_REQUEST.color).not.toBe(DEMO_HOST_REQUEST.color);
  });

  it("reserves a human lead-in and paces individual commands", () => {
    expect(DEMO_LEAD_IN_MS).toBeGreaterThanOrEqual(8_000);
    expect(DEMO_COMMAND_PACE_MS).toBeGreaterThanOrEqual(1_000);
    expect(DEMO_COMMAND_PACE_MS).toBeLessThanOrEqual(1_500);
  });
});

describe("action notices", () => {
  it("renders command acknowledgements as sentences", () => {
    expect(acknowledgementNotice("attack")).toBe("Attack committed to the stream.");
    expect(acknowledgementNotice("end-turn")).toBe("End turn committed to the stream.");
  });
});
