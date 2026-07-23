import { describe, expect, it } from "vitest";

import { REQUIRED_WORKSPACE_DISTS, missingWorkspaceDists, spectatorUrl } from "../scripts/demo.ts";
import { acknowledgementNotice, didGameStatusChange } from "./ui/App.tsx";

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
});

describe("action notices", () => {
  it("renders command acknowledgements as sentences", () => {
    expect(acknowledgementNotice("attack")).toBe("Attack committed to the stream.");
    expect(acknowledgementNotice("end-turn")).toBe("End turn committed to the stream.");
  });

  it("clears an existing notice only across an established status transition", () => {
    expect(didGameStatusChange(null, "lobby")).toBe(false);
    expect(didGameStatusChange("lobby", "lobby")).toBe(false);
    expect(didGameStatusChange("lobby", "playing")).toBe(true);
    expect(didGameStatusChange("playing", "finished")).toBe(true);
  });
});
