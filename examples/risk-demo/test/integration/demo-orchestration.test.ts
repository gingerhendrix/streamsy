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
import { projectEvents } from "../../src/board/projection.ts";
import { RULESET_V2 } from "../../src/domain/map-v2.ts";
import { startGame } from "../testkit.ts";
import { acknowledgementNotice, didGameStatusChange, playerRoleLabel } from "../../src/ui/App.tsx";

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

  it("honestly showcases the v2 ruleset with two scripted bots", () => {
    // Nobody is at the keyboard, so both seats must be able to answer a defence
    // interrupt as well as play their own turn.
    expect(DEMO_HOST_REQUEST.ruleset).toBe(RULESET_V2);
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

  it("clears an existing notice only across an established status transition", () => {
    expect(didGameStatusChange(null, "lobby")).toBe(false);
    expect(didGameStatusChange("lobby", "lobby")).toBe(false);
    expect(didGameStatusChange("lobby", "playing")).toBe(true);
    expect(didGameStatusChange("playing", "finished")).toBe(true);
  });
});

describe("lobby roles", () => {
  it("labels the authoritative creator as host regardless of player ordering", () => {
    const scripted = startGame(2);
    const board = projectEvents(scripted.game.log);
    const [creatorId, joinerId] = scripted.playerIds;

    expect(board.game.hostPlayerId).toBe(creatorId);
    expect(playerRoleLabel(board.game.hostPlayerId, joinerId!)).toBe("Player");
    expect(playerRoleLabel(board.game.hostPlayerId, creatorId!)).toBe("Host");
  });
});
