import { describe, expect, it } from "vitest";

import type { ProjectedMoveV2, ProjectedTurnV2 } from "../board/projection-v2.ts";
import {
  agentSeatUrl,
  countdownFraction,
  countdownLabel,
  countdownSeconds,
  defenseAttribution,
  dicePairs,
  diceOutcomeText,
  moveDetailV2,
  moveTextV2,
  reinforcementEquation,
  reinforcementProgress,
  resolutionLabel,
  revealPlan,
  seatStatusLabel,
  terrainMix,
  turnLedger,
  type NameLookup,
} from "./presentation-v2.ts";

const NAMES: NameLookup = {
  territory: (id) => ({ t1: "Ashfell", t2: "Northgate", t3: "Karrow" })[id] ?? id,
  player: (id) => ({ p1: "Ada", p2: "Mina" })[id ?? ""] ?? "Nobody",
  continent: (id) => ({ c1: "Northreach", c2: "Sunder" })[id] ?? id,
};

function turn(overrides: Partial<ProjectedTurnV2> = {}): ProjectedTurnV2 {
  return {
    id: "turn",
    turnId: "round-2:p1",
    round: 2,
    playerId: "p1",
    phase: "attack",
    reinforcement: {
      base: 4,
      continents: [{ continentId: "c1", bonus: 2 }],
      total: 6,
      remaining: 2,
    },
    reinforcementsPlaced: 4,
    attacksDeclared: 0,
    throwsResolved: 0,
    captures: 0,
    eliminations: 0,
    ...overrides,
  };
}

describe("reinforcement accounting", () => {
  it("explains the whole pool as an equation", () => {
    expect(reinforcementEquation(turn().reinforcement, NAMES.continent)).toBe(
      "6 total = 4 territory + 2 Northreach",
    );
  });

  it("omits continent terms when none are held", () => {
    expect(
      reinforcementEquation({ base: 3, continents: [], total: 3, remaining: 3 }, NAMES.continent),
    ).toBe("3 total = 3 territory");
  });

  it("reports placement progress", () => {
    expect(reinforcementProgress(4, 2)).toBe("4 placed · 2 remaining");
  });
});

describe("defence countdown", () => {
  it("counts whole seconds down to the canonical deadline", () => {
    expect(countdownSeconds(15_000, 2_500)).toBe(13);
    expect(countdownLabel(15_000, 2_500)).toBe("13s");
  });

  it("never runs negative once the window has closed", () => {
    expect(countdownSeconds(1_000, 9_000)).toBe(0);
    expect(countdownLabel(1_000, 9_000)).toBe("time’s up");
    expect(countdownFraction(1_000, 9_000, 15_000)).toBe(0);
  });

  it("reports the fraction of the window still open", () => {
    expect(countdownFraction(15_000, 0, 15_000)).toBe(1);
    expect(countdownFraction(15_000, 7_500, 15_000)).toBe(0.5);
  });
});

describe("dice", () => {
  it("sorts both rows descending and gives ties to the defender", () => {
    expect(dicePairs([3, 6, 2], [4, 6])).toEqual([
      { attacker: 6, defender: 6, loser: "attacker" },
      { attacker: 3, defender: 4, loser: "attacker" },
      { attacker: 2, defender: null, loser: null },
    ]);
  });

  it("decides nothing for an unpaired die", () => {
    expect(dicePairs([5], [])).toEqual([{ attacker: 5, defender: null, loser: null }]);
  });

  it("labels a timeout roll without implying a human clicked", () => {
    expect(resolutionLabel("timeout", "Mina")).toBe("Auto-rolled after timeout");
    expect(resolutionLabel("bot", "Mina")).toBe("Bot rolled");
    expect(resolutionLabel("agent", "Mina")).toBe("Agent rolled");
    expect(resolutionLabel("human", "Mina")).toBe("Rolled by Mina");
  });

  it("attributes a defence in a sentence the same way the combat card does", () => {
    expect(defenseAttribution("human", "Mina", "Ashfell")).toBe("Mina defended Ashfell");
    expect(defenseAttribution("bot", "Mina", "Ashfell")).toBe("Mina’s bot defended Ashfell");
    expect(defenseAttribution("agent", "Mina", "Ashfell")).toBe("Mina’s agent defended Ashfell");
    expect(defenseAttribution("timeout", "Mina", "Ashfell")).toBe(
      "Ashfell was auto-rolled — Mina’s window expired",
    );
  });
});

describe("reveal plan", () => {
  it("shakes a cup for a result arriving live", () => {
    expect(revealPlan({ reducedMotion: false, alreadySeen: false })).toEqual({
      mode: "shake",
      durationMs: 620,
    });
  });

  it("fades instead when the player prefers reduced motion", () => {
    expect(revealPlan({ reducedMotion: true, alreadySeen: false }).mode).toBe("fade");
  });

  it("does not replay a result the client only learned about by reloading", () => {
    expect(revealPlan({ reducedMotion: false, alreadySeen: true })).toEqual({
      mode: "none",
      durationMs: 0,
    });
  });
});

describe("current-turn ledger", () => {
  it("reads the turn row rather than the bounded move feed", () => {
    const entries = turnLedger(
      turn({
        attacksDeclared: 3,
        throwsResolved: 3,
        captures: 1,
        eliminations: 1,
        latestDice: {
          attackId: "a1",
          from: "t1",
          to: "t2",
          attackerRolls: [6, 5, 2],
          defenderRolls: [4, 3],
          attackerLosses: 0,
          defenderLosses: 2,
          territoryCaptured: true,
          resolutionSource: "human",
        },
      }),
      NAMES,
    );
    expect(entries.map((entry) => entry.id)).toEqual([
      "reinforce",
      "attacks",
      "dice:a1",
      "captures",
      "eliminations",
    ]);
    expect(entries[0]?.text).toBe("Placed 4 of 6 reinforcements");
    expect(entries[1]?.detail).toBe("3 throws resolved");
    expect(entries[2]?.detail).toBe("6 · 5 · 2 vs 4 · 3 — Northgate captured");
  });

  it("counts a throw still awaiting its defence separately", () => {
    const entries = turnLedger(turn({ attacksDeclared: 2, throwsResolved: 1 }), NAMES);
    expect(entries.find((entry) => entry.id === "attacks")?.detail).toBe("1 of 2 throws resolved");
  });

  it("says so plainly when a turn has not started", () => {
    const entries = turnLedger(
      turn({
        reinforcement: { base: 0, continents: [], total: 0, remaining: 0 },
        reinforcementsPlaced: 0,
      }),
      NAMES,
    );
    expect(entries).toEqual([
      { id: "idle", icon: "·", text: "Nothing has happened yet this turn" },
    ]);
  });

  it("notes a spent fortify", () => {
    const entries = turnLedger(turn({ phase: "fortify" }), NAMES);
    expect(entries.at(-1)?.id).toBe("fortify");
  });

  it("reports a hold with its losses rather than a capture", () => {
    expect(
      diceOutcomeText(
        {
          attackId: "a2",
          from: "t1",
          to: "t2",
          attackerRolls: [3],
          defenderRolls: [5],
          attackerLosses: 1,
          defenderLosses: 0,
          territoryCaptured: false,
          resolutionSource: "timeout",
        },
        NAMES,
      ),
    ).toBe("3 vs 5 — 1 attacker lost · Auto-rolled after timeout");
  });
});

const move = (overrides: Partial<ProjectedMoveV2>): ProjectedMoveV2 => ({
  id: "7",
  commandId: "c1",
  kind: "ArmiesReinforced",
  sourceOffset: "0000000007_0000",
  ...overrides,
});

describe("game history", () => {
  it("names countries and players rather than ids", () => {
    expect(moveTextV2(move({ playerId: "p1", territoryId: "t1", armies: 2 }), NAMES)).toBe(
      "Ada reinforced Ashfell with 2",
    );
    expect(
      moveTextV2(move({ kind: "TerritoryOccupied", playerId: "p1", to: "t2", armies: 3 }), NAMES),
    ).toBe("Ada occupied Northgate with 3");
  });

  it("never puts a source offset in player-facing text", () => {
    const text = moveTextV2(move({ kind: "GameStarted" }), NAMES);
    expect(text).not.toContain(move({}).sourceOffset);
  });

  it("never lets a lapsed defence window read as a human roll", () => {
    const resolved = {
      kind: "AttackResolved" as const,
      playerId: "p2",
      from: "t1",
      to: "t2",
      attackerRolls: [6, 2],
      defenderRolls: [3],
      attackerLosses: 0,
      defenderLosses: 1,
      territoryCaptured: false,
    };
    expect(moveTextV2(move({ ...resolved, resolutionSource: "human" }), NAMES)).toBe(
      "Mina defended Northgate",
    );
    expect(moveTextV2(move({ ...resolved, resolutionSource: "timeout" }), NAMES)).toBe(
      "Northgate was auto-rolled — Mina’s window expired",
    );
    expect(moveTextV2(move({ ...resolved, resolutionSource: "bot" }), NAMES)).toBe(
      "Mina’s bot defended Northgate",
    );
    expect(moveDetailV2(move({ ...resolved, resolutionSource: "timeout" }), NAMES)).toBe(
      "6 · 2 vs 3 — 1 defender lost · Auto-rolled after timeout",
    );
  });

  it("adds a dice line only for a resolved throw", () => {
    expect(moveDetailV2(move({ kind: "ArmiesFortified" }), NAMES)).toBeNull();
    expect(
      moveDetailV2(
        move({
          kind: "AttackResolved",
          from: "t1",
          to: "t2",
          attackerRolls: [6, 2],
          defenderRolls: [6],
          attackerLosses: 1,
          defenderLosses: 0,
          territoryCaptured: false,
        }),
        NAMES,
      ),
    ).toBe("6 · 2 vs 6 — 1 attacker lost");
  });
});

describe("map and seat language", () => {
  it("summarises a country's terrain mix, commonest first", () => {
    expect(terrainMix(["hills", "forest", "forest", "plains"])).toBe(
      "2 Forest · 1 Hills · 1 Plains",
    );
  });

  it("puts the winner ahead of every other status", () => {
    expect(
      seatStatusLabel({
        spectating: true,
        mode: "waiting",
        activePlayerName: "Ada",
        finished: true,
        winnerName: "Mina",
      }),
    ).toBe("Mina wins the map");
  });

  it("labels a spectator, a defender, and a waiting seat distinctly", () => {
    const base = { finished: false, activePlayerName: "Ada" };
    expect(seatStatusLabel({ ...base, spectating: true, mode: null })).toBe("Spectating live");
    expect(seatStatusLabel({ ...base, spectating: false, mode: "defense" })).toBe(
      "Defend your country",
    );
    expect(seatStatusLabel({ ...base, spectating: false, mode: "active-turn" })).toBe("Your turn");
    expect(seatStatusLabel({ ...base, spectating: false, mode: "waiting" })).toBe(
      "Waiting for Ada",
    );
  });
});

describe("agent seat URL", () => {
  it("builds a private fragment-bearing seat URL without a query capability", () => {
    const url = agentSeatUrl({
      origin: "http://localhost:22392/",
      gameId: "game/a",
      playerId: "p 1",
      token: "rsk_secret",
    });
    expect(url).toBe("http://localhost:22392/agent-seat/game%2Fa/p%201#token=rsk_secret");
    expect(new URL(url).search).toBe("");
  });
});
