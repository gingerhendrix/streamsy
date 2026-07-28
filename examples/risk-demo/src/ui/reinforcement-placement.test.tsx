import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { NameLookup } from "./presentation-v2.ts";
import {
  ReinforcementPlacement,
  adjustPendingReinforcements,
  pendingReinforcementTotal,
} from "./reinforcement-placement.tsx";

const ACTION = {
  type: "reinforce" as const,
  territoryIds: ["t1", "t2"],
  pool: 4,
  submit: {
    type: "reinforce" as const,
    placements: [
      {
        territoryId: "<one of territoryIds>" as const,
        armies: "<1..pool>" as const,
      },
    ],
  },
};

const NAMES: NameLookup = {
  territory: (id) => ({ t1: "Ashfell", t2: "Northgate" })[id] ?? id,
  player: (id) => id ?? "Nobody",
  continent: (id) => id,
};

describe("pending reinforcement state", () => {
  it("increments and decrements per territory without going below zero", () => {
    let pending = adjustPendingReinforcements(new Map(), "t1", 1, ACTION.territoryIds, 4);
    pending = adjustPendingReinforcements(pending, "t1", 1, ACTION.territoryIds, 4);
    pending = adjustPendingReinforcements(pending, "t2", 1, ACTION.territoryIds, 4);
    expect([...pending]).toEqual([
      ["t1", 2],
      ["t2", 1],
    ]);

    pending = adjustPendingReinforcements(pending, "t1", -1, ACTION.territoryIds, 4);
    pending = adjustPendingReinforcements(pending, "t1", -1, ACTION.territoryIds, 4);
    pending = adjustPendingReinforcements(pending, "t1", -1, ACTION.territoryIds, 4);
    expect([...pending]).toEqual([["t2", 1]]);
  });

  it("ignores non-owned territories and caps the allocation at the canonical allowance", () => {
    let pending = new Map<string, number>([["t1", 3]]);
    pending = adjustPendingReinforcements(pending, "enemy", 1, ACTION.territoryIds, 4);
    pending = adjustPendingReinforcements(pending, "t2", 1, ACTION.territoryIds, 4);
    pending = adjustPendingReinforcements(pending, "t2", 1, ACTION.territoryIds, 4);
    expect(pendingReinforcementTotal(pending)).toBe(4);
    expect([...pending]).toEqual([
      ["t1", 3],
      ["t2", 1],
    ]);
  });
});

describe("reinforcement placement panel", () => {
  it("lists affected territories with accessible controls and keeps finish disabled", () => {
    const html = renderToStaticMarkup(
      <ReinforcementPlacement
        action={ACTION}
        names={NAMES}
        pending={new Map([["t1", 2]])}
        busy={false}
        onAdjust={() => {}}
        onFinish={() => {}}
      />,
    );

    expect(html).toContain("Ashfell");
    expect(html).not.toContain("Northgate");
    expect(html).toContain('aria-label="Remove one pending reinforcement from Ashfell"');
    expect(html).toContain('aria-label="Add one pending reinforcement to Ashfell"');
    expect(html).toContain(">4</b><small>Total");
    expect(html).toContain(">2</b><small>Remaining");
    expect(html).toContain("Finish reinforcements");
    expect(html).toMatch(/class="primary finish-reinforcements" disabled=""/);
  });

  it("enables finish only when every available reinforcement is pending", () => {
    const html = renderToStaticMarkup(
      <ReinforcementPlacement
        action={ACTION}
        names={NAMES}
        pending={
          new Map([
            ["t1", 3],
            ["t2", 1],
          ])
        }
        busy={false}
        onAdjust={() => {}}
        onFinish={() => {}}
      />,
    );

    expect(html).toContain("Ashfell");
    expect(html).toContain("Northgate");
    expect(html).toContain(">0</b><small>Remaining");
    expect(html).toMatch(/class="primary finish-reinforcements">Finish reinforcements/);
  });
});
