/**
 * The combat card rendered for real.
 *
 * The property under test is honesty: an unresolved defence must render face-*down*
 * dice rather than invented faces, only the defender who can act is offered the roll
 * button, and a throw the timeout resolved must not read as one a human made.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { CombatCard, type CombatCardProps } from "./combat-card.tsx";
import type { CombatView } from "./combat-view.ts";
import type { NameLookup } from "./presentation.ts";

const NAMES: NameLookup = {
  territory: (id) => ({ t1: "Ashfell", t2: "Northgate" })[id] ?? id,
  player: (id) => ({ p1: "Ada", p2: "Mina" })[id ?? ""] ?? "Nobody",
  continent: (id) => id,
};

const PENDING: CombatView = {
  attackId: "a1",
  status: "awaiting-defense",
  attackerId: "p1",
  defenderId: "p2",
  from: "t1",
  to: "t2",
  attackerDice: 3,
  attackerRolls: [6, 4, 2],
  defenderDice: 2,
  declaredAt: 1_000,
  defenseDeadlineAt: 16_000,
};

const card = (overrides: Partial<CombatCardProps>) =>
  renderToStaticMarkup(
    <CombatCard
      combat={PENDING}
      names={NAMES}
      colorOf={(id) => (id === "p1" ? "#e05a47" : "#3b82f6")}
      controllerOf={() => "human"}
      selfId="p2"
      mode="defense"
      now={9_000}
      defenseWindowMs={15_000}
      reveal={{ mode: "none", durationMs: 0 }}
      busy={false}
      onRollDefense={() => {}}
      {...overrides}
    />,
  );

describe("combat card", () => {
  it("shows the declared attack with face-down defence dice and the roll button", () => {
    const html = card({});
    expect(html).toContain("Attack declared");
    expect(html).toContain("Ashfell");
    expect(html).toContain("Northgate");
    expect(html).toContain("Face-down die");
    expect(html).toContain("Roll defence · 2 dice");
    expect(html).toContain("7s");
  });

  it("tells a watcher who is deciding rather than offering them a roll", () => {
    const html = card({ selfId: "p1", mode: "active-turn" });
    expect(html).not.toContain("Roll defence");
    expect(html).toContain("Waiting for Mina to roll…");
  });

  it("attributes an auto-rolled defence to the timeout, not to the defender", () => {
    const html = card({
      combat: {
        ...PENDING,
        status: "resolved",
        defenderRolls: [5, 3],
        attackerLosses: 1,
        defenderLosses: 1,
        territoryCaptured: false,
        resolutionSource: "timeout",
        defenseDeadlineAt: undefined,
      },
    });
    expect(html).toContain("Throw resolved");
    expect(html).toContain("Northgate holds");
    expect(html).toContain("Auto-rolled after timeout");
  });

  it("offers one clear repeat action only when the resolved pairing remains legal", () => {
    const resolved = {
      ...PENDING,
      status: "resolved" as const,
      defenderRolls: [5, 3],
      attackerLosses: 1,
      defenderLosses: 1,
      territoryCaptured: false,
      resolutionSource: "human" as const,
      defenseDeadlineAt: undefined,
    };
    expect(card({ combat: resolved, onAttackAgain: () => {} })).toContain("Attack again");
    expect(card({ combat: resolved })).not.toContain("Attack again");
    expect(
      card({ combat: { ...resolved, territoryCaptured: true }, onAttackAgain: undefined }),
    ).not.toContain("Attack again");
  });
});
