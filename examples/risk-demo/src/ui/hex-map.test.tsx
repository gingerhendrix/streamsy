/**
 * The map rendered for real: one server-rendered pass over a small hand-built
 * board, checking the things a pure geometry test cannot — that every country
 * gets exactly one name and one army badge, that names are drawn at the
 * de-collided positions rather than blindly above their anchors (D3), and that
 * each country is still a labelled, focusable control.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ProjectedHexV2 } from "../board/projection-v2.ts";
import {
  HexMap,
  territoryInteractionState,
  type MapTerritory,
  type TerritoryInteractionState,
} from "./hex-map.tsx";

/** Two three-hex countries side by side, with long names that would collide. */
const HEXES: ProjectedHexV2[] = [
  { id: "h:0:0", q: 0, r: 0, territoryId: "t1", terrain: "plains" },
  { id: "h:1:0", q: 1, r: 0, territoryId: "t1", terrain: "forest" },
  { id: "h:0:1", q: 0, r: 1, territoryId: "t1", terrain: "hills" },
  { id: "h:2:0", q: 2, r: 0, territoryId: "t2", terrain: "desert" },
  { id: "h:3:0", q: 3, r: 0, territoryId: "t2", terrain: "mountains" },
  { id: "h:2:1", q: 2, r: 1, territoryId: "t2", terrain: "plains" },
];

const TERRITORIES: MapTerritory[] = [
  {
    id: "t1",
    name: "Cinderhold",
    continentId: "c1",
    ownerId: "p1",
    armies: 7,
    hexIds: ["h:0:0", "h:1:0", "h:0:1"],
    labelAnchor: { q: 0, r: 0 },
  },
  {
    id: "t2",
    name: "Windbarrow",
    continentId: "c1",
    ownerId: "p2",
    armies: 3,
    hexIds: ["h:2:0", "h:3:0", "h:2:1"],
    labelAnchor: { q: 2, r: 0 },
  },
];

function render(stateOf: (id: string) => TerritoryInteractionState = () => "normal"): string {
  return renderToStaticMarkup(
    <HexMap
      hexes={HEXES}
      territories={TERRITORIES}
      continents={[
        {
          id: "c1",
          name: "Northreach",
          territoryIds: ["t1", "t2"],
          reinforcementBonus: 2,
          palette: { hue: 160, pattern: "weave" },
        },
      ]}
      colorOf={(id) => (id === "p1" ? "#e05a47" : "#3b82f6")}
      ownerNameOf={(id) => (id === "p1" ? "Ada" : "Mina")}
      stateOf={stateOf}
      actionable={new Set(["t1"])}
      focusedId={null}
      onSelect={() => {}}
      pendingReinforcements={new Map([["t1", 2]])}
      onDecrement={() => {}}
      onFocus={() => {}}
      onHover={() => {}}
      route={null}
      zoom={1}
      pan={{ x: 0, y: 0 }}
    />,
  );
}

describe("hex map", () => {
  it("labels each country once and gives it one army badge", () => {
    const html = render();
    expect(html.match(/class="country-label"/g)).toHaveLength(2);
    expect(html.match(/class="army-marker"/g)).toHaveLength(2);
    expect(html).toContain("Cinderhold");
    expect(html).toContain("Windbarrow");
    // Armies are drawn once per country, never once per tile.
    expect(html.match(/class="army-count"/g)).toHaveLength(2);
    expect(html.match(/class="pending-army-count"/g)).toHaveLength(1);
    expect(html).toContain(">+2</text>");
  });

  it("draws names at their laid-out positions, not on top of each other", () => {
    const html = render();
    const labelPositions = [...html.matchAll(/class="country-label" x="([-\d.]+)" y="([-\d.]+)"/g)];
    expect(labelPositions).toHaveLength(2);
    const [first, second] = labelPositions;
    expect(first![1]).not.toBe(second![1]);
  });

  it("exposes every country as a labelled, focusable control", () => {
    const html = render();
    expect(html).toContain(
      'aria-label="Cinderhold, 7 armies, plus 2 pending reinforcements, held by Ada"',
    );
    expect(html).toContain('aria-label="Windbarrow, 3 armies, held by Mina"');
    expect(html.match(/role="button"/g)).toHaveLength(2);
    // Legality is the decision resource's word: only `t1` is actionable here.
    expect(html).toContain('aria-disabled="true"');
  });
});

describe("territory interaction states", () => {
  it("uses active, hover, dimmed, normal precedence without changing actionability", () => {
    expect(
      territoryInteractionState({ active: true, hovered: true, choosing: true, actionable: false }),
    ).toBe("active");
    expect(
      territoryInteractionState({
        active: false,
        hovered: true,
        choosing: true,
        actionable: false,
      }),
    ).toBe("hover");
    expect(
      territoryInteractionState({
        active: false,
        hovered: false,
        choosing: true,
        actionable: false,
      }),
    ).toBe("dimmed");
    expect(
      territoryInteractionState({
        active: false,
        hovered: false,
        choosing: true,
        actionable: true,
      }),
    ).toBe("normal");
  });

  it("renders public state markers and only presses active territories", () => {
    const html = render((id) => (id === "t1" ? "active" : "dimmed"));
    expect(html).toContain('data-state="active"');
    expect(html).toContain('data-state="dimmed"');
    expect(html).toContain('class="highlight active"');
    expect(html).toContain('aria-pressed="true"');
    expect(html).toContain('class="territory-labels territory-state-dimmed"');
  });
});
