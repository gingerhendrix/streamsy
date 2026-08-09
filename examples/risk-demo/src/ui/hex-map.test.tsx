/**
 * The map rendered for real: one server-rendered pass over a small hand-built
 * board, checking the things a pure geometry test cannot — that every country
 * gets exactly one name and one army badge, that names are drawn at the
 * de-collided positions rather than blindly above their anchors, and that
 * each country is still a labelled, focusable control.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ProjectedHex } from "../board/projection.ts";
import type { AttackTrace } from "./attack-trace.ts";
import {
  HexMap,
  territoryInteractionState,
  throwLayerKey,
  type MapTerritory,
  type TerritoryInteractionState,
} from "./hex-map.tsx";

const near = (point: { x: number; y: number }, other: { x: number; y: number }): number =>
  Math.hypot(point.x - other.x, point.y - other.y);

/** Two three-hex countries side by side, with long names that would collide. */
const HEXES: ProjectedHex[] = [
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

function render(
  stateOf: (id: string) => TerritoryInteractionState = () => "normal",
  trace: AttackTrace | null = null,
  territories: MapTerritory[] = TERRITORIES,
): string {
  return renderToStaticMarkup(
    <HexMap
      hexes={HEXES}
      territories={territories}
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
      trace={trace}
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

describe("resolved throws on the map", () => {
  const BOUNCE: AttackTrace = {
    attackId: "atk-1",
    from: "t1",
    to: "t2",
    attackerLosses: 2,
    defenderLosses: 0,
    captured: false,
    sourceOffset: "0006",
  };

  it("draws nothing until a throw has resolved", () => {
    expect(render()).not.toContain("layer-throw");
  });

  it("shows the route and only the losses that were actually taken", () => {
    const html = render(() => "normal", BOUNCE);
    expect(html).toContain('class="throw-route"');
    // Two lost armies for the attacker, none for the defender: one badge, not two.
    expect(html.match(/class="loss-badge"/g)).toHaveLength(1);
    expect(html).toContain("−2");
    expect(html).not.toContain("capture-flash");
  });

  it("marks a capture on the country that changed hands", () => {
    const html = render(() => "normal", { ...BOUNCE, defenderLosses: 1, captured: true });
    expect(html).toContain('class="throw-route captured"');
    expect(html).toContain('class="capture-flash"');
    expect(html.match(/class="loss-badge"/g)).toHaveLength(2);
    // The head follows the route into signal red; a two-tone arrow would read as
    // two statements about one throw.
    expect(html).toContain('marker-end="url(#throw-arrowhead-captured)"');
  });

  it("draws the arrow and the losses over the name plates, and the capture wash under them", () => {
    const html = render(() => "normal", { ...BOUNCE, defenderLosses: 1, captured: true });
    const labels = html.indexOf('class="layer-labels"');
    // Loss figures behind a name plate or an army counter are unreadable, and the
    // figures are the whole point of the overlay — so they are painted last.
    expect(html.indexOf('class="layer-throw-marks"')).toBeGreaterThan(labels);
    // The wash is a region fill and stays with the other region fills, so the
    // captured country keeps a legible name and army count while it changes hands.
    expect(html.indexOf('class="layer-throw"')).toBeLessThan(labels);
    // ...and still below the hit targets, which must keep receiving clicks.
    expect(html.indexOf('class="layer-throw-marks"')).toBeLessThan(
      html.indexOf('class="layer-interaction"'),
    );
  });

  it("stops the route short of both army counters so the arrowhead is not buried", () => {
    const html = render(() => "normal", BOUNCE);
    const path =
      /class="throw-route" d="M ([-\d.]+) ([-\d.]+) Q [-\d.]+ [-\d.]+ ([-\d.]+) ([-\d.]+)"/.exec(
        html,
      );
    expect(path).not.toBeNull();
    // The anchors are the two label anchors, which is exactly where the counters are.
    const anchors = [
      ...html.matchAll(/class="army-marker" cx="([-\d.]+)" cy="([-\d.]+)" r="([-\d.]+)"/g),
    ];
    expect(anchors).toHaveLength(2);
    const radius = Number(anchors[0]![3]);
    const start = { x: Number(path![1]), y: Number(path![2]) };
    const end = { x: Number(path![3]), y: Number(path![4]) };
    for (const anchor of anchors) {
      const centre = { x: Number(anchor[1]), y: Number(anchor[2]) };
      for (const point of [start, end]) {
        expect(Math.hypot(point.x - centre.x, point.y - centre.y)).toBeGreaterThan(radius - 0.01);
      }
    }
  });

  it("keys its two layers apart so neither leaks nor restarts its own fade", () => {
    // Both groups are children of the same element, so keying both with the bare
    // `attackId` is a duplicate key: React answered it by duplicating capture washes
    // in the DOM without bound and remounting the marks group — restarting the 4.5s
    // fade — on every unrelated board update, so a trace never cleared.
    expect(throwLayerKey("atk-1", "wash")).not.toBe(throwLayerKey("atk-1", "marks"));
    // Each key still changes with the throw, which is what makes a new throw remount
    // its group and start the fade again.
    expect(throwLayerKey("atk-2", "marks")).not.toBe(throwLayerKey("atk-1", "marks"));

    const html = render(() => "normal", { ...BOUNCE, defenderLosses: 1, captured: true });
    // One wash, one marks group — a keyed sibling pair, rendered once each.
    expect(html.match(/class="layer-throw"/g)).toHaveLength(1);
    expect(html.match(/class="layer-throw-marks"/g)).toHaveLength(1);
  });

  it("keeps the two loss figures apart and at their own ends on the shortest run", () => {
    // Adjacent anchors one hex apart: the fixed inset collapsed both figures onto the
    // midpoint, so a mutual-loss bounce read as one smudged roundel, not two numbers.
    const adjacent = TERRITORIES.map((territory) =>
      territory.id === "t1" ? { ...territory, labelAnchor: { q: 1, r: 0 } } : territory,
    );
    const html = render(() => "normal", { ...BOUNCE, defenderLosses: 1 }, adjacent);
    const badges = [
      ...html.matchAll(
        /<g class="loss-badge"><circle cx="([-\d.]+)" cy="([-\d.]+)" r="([-\d.]+)"/g,
      ),
    ];
    expect(badges).toHaveLength(2);
    const points = badges.map((badge) => ({ x: Number(badge[1]), y: Number(badge[2]) }));
    const radius = Number(badges[0]![3]);
    expect(Math.hypot(points[0]!.x - points[1]!.x, points[0]!.y - points[1]!.y)).toBeGreaterThan(
      radius * 2,
    );
    // The attacker's figure belongs to the attacker's end, and stays nearer to it.
    const anchors = [...html.matchAll(/class="army-marker" cx="([-\d.]+)" cy="([-\d.]+)"/g)].map(
      (anchor) => ({ x: Number(anchor[1]), y: Number(anchor[2]) }),
    );
    expect(near(points[0]!, anchors[0]!)).toBeLessThan(near(points[0]!, anchors[1]!));
    expect(near(points[1]!, anchors[1]!)).toBeLessThan(near(points[1]!, anchors[0]!));
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
