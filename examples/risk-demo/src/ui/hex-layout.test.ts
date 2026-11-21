import { describe, expect, it } from "vitest";

import { AXIAL_NEIGHBORS } from "../domain/hex.ts";
import {
  EDGE_NEIGHBORS,
  SQRT3,
  attackArrowPath,
  hexCenter,
  hexCorners,
  hexPolygonPoints,
  hexesViewBox,
  insetSegment,
  lossBadgePlacements,
  regionOutlinePath,
  viewBoxAttribute,
  type Point,
} from "./hex-layout.ts";

const RADIUS = 10;

/** Count how many vertices a path's subpaths declare, per subpath. */
function subpathVertexCounts(path: string): number[] {
  return path
    .split("Z")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => (part.match(/[ML]/g) ?? []).length);
}

describe("pointy-top hex geometry", () => {
  it("places the origin at zero and steps by the pointy-top basis", () => {
    expect(hexCenter({ q: 0, r: 0 }, RADIUS)).toEqual({ x: 0, y: 0 });
    // +q is due east; +r is south-east.
    expect(hexCenter({ q: 1, r: 0 }, RADIUS)).toEqual({ x: RADIUS * SQRT3, y: 0 });
    const southEast = hexCenter({ q: 0, r: 1 }, RADIUS);
    expect(southEast.x).toBeCloseTo((RADIUS * SQRT3) / 2, 9);
    expect(southEast.y).toBeCloseTo(RADIUS * 1.5, 9);
  });

  it("uses the same six directions as the domain, in corner order", () => {
    const canonical = AXIAL_NEIGHBORS.map(([dq, dr]) => `${dq}:${dr}`).toSorted();
    const edges = EDGE_NEIGHBORS.map(([dq, dr]) => `${dq}:${dr}`).toSorted();
    expect(edges).toEqual(canonical);
  });

  it("puts edge i exactly halfway to the neighbour it faces", () => {
    const hex = { q: 2, r: -1 };
    const corners = hexCorners(hex, RADIUS);
    const center = hexCenter(hex, RADIUS);
    EDGE_NEIGHBORS.forEach(([dq, dr], edge) => {
      const neighbour = hexCenter({ q: hex.q + dq, r: hex.r + dr }, RADIUS);
      const start = corners[edge]!;
      const end = corners[(edge + 1) % 6]!;
      expect((start.x + end.x) / 2).toBeCloseTo((center.x + neighbour.x) / 2, 9);
      expect((start.y + end.y) / 2).toBeCloseTo((center.y + neighbour.y) / 2, 9);
    });
  });

  it("emits six polygon points per tile", () => {
    expect(hexPolygonPoints({ q: 0, r: 0 }, RADIUS).split(" ")).toHaveLength(6);
  });
});

describe("region outlines", () => {
  it("traces a lone tile as one closed six-sided loop", () => {
    const path = regionOutlinePath([{ q: 0, r: 0 }], RADIUS);
    expect(subpathVertexCounts(path)).toEqual([6]);
    expect(path.trimEnd().endsWith("Z")).toBe(true);
  });

  it("drops the shared edge between two touching tiles", () => {
    // Ten outer edges remain once the pair's shared edge is excluded.
    const path = regionOutlinePath(
      [
        { q: 0, r: 0 },
        { q: 1, r: 0 },
      ],
      RADIUS,
    );
    expect(subpathVertexCounts(path)).toEqual([10]);
  });

  it("traces a ring and its hole as two separate loops", () => {
    const ring = EDGE_NEIGHBORS.map(([dq, dr]) => ({ q: dq, r: dr }));
    const counts = subpathVertexCounts(regionOutlinePath(ring, RADIUS)).toSorted((a, b) => a - b);
    // The enclosed hole is a six-sided inner loop; the outer rim has eighteen edges.
    expect(counts).toEqual([6, 18]);
  });

  it("has nothing to draw for an empty region", () => {
    expect(regionOutlinePath([], RADIUS)).toBe("");
  });

  it("is unaffected by the order tiles arrive in", () => {
    const tiles = [
      { q: 0, r: 0 },
      { q: 1, r: 0 },
      { q: 0, r: 1 },
    ];
    expect(subpathVertexCounts(regionOutlinePath(tiles, RADIUS))).toEqual(
      subpathVertexCounts(regionOutlinePath(tiles.toReversed(), RADIUS)),
    );
  });
});

describe("view box", () => {
  it("contains every corner of every tile", () => {
    const tiles = [
      { q: 0, r: 0 },
      { q: 3, r: -1 },
      { q: -2, r: 2 },
    ];
    const box = hexesViewBox(tiles, RADIUS, 0);
    for (const tile of tiles) {
      for (const corner of hexCorners(tile, RADIUS)) {
        expect(corner.x).toBeGreaterThanOrEqual(box.minX - 1e-9);
        expect(corner.x).toBeLessThanOrEqual(box.minX + box.width + 1e-9);
        expect(corner.y).toBeGreaterThanOrEqual(box.minY - 1e-9);
        expect(corner.y).toBeLessThanOrEqual(box.minY + box.height + 1e-9);
      }
    }
  });

  it("stays a valid box when there are no tiles yet", () => {
    expect(viewBoxAttribute(hexesViewBox([], RADIUS))).toBe("0 0 1 1");
  });
});

describe("attack route", () => {
  it("bows a quadratic curve between the two label anchors", () => {
    const path = attackArrowPath({ x: 0, y: 0 }, { x: 100, y: 0 });
    expect(path).toMatch(/^M 0 0 Q [\d.-]+ [\d.-]+ 100 0$/);
    // The bow is perpendicular to the run, so a horizontal route bends vertically.
    expect(path).toContain("Q 50 16");
  });
});

describe("route insets", () => {
  it("pulls each end in by its own inset so an arrowhead clears the army counter", () => {
    const ends = insetSegment({ x: 0, y: 0 }, { x: 100, y: 0 }, 16, 22);
    expect(ends.from).toEqual({ x: 16, y: 0 });
    expect(ends.to).toEqual({ x: 78, y: 0 });
  });

  it("insets along the run, not along an axis", () => {
    const ends = insetSegment({ x: 0, y: 0 }, { x: 0, y: -50 }, 10, 10);
    expect(ends.from.y).toBeCloseTo(-10, 9);
    expect(ends.to.y).toBeCloseTo(-40, 9);
    expect(ends.from.x).toBeCloseTo(0, 9);
  });

  it("scales both insets down rather than letting close neighbours invert the line", () => {
    const ends = insetSegment({ x: 0, y: 0 }, { x: 20, y: 0 }, 16, 22);
    // Still pointing the same way, still leaving a stretch of line to see.
    expect(ends.to.x).toBeGreaterThan(ends.from.x);
    expect(ends.to.x - ends.from.x).toBeCloseTo(20 * 0.3, 9);
    // The larger inset still takes the larger share.
    expect(ends.from.x).toBeLessThan(20 - ends.to.x);
  });

  it("leaves a zero-length run alone rather than dividing by it", () => {
    const point = { x: 4, y: 9 };
    expect(insetSegment(point, point, 5, 5)).toEqual({ from: point, to: point });
  });
});

const apart = (a: Point, b: Point): number => Math.hypot(a.x - b.x, a.y - b.y);

describe("loss badge placement", () => {
  const FROM = { x: 0, y: 0 };
  /** The client's real numbers, so these cases are the ones the map actually draws. */
  const OPTIONS = {
    badgeRadius: 11.6,
    counterRadius: 16.1,
    counters: [FROM, { x: 100, y: 0 }],
    inset: 30.6,
    lift: 16.9,
  };
  const MIN_SEPARATION = OPTIONS.badgeRadius * 2 + 3;

  it("keeps the preferred placement when nothing is in the way", () => {
    const badges = lossBadgePlacements(FROM, { x: 100, y: 0 }, OPTIONS);
    expect(badges).toEqual({
      attacker: { x: 30.6, y: 16.9 },
      defender: { x: 69.4, y: 16.9 },
    });
  });

  it("parts the two figures on a run too short to inset them both", () => {
    // Two adjacent anchors: a fixed inset put both figures on the midpoint, so a
    // mutual-loss bounce read as one smudged roundel instead of two numbers.
    const to = { x: 45, y: 0 };
    const badges = lossBadgePlacements(FROM, to, { ...OPTIONS, counters: [FROM, to] })!;
    expect(apart(badges.attacker, badges.defender)).toBeGreaterThanOrEqual(MIN_SEPARATION);
    // ...and each figure is still the one at its own end of the arrow.
    expect(apart(badges.attacker, FROM)).toBeLessThan(apart(badges.attacker, to));
    expect(apart(badges.defender, to)).toBeLessThan(apart(badges.defender, FROM));
  });

  it("flips a figure to the other side of the route rather than over a counter", () => {
    const to = { x: 100, y: 0 };
    // A third country's counter sitting exactly where the attacker's figure prefers.
    const counters = [FROM, to, { x: 30.6, y: 16.9 }];
    const badges = lossBadgePlacements(FROM, to, { ...OPTIONS, counters })!;
    expect(badges.attacker.y).toBeLessThan(0);
    for (const counter of counters) {
      expect(apart(badges.attacker, counter)).toBeGreaterThan(
        OPTIONS.counterRadius + OPTIONS.badgeRadius - 0.01,
      );
    }
    // The unobstructed figure is left exactly where it was.
    expect(badges.defender).toEqual({ x: 69.4, y: 16.9 });
  });

  it("takes the least-covered placement when every side is crowded", () => {
    const to = { x: 100, y: 0 };
    const counters = [FROM, to, { x: 30.6, y: 16.9 }, { x: 30.6, y: -16.9 }];
    const covers = (point: { x: number; y: number }): number =>
      counters.reduce(
        (total, counter) =>
          total + Math.max(0, OPTIONS.counterRadius + OPTIONS.badgeRadius - apart(point, counter)),
        0,
      );
    const badges = lossBadgePlacements(FROM, to, { ...OPTIONS, counters })!;
    // Both preferred sides are taken and no candidate is clear, so the figure moves
    // further off the route: it cannot avoid every number, but it covers less of one.
    expect(Math.abs(badges.attacker.y)).toBeGreaterThan(OPTIONS.lift);
    expect(covers(badges.attacker)).toBeLessThan(covers({ x: 30.6, y: OPTIONS.lift }));
  });

  it("has nowhere to place figures on a zero-length run", () => {
    expect(lossBadgePlacements(FROM, FROM, OPTIONS)).toBeNull();
  });
});
