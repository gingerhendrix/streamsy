/**
 * Pointy-top axial coordinates → SVG geometry.
 *
 * The canonical map stores `(q, r)` and nothing else (see `domain/hex.ts`): pixels
 * are a *rendering* concern, chosen by the client from a hex radius it picks. This
 * module is that choice, kept pure so it can be tested without a DOM and reused by
 * anything that needs to draw the same board.
 *
 * Two derived shapes carry most of the map's legibility:
 *
 *  - **Region outlines.** A country is a set of hexes; its perimeter is exactly the
 *    hex edges whose neighbour is not a member. Stitching those segments into
 *    closed loops gives one path that can be filled (ownership) *and* stroked
 *    (border) with proper joins, instead of six visible seams per tile.
 *  - **Nothing about adjacency is drawn.** Two countries are adjacent because their
 *    hexes touch; the picture already says so, so there are no route lines.
 */

import type { Axial } from "../domain/hex.ts";

export const SQRT3 = Math.sqrt(3);

export interface Point {
  readonly x: number;
  readonly y: number;
}

export interface InsetSegment {
  readonly from: Point;
  readonly to: Point;
}

/**
 * Edge-order neighbour offsets: edge `i` runs from corner `i` to corner `i + 1`
 * and faces `EDGE_NEIGHBORS[i]`. This is the same six directions as
 * `AXIAL_NEIGHBORS`, in the rotational order the corner formula produces rather
 * than the domain's canonical order.
 */
export const EDGE_NEIGHBORS = [
  [1, 0], // east
  [0, 1], // south-east
  [-1, 1], // south-west
  [-1, 0], // west
  [0, -1], // north-west
  [1, -1], // north-east
] as const;

/** Centre of the tile at `(q, r)` for a given hex radius (centre → corner). */
export function hexCenter(hex: Axial, radius: number): Point {
  return { x: radius * SQRT3 * (hex.q + hex.r / 2), y: radius * 1.5 * hex.r };
}

/** Corner `index` (0..5) of a pointy-top hex, starting at the upper-right corner. */
export function hexCorner(hex: Axial, radius: number, index: number): Point {
  const center = hexCenter(hex, radius);
  const angle = (Math.PI / 180) * (60 * index - 30);
  return {
    x: center.x + radius * Math.cos(angle),
    y: center.y + radius * Math.sin(angle),
  };
}

/** The six corners of a tile, in corner order. */
export function hexCorners(hex: Axial, radius: number): Point[] {
  return [0, 1, 2, 3, 4, 5].map((index) => hexCorner(hex, radius, index));
}

const round = (value: number): number => Math.round(value * 1000) / 1000;
const fmt = (point: Point): string => `${round(point.x)} ${round(point.y)}`;
const pointKey = (point: Point): string => `${round(point.x)}|${round(point.y)}`;
const axialKey = (hex: Axial): string => `${hex.q}:${hex.r}`;

/** `points` attribute for a single tile polygon. */
export function hexPolygonPoints(hex: Axial, radius: number): string {
  return hexCorners(hex, radius)
    .map((corner) => `${round(corner.x)},${round(corner.y)}`)
    .join(" ");
}

export interface ViewBox {
  readonly minX: number;
  readonly minY: number;
  readonly width: number;
  readonly height: number;
}

export function viewBoxAttribute(box: ViewBox): string {
  return `${round(box.minX)} ${round(box.minY)} ${round(box.width)} ${round(box.height)}`;
}

/**
 * The tightest box containing every tile, plus padding. Computed from coordinates
 * so it is stable across devices — zoom and pan transform a group *inside* this
 * box rather than changing it.
 */
export function hexesViewBox(
  hexes: readonly Axial[],
  radius: number,
  padding = radius * 0.75,
): ViewBox {
  if (hexes.length === 0) return { minX: 0, minY: 0, width: 1, height: 1 };
  const halfWidth = (radius * SQRT3) / 2;
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (const hex of hexes) {
    const center = hexCenter(hex, radius);
    minX = Math.min(minX, center.x - halfWidth);
    maxX = Math.max(maxX, center.x + halfWidth);
    minY = Math.min(minY, center.y - radius);
    maxY = Math.max(maxY, center.y + radius);
  }
  return {
    minX: minX - padding,
    minY: minY - padding,
    width: maxX - minX + padding * 2,
    height: maxY - minY + padding * 2,
  };
}

type Segment = readonly [Point, Point];

/** Every hex edge of `hexes` whose neighbour across it is not also a member. */
function boundarySegments(hexes: readonly Axial[], radius: number): Segment[] {
  const members = new Set(hexes.map(axialKey));
  const segments: Segment[] = [];
  for (const hex of hexes) {
    const corners = hexCorners(hex, radius);
    for (let edge = 0; edge < 6; edge += 1) {
      const [dq, dr] = EDGE_NEIGHBORS[edge]!;
      if (members.has(axialKey({ q: hex.q + dq, r: hex.r + dr }))) continue;
      segments.push([corners[edge]!, corners[(edge + 1) % 6]!]);
    }
  }
  return segments;
}

/**
 * The outline of a set of tiles as one SVG path of closed subpaths.
 *
 * Fill it with `fill-rule="evenodd"` — a region with an enclosed hole produces two
 * loops whose winding is not guaranteed to be opposite, and even-odd does not care.
 */
export function regionOutlinePath(hexes: readonly Axial[], radius: number): string {
  const segments = boundarySegments(hexes, radius);
  if (segments.length === 0) return "";

  const startingAt = new Map<string, number[]>();
  segments.forEach((segment, index) => {
    const key = pointKey(segment[0]);
    const bucket = startingAt.get(key);
    if (bucket) bucket.push(index);
    else startingAt.set(key, [index]);
  });

  const used: boolean[] = Array.from({ length: segments.length }, () => false);
  const loops: string[] = [];
  for (let index = 0; index < segments.length; index += 1) {
    if (used[index]) continue;
    used[index] = true;
    const start = segments[index]![0];
    let cursor = segments[index]![1];
    const steps = [`M ${fmt(start)}`];
    // Walk the boundary edge-to-edge until it returns to `start`. A well-formed
    // region always closes; the `undefined` guard keeps a malformed one finite.
    for (;;) {
      if (pointKey(cursor) === pointKey(start)) break;
      steps.push(`L ${fmt(cursor)}`);
      const next = (startingAt.get(pointKey(cursor)) ?? []).find((candidate) => !used[candidate]);
      if (next === undefined) break;
      used[next] = true;
      cursor = segments[next]![1];
    }
    loops.push(`${steps.join(" ")} Z`);
  }
  return loops.join(" ");
}

/**
 * Pull both ends of a route in towards its middle.
 *
 * An attack route runs anchor to anchor, and an anchor is exactly where the army
 * counter is drawn — so an arrowhead placed at the raw endpoint lands underneath a
 * counter that is both larger than the head and painted after it, and the direction
 * of the attack becomes unreadable. Insetting the ends is what makes the head
 * visible; the caller decides by how much, because only it knows the counter size.
 *
 * Adjacent countries can sit closer together than the two insets combined, so the
 * insets are scaled down together rather than allowed to cross: a short route stays
 * a shorter line, never an inverted one.
 */
export function insetSegment(
  from: Point,
  to: Point,
  startInset: number,
  endInset: number,
): InsetSegment {
  const span = Math.hypot(to.x - from.x, to.y - from.y);
  const total = startInset + endInset;
  if (span === 0 || total <= 0) return { from, to };
  // Never eat more than this much of the run, so even neighbours whose anchors
  // nearly touch keep a stretch of line between the two ends.
  const budget = span * 0.7;
  const scale = total > budget ? budget / total : 1;
  const unit = { x: (to.x - from.x) / span, y: (to.y - from.y) / span };
  return {
    from: { x: from.x + unit.x * startInset * scale, y: from.y + unit.y * startInset * scale },
    to: { x: to.x - unit.x * endInset * scale, y: to.y - unit.y * endInset * scale },
  };
}

export interface LossBadgeLayoutOptions {
  /** Radius of the roundel a loss figure is drawn on. */
  badgeRadius: number;
  /** Radius of an army counter — the obstacle, same one the labels avoid. */
  counterRadius: number;
  /** Every counter centre on the map, the two ends of this route included. */
  counters: readonly Point[];
  /** Preferred distance in from each anchor, along the run. */
  inset: number;
  /** Preferred distance off the route, so the arrow does not strike the figures. */
  lift: number;
}

/**
 * Where the two loss figures of one throw sit.
 *
 * Both readings the overlay exists to give are positional: *which side* lost what.
 * A fixed inset from each end loses that as soon as the run is short — two adjacent
 * anchors are close enough that `span / 2` puts both figures on the same point, and a
 * mutual-loss bounce renders as one smudged roundel. So the inset is capped by what
 * keeps the two roundels apart rather than by the midpoint: each figure stays as near
 * its own end as the pair's separation allows.
 *
 * The lift is then chosen rather than assumed. Counters are fixed at anchors — unlike
 * the name plates, which are themselves the output of a de-collision pass — so a
 * figure can be flipped to the other side of the route, or pushed further off it,
 * without depending on any layout that moves. Nothing else on the map is avoided: a
 * plate is a name the viewer can read again in a moment, a counter is a number they
 * may need now, and the preferred placement is tried first so an unobstructed throw
 * looks exactly as it did before.
 */
export function lossBadgePlacements(
  from: Point,
  to: Point,
  options: LossBadgeLayoutOptions,
): { attacker: Point; defender: Point } | null {
  const span = Math.hypot(to.x - from.x, to.y - from.y);
  if (span === 0) return null;
  const along = { x: (to.x - from.x) / span, y: (to.y - from.y) / span };
  const across = { x: -along.y, y: along.x };

  // Two roundels plus a hair of map between them: below this they read as one mark.
  const minSeparation = options.badgeRadius * 2 + 3;
  const inset = Math.min(options.inset, Math.max(0, (span - minSeparation) / 2));
  const clearance = options.counterRadius + options.badgeRadius;

  const at = (base: Point, direction: number, side: number, lift: number): Point => ({
    x: base.x + along.x * inset * direction + across.x * lift * side,
    y: base.y + along.y * inset * direction + across.y * lift * side,
  });
  /** How deeply this placement eats into the counters it overlaps; 0 is clear. */
  const covers = (point: Point): number => {
    let total = 0;
    for (const counter of options.counters) {
      const gap = clearance - Math.hypot(point.x - counter.x, point.y - counter.y);
      if (gap > 0) total += gap;
    }
    return total;
  };
  // Preference order: the established placement, then its mirror, then the same two
  // pushed a counter's width further off the route for a genuinely crowded run.
  const offsets = [
    { side: 1, lift: options.lift },
    { side: -1, lift: options.lift },
    { side: 1, lift: options.lift * 1.6 },
    { side: -1, lift: options.lift * 1.6 },
  ] as const;
  const choose = (base: Point, direction: number): { point: Point; side: number } => {
    let best = { point: at(base, direction, 1, options.lift), side: 1 };
    let bestCover = covers(best.point);
    for (const offset of offsets.slice(1)) {
      if (bestCover === 0) break;
      const point = at(base, direction, offset.side, offset.lift);
      const cover = covers(point);
      if (cover < bestCover) {
        best = { point, side: offset.side };
        bestCover = cover;
      }
    }
    return best;
  };

  const attacker = choose(from, 1);
  let defender = choose(to, -1);
  // A run short enough that the inset alone cannot part them: opposite sides of the
  // route always can, and each figure is still at its own end of the arrow.
  if (
    Math.hypot(attacker.point.x - defender.point.x, attacker.point.y - defender.point.y) <
    minSeparation
  ) {
    defender = {
      point: at(to, -1, -attacker.side, options.lift),
      side: -attacker.side,
    };
  }
  return { attacker: attacker.point, defender: defender.point };
}

/**
 * A gently curved attack route from one country's label anchor to another's, bowed
 * perpendicular to the straight line so source and target stay readable underneath.
 */
export function attackArrowPath(from: Point, to: Point, bow = 0.16): string {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const midX = (from.x + to.x) / 2;
  const midY = (from.y + to.y) / 2;
  return `M ${fmt(from)} Q ${fmt({ x: midX - dy * bow, y: midY + dx * bow })} ${fmt(to)}`;
}
