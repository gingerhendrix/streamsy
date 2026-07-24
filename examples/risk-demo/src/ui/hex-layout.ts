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

/**
 * Edge-order neighbour offsets: edge `i` runs from corner `i` to corner `i + 1`
 * and faces `EDGE_NEIGHBORS[i]`. This is the same six directions as
 * `AXIAL_NEIGHBORS`, in the rotational order the corner formula produces rather
 * than the domain's canonical order.
 */
export const EDGE_NEIGHBORS = [
  [+1, 0], // east
  [0, +1], // south-east
  [-1, +1], // south-west
  [-1, 0], // west
  [0, -1], // north-west
  [+1, -1], // north-east
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
 * box rather than changing it (design spec §8.6).
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
