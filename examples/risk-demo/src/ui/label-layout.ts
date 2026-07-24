/**
 * Deterministic de-collision for country labels (D3).
 *
 * A procedural map puts countries wherever the generator put them, so two label
 * anchors can land close enough that the names run together — a real game showed
 * "Cinderhold" and "Windbarrow" rendering as `CINDERWINDBARROW`, and "Graymarch"
 * disappearing behind a neighbouring army badge. Nothing about that is canonical:
 * anchors are map data, but *where the text sits* is rendering, and rendering can
 * move it.
 *
 * The pass is a greedy nudge. Each label tries its preferred spot above the
 * anchor first, then a fixed ladder of offsets, and takes the first that clears
 * every already-placed label and every army badge. Two properties matter more
 * than optimality:
 *
 *  - **Determinism.** The same map always produces the same placement — labels
 *    must not shuffle when an unrelated row changes — so candidates are tried in
 *    a fixed order and labels are processed in a fixed order.
 *  - **Honesty when it fails.** A label that cannot be placed cleanly is moved to
 *    its least-bad spot and marked `leader`, and the renderer draws a line back to
 *    the country it names rather than leaving the reader to guess.
 */

import type { Point } from "./hex-layout.ts";

export interface LabelRequest {
  id: string;
  /** The country's canonical label anchor, in SVG units. */
  anchor: Point;
  text: string;
}

export interface LabelPlacement {
  id: string;
  /** Centre of the placed text. */
  x: number;
  y: number;
  width: number;
  height: number;
  /** True when the label had to move far enough to need a line back to its country. */
  leader: boolean;
}

export interface LabelLayoutOptions {
  /** Army-badge radius; badges are obstacles no label may sit under. */
  badgeRadius: number;
  /**
   * Preferred distance from the anchor to the centre of the label. Raised
   * automatically if it would leave the text sitting on its own army badge.
   */
  baseOffset: number;
  /** Rendered font size, used to estimate the text box. */
  fontSize?: number;
  /** Nudge increment when the preferred distance is taken. */
  step?: number;
  /** How many distances to try before giving up and drawing a leader. */
  rings?: number;
  /** Breathing room required between boxes. */
  padding?: number;
}

interface Box {
  left: number;
  right: number;
  top: number;
  bottom: number;
}

/**
 * Average advance per character for the map's uppercase bold label face,
 * including its letter-spacing. An estimate is enough: the layout only needs to
 * know roughly how wide a name is, and over-estimating merely spaces labels out.
 */
const CHARACTER_ADVANCE = 0.68;

export function measureLabelWidth(text: string, fontSize: number): number {
  return Math.max(fontSize, text.length * fontSize * CHARACTER_ADVANCE);
}

/**
 * Where to try, in order: straight up, straight down, then the four diagonals,
 * each at increasing distance. Horizontal shift is a share of the label's own
 * width so a diagonal actually clears the badge under the anchor.
 */
const DIRECTIONS: ReadonlyArray<{ dx: number; dy: number }> = [
  { dx: 0, dy: -1 },
  { dx: 0, dy: 1 },
  { dx: -1, dy: -1 },
  { dx: 1, dy: -1 },
  { dx: -1, dy: 1 },
  { dx: 1, dy: 1 },
];

const round = (value: number): number => Math.round(value * 1000) / 1000;

function boxOf(x: number, y: number, width: number, height: number, padding: number): Box {
  return {
    left: x - width / 2 - padding,
    right: x + width / 2 + padding,
    top: y - height / 2 - padding,
    bottom: y + height / 2 + padding,
  };
}

function overlapArea(a: Box, b: Box): number {
  const width = Math.min(a.right, b.right) - Math.max(a.left, b.left);
  const height = Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top);
  return width > 0 && height > 0 ? width * height : 0;
}

/** How far a circle intrudes into a box, as an area-like penalty; 0 when clear. */
function circleOverlap(center: Point, radius: number, box: Box): number {
  const nearestX = Math.min(Math.max(center.x, box.left), box.right);
  const nearestY = Math.min(Math.max(center.y, box.top), box.bottom);
  const dx = center.x - nearestX;
  const dy = center.y - nearestY;
  const distance = Math.sqrt(dx * dx + dy * dy);
  return distance >= radius ? 0 : (radius - distance) ** 2;
}

/**
 * Place every country label so that no two overlap and none sits under an army
 * badge, given the same input every time.
 */
export function layoutCountryLabels(
  requests: readonly LabelRequest[],
  options: LabelLayoutOptions,
): LabelPlacement[] {
  const fontSize = options.fontSize ?? 10.5;
  const step = options.step ?? fontSize;
  const rings = options.rings ?? 4;
  const padding = options.padding ?? 1.5;
  const height = fontSize * 1.25;

  const badges = requests.map((request) => request.anchor);
  // Top-to-bottom, left-to-right, id last: a stable order that also tends to give
  // the crowded middle of the map somewhere to go.
  const ordered = requests.toSorted(
    (a, b) => a.anchor.y - b.anchor.y || a.anchor.x - b.anchor.x || (a.id < b.id ? -1 : 1),
  );

  // Never start closer than the anchor's own badge allows: the preferred spot
  // should be a real candidate, not one that always fails on the first check.
  const nearest = Math.max(options.baseOffset, options.badgeRadius + height / 2 + padding + 0.5);

  const placedBoxes: Box[] = [];
  const placements = new Map<string, LabelPlacement>();

  for (const request of ordered) {
    const width = measureLabelWidth(request.text, fontSize);
    let best: { x: number; y: number; penalty: number; distance: number } | null = null;

    outer: for (let ring = 0; ring < rings; ring += 1) {
      const distance = nearest + ring * step;
      for (const direction of DIRECTIONS) {
        const x = request.anchor.x + direction.dx * (width / 2 + options.badgeRadius * 0.9);
        const y = request.anchor.y + direction.dy * distance;
        const box = boxOf(x, y, width, height, padding);
        const penalty =
          placedBoxes.reduce((sum, other) => sum + overlapArea(box, other), 0) +
          badges.reduce((sum, badge) => sum + circleOverlap(badge, options.badgeRadius, box), 0);
        if (penalty === 0) {
          best = { x, y, penalty, distance };
          break outer;
        }
        if (!best || penalty < best.penalty) best = { x, y, penalty, distance };
      }
    }

    const chosen = best ?? {
      x: request.anchor.x,
      y: request.anchor.y - nearest,
      penalty: 0,
      distance: nearest,
    };
    placedBoxes.push(boxOf(chosen.x, chosen.y, width, height, padding));
    placements.set(request.id, {
      id: request.id,
      x: round(chosen.x),
      y: round(chosen.y),
      width: round(width),
      height: round(height),
      // Either it travelled, or it could not get clear at all: both are cases
      // where the reader needs the line to know which country this names.
      leader: chosen.distance > nearest || chosen.penalty > 0,
    });
  }

  // Return in the caller's order so rendering keys stay put.
  return requests.map((request) => placements.get(request.id)!);
}
