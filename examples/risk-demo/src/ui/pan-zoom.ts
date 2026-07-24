/**
 * Pan and zoom arithmetic for the hex map (D7), kept pure and dependency-free.
 *
 * The SVG's `viewBox` never changes — it is derived from canonical coordinates so
 * the board is the same shape on every device (design spec §8.6). Zooming and
 * panning transform a group *inside* that box, which is why every function here
 * works in viewBox units and the only browser-flavoured helper is the one that
 * converts a pointer's client position into them.
 *
 * Buttons still drive the same state; drag and wheel are additional ways to reach
 * it, not a second source of truth.
 */

import type { Point, ViewBox } from "./hex-layout.ts";

export interface ViewTransform {
  zoom: number;
  pan: Point;
}

/** Matches the zoom buttons' range, so the two controls cannot disagree. */
export const ZOOM_LIMITS = { min: 0.6, max: 2.4 } as const;

export function clampZoom(
  zoom: number,
  limits: { min: number; max: number } = ZOOM_LIMITS,
): number {
  return Math.min(limits.max, Math.max(limits.min, zoom));
}

/** Shift the view by a delta already expressed in viewBox units. */
export function panBy(view: ViewTransform, delta: Point): ViewTransform {
  return { zoom: view.zoom, pan: { x: view.pan.x + delta.x, y: view.pan.y + delta.y } };
}

/**
 * Zoom by `factor` while keeping whatever is under `focus` under `focus`.
 *
 * A tile is drawn at `pan + zoom · tile`, so holding that product fixed for the
 * focus point gives the new pan directly. Without this, zooming with the wheel
 * walks the map out from under the cursor.
 */
export function zoomAbout(
  view: ViewTransform,
  focus: Point,
  factor: number,
  limits: { min: number; max: number } = ZOOM_LIMITS,
): ViewTransform {
  const zoom = clampZoom(view.zoom * factor, limits);
  if (zoom === view.zoom) return view;
  const ratio = zoom / view.zoom;
  return {
    zoom,
    pan: {
      x: focus.x - (focus.x - view.pan.x) * ratio,
      y: focus.y - (focus.y - view.pan.y) * ratio,
    },
  };
}

/**
 * A wheel notch → a zoom factor. Line- and page-mode wheels report much smaller
 * numbers than pixel-mode ones, so they are scaled up before the curve is applied.
 */
export function wheelZoomFactor(deltaY: number, deltaMode = 0): number {
  const pixels = deltaY * (deltaMode === 1 ? 16 : deltaMode === 2 ? 400 : 1);
  return Math.exp(-pixels * 0.0015);
}

export interface ClientRect {
  left: number;
  top: number;
  width: number;
  height: number;
}

/**
 * Client pixels → viewBox units, honouring `preserveAspectRatio="xMidYMid meet"`:
 * the box is scaled to fit and centred, so both the scale and the letterboxing
 * offsets have to be undone.
 */
export function viewBoxScale(rect: ClientRect, viewBox: ViewBox): number {
  if (rect.width <= 0 || rect.height <= 0) return 1;
  return Math.min(rect.width / viewBox.width, rect.height / viewBox.height);
}

export function clientToViewBox(client: Point, rect: ClientRect, viewBox: ViewBox): Point {
  const scale = viewBoxScale(rect, viewBox);
  const offsetX = (rect.width - viewBox.width * scale) / 2;
  const offsetY = (rect.height - viewBox.height * scale) / 2;
  return {
    x: viewBox.minX + (client.x - rect.left - offsetX) / scale,
    y: viewBox.minY + (client.y - rect.top - offsetY) / scale,
  };
}

/** A drag of `delta` client pixels, in viewBox units. */
export function clientDeltaToViewBox(delta: Point, rect: ClientRect, viewBox: ViewBox): Point {
  const scale = viewBoxScale(rect, viewBox);
  return { x: delta.x / scale, y: delta.y / scale };
}

/** Distance between two active pointers; the pinch gesture's whole input. */
export function pointerDistance(a: Point, b: Point): number {
  return Math.hypot(a.x - b.x, a.y - b.y);
}
