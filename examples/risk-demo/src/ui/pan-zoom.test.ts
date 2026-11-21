import { describe, expect, it } from "vitest";

import {
  ZOOM_LIMITS,
  clampZoom,
  clientDeltaToViewBox,
  clientToViewBox,
  panBy,
  pointerDistance,
  wheelZoomFactor,
  zoomAbout,
} from "./pan-zoom.ts";

const VIEW_BOX = { minX: -100, minY: -50, width: 400, height: 200 };
const RECT = { left: 20, top: 10, width: 800, height: 400 };

describe("zoom", () => {
  it("stays inside the same range the buttons offer", () => {
    expect(clampZoom(9)).toBe(ZOOM_LIMITS.max);
    expect(clampZoom(0.1)).toBe(ZOOM_LIMITS.min);
    expect(clampZoom(1.4)).toBe(1.4);
  });

  it("keeps the point under the cursor under the cursor", () => {
    const view = { zoom: 1, pan: { x: 0, y: 0 } };
    const focus = { x: 120, y: -30 };
    const zoomed = zoomAbout(view, focus, 1.5);

    const contentBefore = {
      x: (focus.x - view.pan.x) / view.zoom,
      y: (focus.y - view.pan.y) / view.zoom,
    };
    const screenAfter = {
      x: zoomed.pan.x + zoomed.zoom * contentBefore.x,
      y: zoomed.pan.y + zoomed.zoom * contentBefore.y,
    };
    expect(screenAfter.x).toBeCloseTo(focus.x, 6);
    expect(screenAfter.y).toBeCloseTo(focus.y, 6);
    expect(zoomed.zoom).toBe(1.5);
  });

  it("does not move the map when it is already at a limit", () => {
    const view = { zoom: ZOOM_LIMITS.max, pan: { x: 12, y: -4 } };
    expect(zoomAbout(view, { x: 50, y: 50 }, 1.4)).toBe(view);
  });

  it("reads a wheel notch in whichever unit the browser reports", () => {
    expect(wheelZoomFactor(-100)).toBeGreaterThan(1); // scroll up zooms in
    expect(wheelZoomFactor(100)).toBeLessThan(1);
    // Three lines and 48 pixels are the same gesture.
    expect(wheelZoomFactor(3, 1)).toBeCloseTo(wheelZoomFactor(48, 0), 10);
  });
});

describe("pan", () => {
  it("shifts by a delta already in viewBox units", () => {
    expect(panBy({ zoom: 2, pan: { x: 5, y: 5 } }, { x: -3, y: 7 })).toEqual({
      zoom: 2,
      pan: { x: 2, y: 12 },
    });
  });

  it("converts client pixels through the letterboxed viewBox", () => {
    // 800×400 client for a 400×200 box: a clean 2× fit with no letterboxing.
    expect(clientToViewBox({ x: 20, y: 10 }, RECT, VIEW_BOX)).toEqual({ x: -100, y: -50 });
    expect(clientToViewBox({ x: 420, y: 210 }, RECT, VIEW_BOX)).toEqual({ x: 100, y: 50 });
    expect(clientDeltaToViewBox({ x: 80, y: 40 }, RECT, VIEW_BOX)).toEqual({ x: 40, y: 20 });
  });

  it("centres the box when the client rect has a different aspect ratio", () => {
    // 800×800 client, 400×200 box: scale 2, and 200px of letterbox top and bottom.
    const square = { left: 0, top: 0, width: 800, height: 800 };
    expect(clientToViewBox({ x: 0, y: 200 }, square, VIEW_BOX)).toEqual({ x: -100, y: -50 });
  });
});

describe("pinch", () => {
  it("measures the gap between two pointers", () => {
    expect(pointerDistance({ x: 0, y: 0 }, { x: 3, y: 4 })).toBe(5);
  });
});
