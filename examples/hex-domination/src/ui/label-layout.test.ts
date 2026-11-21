import { describe, expect, it } from "vitest";

import { layoutCountryLabels, measureLabelWidth, type LabelRequest } from "./label-layout.ts";

const OPTIONS = { badgeRadius: 16, baseOffset: 30, fontSize: 10.5 };

interface Box {
  left: number;
  right: number;
  top: number;
  bottom: number;
}

const boxOf = (placement: { x: number; y: number; width: number; height: number }): Box => ({
  left: placement.x - placement.width / 2,
  right: placement.x + placement.width / 2,
  top: placement.y - placement.height / 2,
  bottom: placement.y + placement.height / 2,
});

const overlaps = (a: Box, b: Box): boolean =>
  a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;

const hitsBadge = (box: Box, anchor: { x: number; y: number }, radius: number): boolean => {
  const nearestX = Math.min(Math.max(anchor.x, box.left), box.right);
  const nearestY = Math.min(Math.max(anchor.y, box.top), box.bottom);
  return Math.hypot(anchor.x - nearestX, anchor.y - nearestY) < radius;
};

describe("label measurement", () => {
  it("scales with the name and never collapses to nothing", () => {
    expect(measureLabelWidth("Cinderhold", 10.5)).toBeGreaterThan(
      measureLabelWidth("Karrow", 10.5),
    );
    expect(measureLabelWidth("", 10.5)).toBe(10.5);
  });
});

describe("country label layout", () => {
  it("leaves a lone label directly above its country", () => {
    const [placement] = layoutCountryLabels(
      [{ id: "t1", anchor: { x: 100, y: 100 }, text: "Karrow" }],
      OPTIONS,
    );
    expect(placement).toMatchObject({ id: "t1", x: 100, y: 70, leader: false });
  });

  it("separates the neighbours that used to render as one word", () => {
    // The exact failure from the browser review: two anchors close enough that
    // "Cinderhold" and "Windbarrow" ran together as CINDERWINDBARROW.
    const requests: LabelRequest[] = [
      { id: "cinderhold", anchor: { x: 200, y: 200 }, text: "Cinderhold" },
      { id: "windbarrow", anchor: { x: 236, y: 206 }, text: "Windbarrow" },
    ];
    const placements = layoutCountryLabels(requests, OPTIONS);
    expect(overlaps(boxOf(placements[0]!), boxOf(placements[1]!))).toBe(false);
  });

  it("keeps every label clear of every army badge", () => {
    // A dense cluster: six countries within a couple of hexes of each other.
    const requests: LabelRequest[] = [
      { id: "a", anchor: { x: 0, y: 0 }, text: "Graymarch" },
      { id: "b", anchor: { x: 34, y: 8 }, text: "Farrowdale" },
      { id: "c", anchor: { x: 18, y: 40 }, text: "Coldharbour" },
      { id: "d", anchor: { x: -20, y: 34 }, text: "Ashfell" },
      { id: "e", anchor: { x: 60, y: 44 }, text: "Windbarrow" },
      { id: "f", anchor: { x: -46, y: 6 }, text: "Cinderhold" },
    ];
    const placements = layoutCountryLabels(requests, OPTIONS);

    for (const placement of placements) {
      for (const request of requests) {
        expect(hitsBadge(boxOf(placement), request.anchor, OPTIONS.badgeRadius)).toBe(false);
      }
    }
    for (let i = 0; i < placements.length; i += 1) {
      for (let j = i + 1; j < placements.length; j += 1) {
        expect(overlaps(boxOf(placements[i]!), boxOf(placements[j]!))).toBe(false);
      }
    }
  });

  it("marks a displaced label so the renderer can draw a leader line", () => {
    // Eight long names inside two hexes' worth of space: something has to travel.
    const placements = layoutCountryLabels(
      Array.from({ length: 8 }, (_, index) => ({
        id: `t${index}`,
        anchor: { x: (index % 3) * 14, y: Math.floor(index / 3) * 12 },
        text: "Coldharbour",
      })),
      OPTIONS,
    );
    expect(placements.some((placement) => placement.leader)).toBe(true);
    expect(placements.every((placement) => placement.leader)).toBe(false);
  });

  it("is deterministic and returns placements in the caller's order", () => {
    const requests: LabelRequest[] = [
      { id: "c", anchor: { x: 18, y: 40 }, text: "Coldharbour" },
      { id: "a", anchor: { x: 0, y: 0 }, text: "Graymarch" },
      { id: "b", anchor: { x: 34, y: 8 }, text: "Farrowdale" },
    ];
    const first = layoutCountryLabels(requests, OPTIONS);
    const second = layoutCountryLabels(requests, OPTIONS);
    expect(first).toEqual(second);
    expect(first.map((placement) => placement.id)).toEqual(["c", "a", "b"]);
  });
});
