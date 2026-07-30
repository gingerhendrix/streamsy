import { describe, expect, it } from "vitest";

import type { ProjectedMove } from "../board/projection.ts";
import { latestAttackTrace, traceToDraw, type AttackTrace } from "./attack-trace.ts";

function resolved(offset: string, detail: Partial<ProjectedMove>): ProjectedMove {
  return {
    id: offset,
    commandId: `c-${offset}`,
    kind: "AttackResolved",
    playerId: "p2",
    sourceOffset: offset,
    attackId: `atk-${offset}`,
    from: "t1",
    to: "t2",
    attackerLosses: 1,
    defenderLosses: 1,
    territoryCaptured: false,
    ...detail,
  };
}

const DECLARED: ProjectedMove = {
  id: "0005",
  commandId: "atk-0006",
  kind: "AttackDeclared",
  playerId: "p1",
  sourceOffset: "0005",
  attackId: "atk-0006",
  from: "t1",
  to: "t2",
};

describe("latest attack trace", () => {
  it("takes the newest throw whichever way round the feed is held", () => {
    const older = resolved("0006", { attackerLosses: 2, defenderLosses: 0 });
    const newer = resolved("0011", { attackerLosses: 0, defenderLosses: 2, to: "t3" });
    // The UI holds the feed newest-first; the projection builds it oldest-first.
    for (const feed of [
      [newer, older, DECLARED],
      [DECLARED, older, newer],
    ]) {
      expect(latestAttackTrace(feed)).toEqual({
        attackId: "atk-0011",
        from: "t1",
        to: "t3",
        attackerLosses: 0,
        defenderLosses: 2,
        captured: false,
        sourceOffset: "0011",
      });
    }
  });

  it("marks a capture so the map can draw it differently from a bounce", () => {
    expect(latestAttackTrace([resolved("0007", { territoryCaptured: true })])?.captured).toBe(true);
    expect(latestAttackTrace([resolved("0007", {})])?.captured).toBe(false);
  });

  it("has nothing to draw before the first throw", () => {
    expect(latestAttackTrace([])).toBeNull();
    // A declaration is not a result: the losses are not known until it resolves.
    expect(latestAttackTrace([DECLARED])).toBeNull();
  });

  it("skips a row that does not name its route rather than guessing at one", () => {
    expect(latestAttackTrace([resolved("0008", { to: undefined })])).toBeNull();
    expect(latestAttackTrace([resolved("0008", { attackId: undefined })])).toBeNull();
  });

  it("reads absent losses as none, never as unknown", () => {
    expect(
      latestAttackTrace([resolved("0009", { attackerLosses: undefined, defenderLosses: 2 })]),
    ).toMatchObject({ attackerLosses: 0, defenderLosses: 2 });
  });
});

const trace = (offset: string): AttackTrace => ({
  attackId: `atk-${offset}`,
  from: "t1",
  to: "t2",
  attackerLosses: 1,
  defenderLosses: 0,
  captured: false,
  sourceOffset: offset,
});

/**
 * One page load, as the map actually sees it: a sequence of move feeds, each render's
 * trace judged against the watermark canonical history stood at when the screen
 * opened. Returns every throw that would have been drawn.
 *
 * The sequence is the point. A single feed cannot express this defect — the flash
 * needs a *first* state that is coherent and stale followed by a catch-up, which is
 * exactly what the projection stream's cacheable first response produces.
 */
function drawnDuringLoad(
  openedThroughOffset: string | null | undefined,
  feeds: readonly ProjectedMove[][],
): string[] {
  const drawn: string[] = [];
  for (const feed of feeds) {
    const visible = traceToDraw(latestAttackTrace(feed), openedThroughOffset);
    if (visible && drawn.at(-1) !== visible.attackId) drawn.push(visible.attackId);
  }
  return drawn;
}

describe("suppressing throws that were already history when the screen opened", () => {
  // A reload whose first state comes from the stream's `max-age=60` cached response:
  // the browser hydrates a feed that is internally coherent — its own watermark, meta
  // row and move rows all agree — but a transaction or two behind canonical history,
  // and everything committed since arrives immediately afterwards as ordinary changes.
  const CACHED = [resolved("0006", {})];
  const CAUGHT_UP = [resolved("0006", {}), resolved("0020", {})];

  it("draws nothing when a stale cached feed is followed by a catch-up", () => {
    // `/board` is uncacheable, so the watermark knows about atk-0020 even though the
    // cached hydration did not. Neither throw is news, so neither may be drawn.
    expect(drawnDuringLoad("0020", [[], CACHED, CACHED, CAUGHT_UP])).toEqual([]);
  });

  it("suppresses every throw already in history, not only the newest one", () => {
    expect(drawnDuringLoad("0020", [CAUGHT_UP])).toEqual([]);
    expect(traceToDraw(trace("0006"), "0020")).toBeNull();
  });

  it("still draws a genuinely new throw after that same load", () => {
    // The positive control, and the reason this is a comparison rather than a latch:
    // suppression must not become a blanket, or the overlay is silently deleted.
    const NEW_THROW = [...CAUGHT_UP, resolved("0031", {})];
    expect(drawnDuringLoad("0020", [CACHED, CAUGHT_UP, NEW_THROW])).toEqual(["atk-0031"]);
  });

  it("counts a throw at the watermark itself as history", () => {
    // The watermark is inclusive: it is the offset history has been read *through*.
    expect(traceToDraw(trace("0020"), "0020")).toBeNull();
    expect(traceToDraw(trace("0021"), "0020")).toEqual(trace("0021"));
  });

  it("draws the first throw of a game opened before anyone attacked", () => {
    expect(traceToDraw(trace("0006"), null)).toEqual(trace("0006"));
    expect(traceToDraw(null, null)).toBeNull();
  });

  it("draws nothing at all until the watermark is known", () => {
    // In flight, or unreadable: a throw drawn now might be one that resolved before
    // the viewer arrived, and a missed flash costs less than a false one.
    expect(traceToDraw(trace("0006"), undefined)).toBeNull();
    expect(drawnDuringLoad(undefined, [CACHED, CAUGHT_UP])).toEqual([]);
  });

  it("draws a throw that landed while the watermark was still in flight", () => {
    // Not latched, so the throw is not lost: the render after the watermark arrives
    // proves it is above the watermark and draws it.
    expect(drawnDuringLoad(undefined, [CAUGHT_UP])).toEqual([]);
    expect(drawnDuringLoad("0006", [CAUGHT_UP])).toEqual(["atk-0020"]);
  });
});
