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

const trace = (attackId: string): AttackTrace => ({
  attackId,
  from: "t1",
  to: "t2",
  attackerLosses: 1,
  defenderLosses: 0,
  captured: false,
});

describe("suppressing the throw that was already history at mount", () => {
  it("never replays the throw that had already resolved when the screen opened", () => {
    expect(traceToDraw(trace("atk-1"), "atk-1")).toBeNull();
    // Not just on the first update: any later projection transaction still leaves
    // that throw history, which is exactly the case an offset comparison missed.
    expect(traceToDraw(trace("atk-1"), "atk-1")).toBeNull();
  });

  it("draws the next throw, which did happen in front of the viewer", () => {
    expect(traceToDraw(trace("atk-2"), "atk-1")).toEqual(trace("atk-2"));
  });

  it("draws the first throw of a game joined before anyone attacked", () => {
    expect(traceToDraw(trace("atk-1"), null)).toEqual(trace("atk-1"));
    expect(traceToDraw(null, null)).toBeNull();
  });
});
