import { describe, expect, it } from "vitest";

import type { ProjectedMove } from "../board/projection.ts";
import { latestAttackTrace } from "./attack-trace.ts";

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
