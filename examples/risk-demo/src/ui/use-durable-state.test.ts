import { describe, expect, it } from "vitest";

import { applyBoardChanges, boardRows, emptyBoardCollections } from "./use-durable-state.ts";

describe("live Durable State board decoding", () => {
  it("applies inserts, updates, deletes, and projection metadata", () => {
    const state = applyBoardChanges(emptyBoardCollections(), [
      {
        type: "game",
        key: "g1",
        value: { id: "g1", status: "lobby", round: 0 },
        headers: { operation: "insert", offset: "1" },
      },
      {
        type: "player",
        key: "p1",
        value: { id: "p1", name: "Ada", color: "red", remainingArmies: 0, eliminated: false },
        headers: { operation: "insert", offset: "2" },
      },
      {
        type: "player",
        key: "p1",
        value: { id: "p1", name: "Ada", color: "red", remainingArmies: 3, eliminated: false },
        headers: { operation: "update", offset: "3" },
      },
      {
        type: "projectionMeta",
        key: "board",
        value: { generation: "v1", sourceThroughOffset: "3", snapshot: {} },
        headers: { operation: "update", offset: "3" },
      },
    ]);
    expect(boardRows(state)?.players[0]?.remainingArmies).toBe(3);
    expect(boardRows(state)?.meta?.sourceThroughOffset).toBe("3");

    const deleted = applyBoardChanges(state, [
      { type: "player", key: "p1", headers: { operation: "delete", offset: "4" } },
    ]);
    expect(boardRows(deleted)?.players).toEqual([]);
  });

  it("clears stale rows when a reset or snapshot begins", () => {
    const populated = applyBoardChanges(emptyBoardCollections(), [
      {
        type: "game",
        key: "old",
        value: { id: "old", status: "lobby", round: 0 },
        headers: { operation: "insert" },
      },
    ]);
    expect(boardRows(populated)?.game.id).toBe("old");
    expect(boardRows(applyBoardChanges(populated, [{ headers: { control: "reset" } }]))).toBeNull();
  });
});
