import { describe, expect, it } from "vitest";

import { boardRowsFromQueries, riskBoardState } from "./board-stream-db.ts";

describe("Risk StreamDB query shaping", () => {
  it("shapes typed collection queries and orders the newest moves first", () => {
    const rows = boardRowsFromQueries({
      games: [{ id: "g1", status: "playing", phase: "attack", round: 2 }],
      players: [{ id: "p1", name: "Ada", color: "red", remainingArmies: 0, eliminated: false }],
      territories: [{ id: "alpha", ownerId: "p1", armies: 3 }],
      moves: [
        { id: "1", commandId: "a", kind: "GameCreated", sourceOffset: "1" },
        { id: "2", commandId: "b", kind: "GameStarted", sourceOffset: "2" },
      ],
      projectionMeta: [],
    });

    expect(rows?.game.phase).toBe("attack");
    expect(rows?.moves.map((move) => move.id)).toEqual(["2", "1"]);
    expect(rows?.territories[0]?.armies).toBe(3);
  });

  it("uses the stream event key as the typed collection primary key", () => {
    expect(
      riskBoardState.players.insert({
        key: "p1",
        value: {
          id: "p1",
          name: "Ada",
          color: "red",
          remainingArmies: 0,
          eliminated: false,
        },
      }),
    ).toMatchObject({ type: "player", key: "p1", headers: { operation: "insert" } });
  });
});
