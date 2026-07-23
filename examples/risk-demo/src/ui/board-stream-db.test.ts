import { afterEach, describe, expect, it, vi } from "vitest";

import {
  boardRowsFromQueries,
  createRiskBoardSession,
  riskBoardState,
  type RiskBoardDb,
} from "./board-stream-db.ts";

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

afterEach(() => vi.unstubAllGlobals());

describe("Risk board session", () => {
  it("encapsulates typed collections and delegates preload and awaitTxId", async () => {
    vi.stubGlobal("window", { location: { origin: "https://risk.test" } });
    const collections = { games: {}, players: {}, territories: {}, moves: {}, projectionMeta: {} };
    const preload = vi.fn(async () => undefined);
    const awaitTxId = vi.fn(async () => undefined);
    const db = {
      collections,
      offset: "42",
      preload,
      close: vi.fn(),
      utils: { awaitTxId },
    } as unknown as RiskBoardDb;
    const createDb = vi.fn((_options: unknown) => db);

    const session = createRiskBoardSession({ streamId: "games/g1/board/v1", createDb });
    await session.preload();
    await session.awaitTxId("risk-board:cmd:42", 1234);

    expect(session.collections).toBe(collections);
    expect(session.offset).toBe("42");
    expect(preload).toHaveBeenCalledOnce();
    expect(awaitTxId).toHaveBeenCalledWith("risk-board:cmd:42", 1234);
    expect(createDb.mock.calls[0]?.[0]).toMatchObject({
      streamOptions: { url: "https://risk.test/streams/games/g1/board/v1" },
      live: "long-poll",
      state: riskBoardState,
    });
  });

  it("closes the owned StreamDB exactly once across concurrent and later calls", async () => {
    vi.stubGlobal("window", { location: { origin: "https://risk.test" } });
    const close = vi.fn();
    const db = {
      collections: {},
      offset: "-1",
      preload: vi.fn(),
      close,
      utils: { awaitTxId: vi.fn() },
    } as unknown as RiskBoardDb;
    const session = createRiskBoardSession({
      streamId: "board",
      createDb: () => db,
    });

    const first = session.close();
    const concurrent = session.close();
    expect(concurrent).toBe(first);
    await Promise.all([first, concurrent]);
    expect(session.close()).toBe(first);
    expect(close).toHaveBeenCalledOnce();
  });
});
