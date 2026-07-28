import { afterEach, describe, expect, it, vi } from "vitest";

import {
  boardRowsFromQueries,
  createRiskBoardSession,
  riskBoardState,
  type RiskBoardDb,
} from "./board-stream-db.ts";

const REINFORCEMENT = { base: 5, continents: [], total: 5, remaining: 2 };

describe("Hex Domination StreamDB query shaping", () => {
  it("collapses the zero-or-one turn and combat collections to a single row or null", () => {
    const base = {
      games: [{ id: "g2", status: "playing" as const, round: 1 }],
      players: [],
      hexes: [],
      territories: [],
      continents: [],
      moves: [],
      projectionMeta: [],
    };

    const idle = boardRowsFromQueries({ ...base, turn: [], combat: [] });
    expect(idle?.turn).toBeNull();
    expect(idle?.combat).toBeNull();

    const pending = boardRowsFromQueries({
      ...base,
      turn: [
        {
          id: "turn",
          turnId: "round-1:p1",
          round: 1,
          playerId: "p1",
          phase: "attack",
          reinforcement: REINFORCEMENT,
          reinforcementsPlaced: 3,
          attacksDeclared: 1,
          throwsResolved: 0,
          captures: 0,
          eliminations: 0,
        },
      ],
      combat: [
        {
          id: "combat",
          attackId: "atk-1",
          turnId: "round-1:p1",
          status: "awaiting-defense",
          attackerId: "p1",
          defenderId: "p2",
          from: "t:01",
          to: "t:02",
          attackerDice: 3,
          attackerRolls: [6, 5, 2],
          defenderDice: 2,
          declaredAt: 1_000,
          defenseDeadlineAt: 16_000,
        },
      ],
    });
    expect(pending?.turn?.turnId).toBe("round-1:p1");
    expect(pending?.combat?.status).toBe("awaiting-defense");
  });

  it("keys the current combat collection so a cleared interrupt is a delete", () => {
    expect(riskBoardState.combat.delete({ key: "combat" })).toMatchObject({
      type: "combat",
      key: "combat",
      headers: { operation: "delete" },
    });
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

    const session = createRiskBoardSession({ streamId: "games/g1/board/board1", createDb });
    await session.preload();
    await session.awaitTxId("risk-board:cmd:42", 1234);

    expect(session.collections).toBe(collections);
    expect(session.offset).toBe("42");
    expect(preload).toHaveBeenCalledOnce();
    expect(awaitTxId).toHaveBeenCalledWith("risk-board:cmd:42", 1234);
    expect(createDb.mock.calls[0]?.[0]).toMatchObject({
      streamOptions: { url: "https://risk.test/streams/games/g1/board/board1" },
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
