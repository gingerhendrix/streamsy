/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { createStreamDB } from "@durable-streams/state/db";
import { StreamAddress } from "@streamsy/core";
import { memoryStreams, BoardStream } from "../../server/game/streams.ts";
import { materializeBoard } from "../../server/game/board.ts";
import { appendRecordedEvents, readBoardRows } from "../../test/stream-fixtures.ts";
import { startGame } from "../../test/testkit.ts";

import {
  boardRowsFromQueries,
  createRiskBoardSession,
  riskBoardState,
  type CreateRiskBoardSessionOptions,
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
    const preload = vi.fn(async () => undefined);
    const awaitTxId = vi.fn(async () => undefined);
    const db = createStreamDB({
      streamOptions: { url: "https://risk.test/streams/board" },
      state: riskBoardState,
    });
    Object.defineProperty(db, "offset", { value: "42" });
    db.preload = preload;
    db.utils.awaitTxId = awaitTxId;
    type CreateDb = NonNullable<CreateRiskBoardSessionOptions["createDb"]>;
    let receivedOptions: Parameters<CreateDb>[0] | undefined;
    const createDb: CreateDb = (options) => {
      receivedOptions = options;
      return db;
    };

    const session = createRiskBoardSession({ streamId: "games/g1/board/board1", createDb });
    await session.preload();
    await session.awaitTxId("risk-board:cmd:42", 1234);

    expect(session.collections).toBe(db.collections);
    expect(session.offset).toBe("42");
    expect(preload).toHaveBeenCalledOnce();
    expect(awaitTxId).toHaveBeenCalledWith("risk-board:cmd:42", 1234);
    expect(receivedOptions).toMatchObject({
      streamOptions: { url: "https://risk.test/streams/games/g1/board/board1" },
      live: "long-poll",
      state: riskBoardState,
    });
  });

  it("closes the owned StreamDB exactly once across concurrent and later calls", async () => {
    vi.stubGlobal("window", { location: { origin: "https://risk.test" } });
    const close = vi.fn();
    const db = createStreamDB({
      streamOptions: { url: "https://risk.test/streams/board" },
      state: riskBoardState,
    });
    db.close = close;
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

it("registers every declared projection collection and accepts every emitted row", async () => {
  const streams = memoryStreams();
  try {
    const gameId = "game";
    const game = startGame({ players: 2, mapSeed: "mirror-coverage" });
    await appendRecordedEvents(streams, gameId, game.log);
    await materializeBoard(streams, gameId);
    const types = Object.values(riskBoardState)
      .map((definition) => definition.type)
      .toSorted();
    expect(types).toEqual(Object.keys(BoardStream.collections).toSorted());
    const { items } = await readBoardRows(streams, gameId);
    expect(items.length).toBeGreaterThan(0);
    for (const item of items) {
      expect(StreamAddress.isChange(item)).toBe(true);
      if (StreamAddress.isChange(item)) expect(types).toContain(item.type);
    }
  } finally {
    await streams.dispose();
  }
});
