/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
import { afterEach, describe, expect, it } from "vitest";
import { StreamAddress } from "@streamsy/core";
import { CheckpointStore, Projection } from "@streamsy/projection";
import { BoardState, boardProjection, materializeBoard } from "../../server/game/board.ts";
import { memoryStreams, type GameStreams } from "../../server/game/streams.ts";
import { appendRecordedEvents, readBoardRows } from "../../test/stream-fixtures.ts";
import full from "../../../hex-board/test/fixtures/full.json";
import long from "../../../hex-board/test/fixtures/long.json";
import { foldAggregate } from "../domain/aggregate.ts";
import { GameEvent } from "../domain/events.ts";
import { Effect, Option, Schema } from "effect";
import {
  ProjectionIntegrityError,
  aggregateBoardView,
  boardsEqual,
  initialProjection,
  projectEvent,
  projectionBoardView,
  type ProjectionState,
} from "./projection.ts";
import { boardProjectionTxId } from "./transaction.ts";
import {
  armForAttack,
  declareAttack,
  nextCommandId,
  occupyPending,
  startGame,
  throwUntilCapture,
  winThrow,
  type ScriptedGame,
} from "../../test/testkit.ts";

const params = { gameId: "game", generation: "board1" };
const runtimes: GameStreams[] = [];
afterEach(async () => {
  for (const runtime of runtimes.splice(0)) await runtime.dispose();
});
function newRuntime() {
  const runtime = memoryStreams();
  runtimes.push(runtime);
  return runtime;
}

/** Any owned country with a spare army and an enemy neighbour, or null. */
function findAttack(game: ScriptedGame) {
  const state = game.state();
  const active = state.activePlayerId!;
  for (const territory of Object.values(state.territories).toSorted((a, b) =>
    a.id.localeCompare(b.id),
  )) {
    if (territory.ownerId !== active || territory.armies < 2) continue;
    const to = state
      .index!.territoryById.get(territory.id)!
      .adjacentTerritoryIds.find((adj) => state.territories[adj]!.ownerId !== active);
    if (to) {
      return {
        from: territory.id,
        to,
        attackerId: active,
        defenderId: state.territories[to]!.ownerId!,
      };
    }
  }
  return null;
}

/**
 * Reinforce → attack (always winning) → occupy → skip fortifications, for up to `maxSteps`
 * decisions. Whoever is active plays, so both seats take real turns.
 */
function drive(game: ScriptedGame, maxSteps: number): readonly GameEvent[] {
  for (let step = 0; step < maxSteps; step += 1) {
    const state = game.state();
    if (state.status !== "playing") return game.log;
    if (state.pendingInteraction?.type === "occupation") {
      occupyPending(game);
      continue;
    }
    if (state.phase === "reinforce") {
      armForAttack(game);
      continue;
    }
    const setup = findAttack(game);
    if (setup) {
      winThrow(game, setup);
      continue;
    }
    game.must({
      type: "skip-fortifications",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: state.activePlayerId!,
    });
  }
  return game.log;
}

/**
 * A game that runs all the way to `GameWon`.
 *
 * Rigged dice alone do not converge — both seats sweep their own throws, so
 * ownership just oscillates — so the canonical allocation is rewritten to leave
 * the second player a two-country holding. The result is still an ordinary
 * `GameStarted`, folded by both reducers exactly as in a real game, and it
 * exercises capture, elimination, and victory in one log.
 */
function playFullGame(mapSeed: string): readonly GameEvent[] {
  const game = startGame({
    players: 2,
    mapSeed,
    board: ({ map, turnOrder }) => {
      const [winner, loser] = turnOrder;
      const ids = map.territories.map((t) => t.id);
      return {
        initialTerritories: ids.map((territoryId, index) => ({
          territoryId,
          ownerId: index >= ids.length - 2 ? loser! : winner!,
          armies: index >= ids.length - 2 ? 1 : 3,
        })),
      };
    },
  });
  const log = drive(game, 400);
  if (game.state().status !== "finished") throw new Error("scripted current game did not finish");
  if (!log.some((event) => event.type === "PlayerEliminated")) {
    throw new Error("scripted current game did not eliminate anyone");
  }
  return log;
}

/** A long, ordinary two-sided game — many turns, captures, and turn resets. */
function playLongGame(mapSeed: string, steps = 220): readonly GameEvent[] {
  return drive(startGame({ players: 3, mapSeed }), steps);
}

describe("Hex Domination board projection reducer", () => {
  it.each([
    ["a game played through to victory", () => playFullGame("projection-equivalence")],
    ["a long three-player game", () => playLongGame("projection-long")],
  ])("agrees with the aggregate fold at every prefix of %s", (_label, build) => {
    const events = build();
    expect(events.length).toBeGreaterThan(8);

    let projection = initialProjection();
    for (let index = 0; index < events.length; index += 1) {
      projection = projectEvent(projection, events[index]!, String(index), index);
      const authoritative = aggregateBoardView(foldAggregate(events.slice(0, index + 1)));
      const projected = projectionBoardView(projection);
      if (!boardsEqual(projected, authoritative)) {
        // Surface the offending prefix rather than a bare `false`.
        expect({ index, kind: events[index]!.type, projected }).toEqual({
          index,
          kind: events[index]!.type,
          projected: authoritative,
        });
      }
    }
  });

  it("carries the pending defence interrupt, including the recorded attacker dice", () => {
    const game = startGame({ players: 2, mapSeed: "pending-defence" });
    const setup = armForAttack(game);
    game.rig([6, 6, 6]);
    const attackId = declareAttack(game, setup);

    const projection = replay(game.log);
    const view = projectionBoardView(projection);
    expect(view.pending?.type).toBe("defense");
    expect(boardsEqual(view, aggregateBoardView(game.state()))).toBe(true);

    expect(projection.combat).toMatchObject({
      attackId,
      status: "awaiting-defense",
      attackerRolls: [6, 6, 6],
    });
    expect(projection.turn?.attacksDeclared).toBe(1);
  });

  it("carries the pending occupation bounds and clears combat once occupied", () => {
    const game = startGame({ players: 2, mapSeed: "pending-occupation" });
    const setup = armForAttack(game);
    const pending = throwUntilCapture(game, setup);

    const captured = replay(game.log);
    expect(captured.combat).toMatchObject({
      status: "awaiting-occupation",
      minArmies: pending.minArmies,
      maxArmies: pending.maxArmies,
      territoryCaptured: true,
    });
    expect(boardsEqual(projectionBoardView(captured), aggregateBoardView(game.state()))).toBe(true);

    occupyPending(game, pending.maxArmies);
    const occupied = replay(game.log);
    expect(occupied.combat).toBeNull();
    expect(occupied.turn?.captures).toBe(1);
    expect(occupied.territories.find((t) => t.id === pending.to)!.ownerId).toBe(pending.playerId);
    expect(boardsEqual(projectionBoardView(occupied), aggregateBoardView(game.state()))).toBe(true);
  });

  it("projects the canonical map snapshot verbatim and derives nothing about geometry", () => {
    const game = startGame({ players: 3, mapSeed: "map-rows" });
    const started = game.log.find((event) => event.type === "GameStarted");
    if (started?.type !== "GameStarted") throw new Error("no GameStarted");
    const projection = replay(game.log);

    expect(projection.hexes.map((h) => h.id)).toEqual(started.map.tiles.map((t) => t.id));
    expect(projection.territories.map((t) => t.adjacentTerritoryIds)).toEqual(
      started.map.territories.map((t) => [...t.adjacentTerritoryIds]),
    );
    expect(projection.continents.map((c) => c.territoryIds)).toEqual(
      started.map.continents.map((c) => [...c.territoryIds]),
    );
    expect(projection.territories.map((t) => t.labelAnchor)).toEqual(
      started.map.territories.map((t) => ({ ...t.labelAnchor })),
    );
  });

  it("tracks the current-turn reinforcement breakdown and resets it on TurnEnded", () => {
    const game = startGame({ players: 2, mapSeed: "turn-row" });
    const before = replay(game.log);
    const first = before.turn!;
    expect(first.reinforcement.total).toBe(first.reinforcement.base + bonusSum(first));
    expect(first.reinforcementsPlaced).toBe(0);
    expect(first.reinforcement.remaining).toBe(first.reinforcement.total);

    armForAttack(game);
    const placed = replay(game.log).turn!;
    expect(placed.reinforcementsPlaced).toBe(first.reinforcement.total);
    expect(placed.reinforcement.remaining).toBe(0);
    expect(placed.phase).toBe("attack");

    game.must({
      type: "skip-fortifications",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: game.state().activePlayerId!,
    });
    const next = replay(game.log).turn!;
    expect(next.turnId).not.toBe(first.turnId);
    expect(next.reinforcementsPlaced).toBe(0);
    expect(next.attacksDeclared).toBe(0);
  });

  it("fails loudly when an AttackResolved contradicts its declaration", () => {
    const game = startGame({ players: 2, mapSeed: "integrity" });
    const setup = armForAttack(game);
    game.rig([6, 6, 6]);
    declareAttack(game, setup);
    game.rig([1, 1]);
    game.must({
      type: "roll-defense",
      commandId: nextCommandId(),
      turnId: game.turnId(),
      playerId: setup.defenderId,
      attackId: game.log.findLast((e) => e.type === "AttackDeclared")!.attackId,
    });

    const events = game.log.slice();
    const resolvedIndex = events.findIndex((event) => event.type === "AttackResolved");
    const resolved = events[resolvedIndex]!;
    if (resolved.type !== "AttackResolved") throw new Error("unreachable");
    // Rewrite the repeated attacker rolls: the projection must not fold this.
    events[resolvedIndex] = { ...resolved, attackerRolls: [1, 1, 1] };

    expect(() => replay(events)).toThrow(ProjectionIntegrityError);
  });
});

function bonusSum(turn: { reinforcement: { continents: Array<{ bonus: number }> } }): number {
  return turn.reinforcement.continents.reduce((sum, c) => sum + c.bonus, 0);
}

function replay(events: readonly GameEvent[]): ProjectionState {
  let state = initialProjection("game");
  for (let index = 0; index < events.length; index += 1) {
    state = projectEvent(state, events[index]!, String(index), index);
  }
  return state;
}

describe("Hex Domination board projection materializer", () => {
  it.each([
    ["historical victory", full],
    ["historical long game", long],
  ])(
    "replays the %s in chunks, resumes the checkpoint, and rebuilds with the same watermark",
    async (label, raw) => {
      const streams = newRuntime();
      const events = Schema.decodeUnknownSync(Schema.Array(GameEvent))(raw);
      // Split only at command boundaries. A command is the current canonical item.
      let cut = Math.floor(events.length / 2);
      while (events[cut - 1]?.commandId === events[cut]?.commandId) cut += 1;
      const midpoint = await appendRecordedEvents(streams, params.gameId, events.slice(0, cut));
      const before = await materializeBoard(streams, params.gameId);
      expect(before.sourceThroughOffset).toBe(midpoint);
      const head = await appendRecordedEvents(streams, params.gameId, events.slice(cut));
      const after = await materializeBoard(streams, params.gameId);
      expect(after.sourceThroughOffset).toBe(head);
      expect(projectionBoardView(after.state)).toEqual(aggregateBoardView(foldAggregate(events)));
      if (label === "historical victory") expect(after.state.game.status).toBe("finished");
      expect((await materializeBoard(streams, params.gameId, "board2")).state).toEqual(after.state);
      const binding = boardProjection.at(params);
      expect((await streams.runPromise(Projection.run(binding))).items).toBe(0);
      const checkpoint = await streams.runPromise(Projection.read(binding));
      expect(checkpoint.state.ordinal).toBe(events.length);
      expect(checkpoint.offsets.events).toBe(head);
    },
  );

  it("puts a single receipt after each command's rows, even for a multi-command backlog", async () => {
    const streams = newRuntime();
    const events = playFullGame("one-transaction");
    await appendRecordedEvents(streams, params.gameId, events);
    const { state } = await materializeBoard(streams, params.gameId);
    const { items } = await readBoardRows(streams, params.gameId);
    const moves = new Map<string, (typeof state.moves)[number]>();
    const seen = new Map<string, string[]>();
    for (const row of items) {
      if (!StreamAddress.isChange(row)) continue;
      if (row.type === "move") {
        if (StreamAddress.isUpsert(row)) moves.set(row.key, row.value);
        else moves.delete(row.key);
      }
      if (row.type !== "projectionMeta" || !StreamAddress.isUpsert(row)) {
        expect(row.headers.txid ?? "").not.toMatch(/^risk-board:/);
        continue;
      }
      const lastMove = row.value.snapshot.moves.at(-1)!;
      expect(row.headers.txid).toBe(boardProjectionTxId(lastMove.commandId));
      expect(seen.has(lastMove.commandId)).toBe(false);
      const ids = row.value.snapshot.moves
        .filter((move) => move.commandId === lastMove.commandId)
        .map((move) => move.id);
      expect(ids.every((key) => moves.has(key))).toBe(true);
      seen.set(lastMove.commandId, ids);
    }
    const victim = [...seen.values()].find((ids) => ids.length > 1);
    expect(victim).toBeDefined();
    expect(new Set(victim).size).toBe(victim!.length);
    expect([...seen.keys()]).toEqual([...new Set(events.map((event) => event.commandId))]);
    expect([...moves.values()]).toEqual(state.moves);
  });

  it.each([false, true])(
    "refuses a checkpoint whose state watermark was altered (unread input: %s)",
    async (unread) => {
      const streams = newRuntime();
      const events = Schema.decodeUnknownSync(Schema.Array(GameEvent))(full);
      const cut = events.findIndex((event) => event.type === "AttackResolved");
      await appendRecordedEvents(streams, params.gameId, events.slice(0, cut));
      await materializeBoard(streams, params.gameId);
      const binding = boardProjection.at(params);
      const before = await readBoardRows(streams, params.gameId);
      const checkpoint = await streams.runPromise(Projection.read(binding));
      await streams.runPromise(
        Effect.gen(function* () {
          const store = yield* CheckpointStore;
          const loaded = yield* store.load(binding);
          const record = Option.getOrThrow(loaded.record);
          const state = {
            ...checkpoint.state,
            board: { ...checkpoint.state.board, sourceThroughOffset: null },
          };
          const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(BoardState))(state);
          // oxlint-disable-next-line typescript/no-misused-spread -- CheckpointRecord is a plain Schema.Struct value.
          yield* store.compareAndSet(binding, loaded.token, { ...record, state: encoded });
        }),
      );
      if (unread) await appendRecordedEvents(streams, params.gameId, events.slice(cut));
      await expect(materializeBoard(streams, params.gameId)).rejects.toThrow(
        /checkpoint disagrees/i,
      );
      expect(await readBoardRows(streams, params.gameId)).toEqual(before);
    },
  );
});
