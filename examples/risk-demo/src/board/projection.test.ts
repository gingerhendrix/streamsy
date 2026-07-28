import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";

import { foldAggregate } from "../domain/aggregate.ts";
import type { GameEvent } from "../domain/events.ts";
import {
  ProjectionIntegrityError,
  aggregateBoardView,
  boardsEqual,
  initialProjection,
  projectEvent,
  projectionBoardView,
  type ProjectionState,
} from "./projection.ts";
import { createBoardProjectionAdapter, writeCanonicalEvents } from "./board-projection.ts";
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

const SOURCE = "games/game/events";
const OUTPUT = "games/game/projections/board/board1";

function newProtocol(): StreamProtocolFactory {
  return createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
}

function adapterFor(generation = "board1") {
  return createBoardProjectionAdapter({
    gameId: "game",
    sourceStreamId: SOURCE,
    outputStreamId: `games/game/projections/board/${generation}`,
    generation,
  });
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
      projection = projectEvent(projection, events[index]!, String(index));
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
    state = projectEvent(state, events[index]!, String(index));
  }
  return state;
}

describe("Hex Domination board projection materializer", () => {
  it("materializes a full game so the projection equals the aggregate at the source head", async () => {
    const events = playFullGame("materialize-full");
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);

    const runtime = new ProjectionRuntime({ protocol, adapter: adapterFor() });
    const { status } = await runtime.catchUp();

    expect(status.caughtUp).toBe(true);
    expect(status.sourceSeq).toBe(events.length - 1);
    expect(
      boardsEqual(
        projectionBoardView(runtime.currentState()),
        aggregateBoardView(foldAggregate(events)),
      ),
    ).toBe(true);
    expect(runtime.currentState().game.status).toBe("finished");
    expect(runtime.currentState().combat).toBeNull();
  });

  it("marks each transition with its command and exact source position", async () => {
    const events = playFullGame("materialize-txid").slice(0, 6);
    const protocol = newProtocol();
    const offsets = await writeCanonicalEvents(protocol, SOURCE, events);
    await new ProjectionRuntime({ protocol, adapter: adapterFor() }).catchUp();

    const output = await protocol.get(OUTPUT);
    if (output.status !== "ok") throw new Error("no projection stream");
    const read = await output.stream.read({});
    if (read.status !== "ok") throw new Error("cannot read projection stream");
    const messages = read.messages.map(
      (message) =>
        JSON.parse(new TextDecoder().decode(message.data)) as { headers: { txid?: string } },
    );
    expect(messages.at(-1)?.headers.txid).toBe(
      boardProjectionTxId(events.at(-1)!.commandId, offsets.at(-1)!),
    );
  });

  it("rebuilds a byte-identical generation, including combat and watermark", async () => {
    // Stop mid-combat so the rebuild has a pending interrupt to reproduce.
    const game = startGame({ players: 2, mapSeed: "rebuild-pending" });
    const setup = armForAttack(game);
    game.rig([6, 6, 6]);
    declareAttack(game, setup);

    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, game.log);

    const first = new ProjectionRuntime({ protocol, adapter: adapterFor("board1") });
    await first.catchUp();
    const rebuilt = new ProjectionRuntime({ protocol, adapter: adapterFor("board2") });
    await rebuilt.catchUp();

    expect(rebuilt.currentState()).toEqual(first.currentState());
    expect(rebuilt.currentState().sourceThroughOffset).toBe(
      first.currentState().sourceThroughOffset,
    );
    expect(rebuilt.currentState().combat?.status).toBe("awaiting-defense");
  });

  it("catches up incrementally as canonical events arrive, gap-free", async () => {
    const events = playFullGame("materialize-incremental");
    const protocol = newProtocol();
    const runtime = new ProjectionRuntime({ protocol, adapter: adapterFor() });

    const midpoint = Math.floor(events.length / 2);
    await writeCanonicalEvents(protocol, SOURCE, events.slice(0, midpoint));
    const first = await runtime.catchUp();
    expect(first.applied).toBe(midpoint);
    // The mid-game snapshot is itself a valid board.
    expect(
      boardsEqual(
        projectionBoardView(runtime.currentState()),
        aggregateBoardView(foldAggregate(events.slice(0, midpoint))),
      ),
    ).toBe(true);

    await writeCanonicalEvents(protocol, SOURCE, events.slice(midpoint));
    const second = await runtime.catchUp();
    expect(second.applied).toBe(events.length - midpoint);
    expect(
      boardsEqual(
        projectionBoardView(runtime.currentState()),
        aggregateBoardView(foldAggregate(events)),
      ),
    ).toBe(true);
  });

  it("recovers the board and watermark from the projection stream alone", async () => {
    const events = playFullGame("materialize-reload");
    const protocol = newProtocol();
    await writeCanonicalEvents(protocol, SOURCE, events);
    await new ProjectionRuntime({ protocol, adapter: adapterFor() }).catchUp();

    const reloaded = new ProjectionRuntime({ protocol, adapter: adapterFor() });
    await reloaded.load();
    expect(
      boardsEqual(
        projectionBoardView(reloaded.currentState()),
        aggregateBoardView(foldAggregate(events)),
      ),
    ).toBe(true);
    expect(reloaded.currentState().sourceThroughOffset).not.toBeNull();
  });
});
