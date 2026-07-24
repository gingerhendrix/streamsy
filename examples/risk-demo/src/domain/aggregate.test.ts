import { describe, expect, it } from "vitest";

import type { AggregateState } from "./aggregate.ts";
import { buildTurnId, foldAggregate } from "./aggregate.ts";
import type { AttackCommand, Command } from "./commands.ts";
import type { GameEvent } from "./events.ts";
import { resolveAttack } from "./dice.ts";
import { RULES, TERRITORIES, TERRITORY_IDS, areAdjacent, reinforcementPool } from "./map.ts";
import {
  aggregateBoardView,
  boardsEqual,
  projectEvents,
  projectionBoardView,
} from "../board/projection.ts";
import { createSeededRng } from "./rng.ts";
import {
  applyCommand,
  nextCommandId,
  RiskGame,
  startGame,
  type ScriptedGame,
} from "../../test/testkit.ts";

// ---------------------------------------------------------------------------
// Map + rng + dice primitives
// ---------------------------------------------------------------------------

describe("map", () => {
  it("has a symmetric, connected adjacency graph", () => {
    for (const t of TERRITORIES) {
      for (const other of t.adjacent) {
        expect(areAdjacent(t.id, other)).toBe(true);
        expect(areAdjacent(other, t.id)).toBe(true);
      }
    }
  });

  it("computes the classic reinforcement pool with a floor of the minimum", () => {
    expect(reinforcementPool(0)).toBe(RULES.minReinforcements);
    expect(reinforcementPool(6)).toBe(RULES.minReinforcements);
    expect(reinforcementPool(12)).toBe(4);
  });
});

describe("rng", () => {
  it("is deterministic for a given seed", () => {
    const a = createSeededRng(7);
    const b = createSeededRng(7);
    const seqA = Array.from({ length: 10 }, () => a.nextInt(6));
    const seqB = Array.from({ length: 10 }, () => b.nextInt(6));
    expect(seqA).toEqual(seqB);
    expect(seqA.every((n) => n >= 0 && n < 6)).toBe(true);
  });
});

describe("dice", () => {
  it("never inflicts more losses than dice compared, and captures leave no attacker loss", () => {
    for (let seed = 0; seed < 200; seed += 1) {
      const rng = createSeededRng(seed);
      const toArmies = (seed % 2) + 1; // 1 or 2 defenders
      const attackerDice = (seed % 3) + 1;
      const r = resolveAttack(toArmies, attackerDice, rng);
      const pairs = Math.min(attackerDice, Math.min(RULES.maxDefenderDice, toArmies));
      expect(r.attackerLosses + r.defenderLosses).toBe(pairs);
      if (r.territoryCaptured) {
        expect(r.attackerLosses).toBe(0);
        expect(r.occupyingArmies).toBe(attackerDice);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Setup + rules
// ---------------------------------------------------------------------------

describe("game setup", () => {
  it("deals every territory once with one army and starts player one in reinforce", () => {
    const scripted = startGame(2);
    const s = scripted.state();
    expect(s.status).toBe("playing");
    expect(s.round).toBe(1);
    expect(s.phase).toBe("reinforce");
    expect(Object.keys(s.territories).toSorted()).toEqual([...TERRITORY_IDS].toSorted());
    for (const t of Object.values(s.territories)) {
      expect(t.armies).toBe(RULES.initialArmiesPerTerritory);
      expect(t.ownerId).toBeDefined();
    }
    expect(s.reinforcementsRemaining).toBe(reinforcementPool(3));
    expect(scripted.turnId()).toBe(buildTurnId(1, s.activePlayerId!));
  });

  it("rejects starting with too few players", () => {
    const game = new RiskGame(createSeededRng(1));
    game.submit({
      type: "create-game",
      commandId: "c1",
      gameId: "g",
      hostPlayerId: "p1",
      hostName: "P1",
      hostColor: "red",
    });
    const outcome = game.submit({ type: "start-game", commandId: "c2" });
    expect(outcome.status).toBe("rejected");
    if (outcome.status === "rejected") expect(outcome.error.code).toBe("NOT_ENOUGH_PLAYERS");
  });
});

describe("reinforce phase", () => {
  it("transitions to attack only once the whole pool is placed", () => {
    const scripted = startGame(2);
    const s0 = scripted.state();
    const active = s0.activePlayerId!;
    const owned = Object.values(s0.territories)
      .filter((t) => t.ownerId === active)
      .map((t) => t.id);

    // Place one army; still reinforcing.
    scripted.submit({
      type: "reinforce",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: active,
      territoryId: owned[0]!,
      armies: 1,
    });
    expect(scripted.state().phase).toBe("reinforce");

    // Place the rest; now attacking.
    scripted.submit({
      type: "reinforce",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: active,
      territoryId: owned[0]!,
      armies: scripted.state().reinforcementsRemaining,
    });
    expect(scripted.state().phase).toBe("attack");
  });

  it("rejects placing more armies than remain", () => {
    const scripted = startGame(2);
    const s = scripted.state();
    const active = s.activePlayerId!;
    const owned = Object.values(s.territories).find((t) => t.ownerId === active)!.id;
    const outcome = scripted.submit({
      type: "reinforce",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: active,
      territoryId: owned,
      armies: s.reinforcementsRemaining + 1,
    });
    expect(outcome.status).toBe("rejected");
    if (outcome.status === "rejected") expect(outcome.error.code).toBe("INSUFFICIENT_ARMIES");
  });
});

// ---------------------------------------------------------------------------
// Turn preconditions
// ---------------------------------------------------------------------------

describe("turn preconditions", () => {
  it("rejects another player's command with NOT_YOUR_TURN", () => {
    const scripted = startGame(2);
    const s = scripted.state();
    const other = scripted.playerIds.find((id) => id !== s.activePlayerId)!;
    const owned = Object.values(s.territories).find((t) => t.ownerId === other)!.id;
    const outcome = scripted.submit({
      type: "reinforce",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: other,
      territoryId: owned,
      armies: 1,
    });
    expect(outcome.status).toBe("rejected");
    if (outcome.status === "rejected") expect(outcome.error.code).toBe("NOT_YOUR_TURN");
  });

  it("rejects a stale observed turn id and reports the current one", () => {
    const scripted = startGame(2);
    const s = scripted.state();
    const active = s.activePlayerId!;
    const owned = Object.values(s.territories).find((t) => t.ownerId === active)!.id;
    const outcome = scripted.submit({
      type: "reinforce",
      commandId: nextCommandId(),
      turnId: buildTurnId(99, active),
      playerId: active,
      territoryId: owned,
      armies: 1,
    });
    expect(outcome.status).toBe("rejected");
    if (outcome.status === "rejected") {
      expect(outcome.error.code).toBe("STALE_TURN");
      expect(outcome.error.currentTurnId).toBe(scripted.turnId());
    }
  });

  it("rejects attacking during the reinforce phase with INVALID_PHASE", () => {
    const scripted = startGame(2);
    const attack = findAttack(scripted.state());
    const outcome = scripted.submit({
      type: "attack",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: scripted.state().activePlayerId!,
      from: attack.from,
      to: attack.to,
      attackerDice: 1,
    });
    expect(outcome.status).toBe("rejected");
    if (outcome.status === "rejected") expect(outcome.error.code).toBe("INVALID_PHASE");
  });
});

// ---------------------------------------------------------------------------
// Attack + idempotency
// ---------------------------------------------------------------------------

describe("attack idempotency", () => {
  it("returns the original recorded outcome for a retried commandId without re-rolling", () => {
    const scripted = startGame(2);
    reinforceThenReachAttack(scripted);
    const attack = findAttack(scripted.state());
    const attackCommand: AttackCommand = {
      type: "attack",
      commandId: "attack-once",
      turnId: scripted.turnId(),
      playerId: scripted.state().activePlayerId!,
      from: attack.from,
      to: attack.to,
      attackerDice: Math.min(
        RULES.maxAttackerDice,
        scripted.state().territories[attack.from]!.armies - 1,
      ),
    };

    const first = scripted.submit(attackCommand);
    expect(first.status).toBe("accepted");
    const logLength = scripted.game.log.length;

    // Exact retry.
    const retry = scripted.submit(attackCommand);
    expect(retry.status).toBe("duplicate");
    if (first.status !== "rejected" && retry.status !== "rejected") {
      expect(retry.events).toEqual(first.events);
      expect(retry.sourceOffset).toBe(first.sourceOffset);
    }
    // No new events appended by the retry.
    expect(scripted.game.log.length).toBe(logLength);
  });
});

// ---------------------------------------------------------------------------
// Determinism, replay, and projection equivalence
// ---------------------------------------------------------------------------

describe("determinism and projection equivalence", () => {
  it("replays a full game deterministically and keeps aggregate/projection in lockstep", () => {
    const commands = recordFullGame(1234);
    expect(commands.length).toBeGreaterThan(5);

    const logA = replay(commands, 1234);
    const logB = replay(commands, 1234);
    expect(logB).toEqual(logA);

    const finalA = foldAggregate(logA);
    expect(finalA.status).toBe("finished");
    expect(finalA.winnerId).toBeDefined();

    // Folding the identical events twice yields identical aggregate state.
    expect(foldAggregate(logA)).toEqual(foldAggregate(logB));

    // Projection equals the command-side fold at every prefix offset.
    for (let i = 0; i <= logA.length; i += 1) {
      const prefix = logA.slice(0, i);
      const aggView = aggregateBoardView(foldAggregate(prefix));
      const projView = projectionBoardView(projectEvents(prefix));
      expect(boardsEqual(aggView, projView)).toBe(true);
    }
  });

  it("winner owns every territory and every other player is eliminated", () => {
    const commands = recordFullGame(1234);
    const state = foldAggregate(replay(commands, 1234));
    const winner = state.winnerId!;
    for (const t of Object.values(state.territories)) expect(t.ownerId).toBe(winner);
    for (const p of state.players) {
      if (p.id !== winner) expect(p.eliminated).toBe(true);
    }
  });
});

// ---------------------------------------------------------------------------
// Fortify (one move per turn) + emitted terminal events
// ---------------------------------------------------------------------------

describe("fortify", () => {
  it("moves armies once, then only ends the turn", () => {
    const { scripted, from, to, active } = gameWithFortifyPair();
    // Put the whole pool on `from`, advancing into the attack phase.
    scripted.submit({
      type: "reinforce",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: active,
      territoryId: from,
      armies: scripted.state().reinforcementsRemaining,
    });
    expect(scripted.state().phase).toBe("attack");

    const before = scripted.state().territories;
    const fromArmies = before[from]!.armies;
    const toArmies = before[to]!.armies;

    const fortify = scripted.submit({
      type: "fortify",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: active,
      from,
      to,
      armies: 1,
    });
    expect(fortify.status).toBe("accepted");
    const after = scripted.state();
    expect(after.phase).toBe("fortify");
    expect(after.territories[from]!.armies).toBe(fromArmies - 1);
    expect(after.territories[to]!.armies).toBe(toArmies + 1);

    // A second maneuver is refused; only end-turn remains.
    const secondFortify = scripted.submit({
      type: "attack",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: active,
      from,
      to,
      attackerDice: 1,
    });
    expect(secondFortify.status).toBe("rejected");
    if (secondFortify.status === "rejected") {
      expect(secondFortify.error.code).toBe("INVALID_PHASE");
    }

    const endTurn = scripted.submit({
      type: "end-turn",
      commandId: nextCommandId(),
      turnId: scripted.turnId(),
      playerId: active,
    });
    expect(endTurn.status).toBe("accepted");
    expect(scripted.state().activePlayerId).not.toBe(active);
  });
});

describe("terminal events", () => {
  it("records exactly one elimination per loser and a single GameWon", () => {
    const log = replay(recordFullGame(1234), 1234);
    const won = log.filter((e) => e.type === "GameWon");
    const eliminated = log.filter((e) => e.type === "PlayerEliminated");
    expect(won).toHaveLength(1);
    expect(eliminated).toHaveLength(1); // 2-player game: one loser
  });
});

// ---------------------------------------------------------------------------
// Helpers: a greedy deterministic auto-player used to exercise full games.
// ---------------------------------------------------------------------------

/** Find a seed whose opening position gives the active player an adjacent owned pair. */
function gameWithFortifyPair(): {
  scripted: ScriptedGame;
  from: string;
  to: string;
  active: string;
} {
  for (let seed = 1; seed < 200; seed += 1) {
    const scripted = startGame(2, seed);
    const s = scripted.state();
    const active = s.activePlayerId!;
    const owned = new Set(
      Object.values(s.territories)
        .filter((t) => t.ownerId === active)
        .map((t) => t.id),
    );
    for (const id of owned) {
      const neighbour = TERRITORIES.find((t) => t.id === id)!.adjacent.find((adj) =>
        owned.has(adj),
      );
      if (neighbour) return { scripted, from: id, to: neighbour, active };
    }
  }
  throw new Error("no seed produced an adjacent owned pair");
}

interface AttackChoice {
  from: string;
  to: string;
}

function enemyNeighbours(state: AggregateState, territoryId: string, ownerId: string): string[] {
  return TERRITORIES.find((t) => t.id === territoryId)!.adjacent.filter(
    (adj) => state.territories[adj]!.ownerId !== ownerId,
  );
}

/** A frontier attack for the active player, or throws if none exists yet. */
function findAttack(state: AggregateState): AttackChoice {
  const active = state.activePlayerId!;
  for (const t of Object.values(state.territories)) {
    if (t.ownerId !== active) continue;
    const enemy = enemyNeighbours(state, t.id, active)[0];
    if (enemy) return { from: t.id, to: enemy };
  }
  throw new Error("no frontier attack available");
}

/** Reinforce onto a frontier territory so the active player can attack. */
function reinforceThenReachAttack(scripted: ScriptedGame): void {
  const s = scripted.state();
  const active = s.activePlayerId!;
  const frontier =
    Object.values(s.territories).find(
      (t) => t.ownerId === active && enemyNeighbours(s, t.id, active).length > 0,
    ) ?? Object.values(s.territories).find((t) => t.ownerId === active)!;
  scripted.submit({
    type: "reinforce",
    commandId: nextCommandId(),
    turnId: scripted.turnId(),
    playerId: active,
    territoryId: frontier.id,
    armies: s.reinforcementsRemaining,
  });
}

/**
 * Record the complete command sequence (setup + greedy play) that drives a
 * two-player game to a win on the given seed. The setup commands are captured so
 * {@link replay} can reproduce the entire game from an empty log.
 */
function recordFullGame(seed: number): Command[] {
  const rng = createSeededRng(seed);
  let events: GameEvent[] = [];
  const commands: Command[] = [];
  const submit = (command: Command): void => {
    commands.push(command);
    const result = applyCommand(events, command, rng);
    if (result.outcome.status === "rejected") {
      throw new Error(`unexpected rejection: ${result.outcome.error.code}`);
    }
    events = result.events;
  };

  submit({
    type: "create-game",
    commandId: nextCommandId(),
    gameId: "game-1",
    hostPlayerId: "p1",
    hostName: "Player 1",
    hostColor: "red",
  });
  submit({
    type: "join-game",
    commandId: nextCommandId(),
    playerId: "p2",
    name: "Player 2",
    color: "blue",
  });
  submit({ type: "start-game", commandId: nextCommandId() });

  for (let guard = 0; guard < 1000; guard += 1) {
    const s = foldAggregate(events);
    if (s.status === "finished") return commands;
    const active = s.activePlayerId!;
    const turnId = buildTurnId(s.round, active);

    if (s.phase === "reinforce") {
      const frontier =
        Object.values(s.territories).find(
          (t) => t.ownerId === active && enemyNeighbours(s, t.id, active).length > 0,
        ) ?? Object.values(s.territories).find((t) => t.ownerId === active)!;
      submit({
        type: "reinforce",
        commandId: nextCommandId(),
        turnId,
        playerId: active,
        territoryId: frontier.id,
        armies: s.reinforcementsRemaining,
      });
      continue;
    }

    if (s.phase === "attack") {
      const from = Object.values(s.territories).find(
        (t) => t.ownerId === active && t.armies >= 2 && enemyNeighbours(s, t.id, active).length > 0,
      );
      if (from) {
        submit({
          type: "attack",
          commandId: nextCommandId(),
          turnId,
          playerId: active,
          from: from.id,
          to: enemyNeighbours(s, from.id, active)[0]!,
          attackerDice: Math.min(RULES.maxAttackerDice, from.armies - 1),
        });
        continue;
      }
    }

    // Nothing productive to do (attack or fortify phase): end the turn.
    submit({ type: "end-turn", commandId: nextCommandId(), turnId, playerId: active });
  }

  throw new Error("game did not converge");
}

/** Re-apply a recorded command list from an empty log under the same rng seed. */
function replay(commands: readonly Command[], seed: number): GameEvent[] {
  const rng = createSeededRng(seed);
  let events: GameEvent[] = [];
  for (const command of commands) {
    events = applyCommand(events, command, rng).events;
  }
  return events;
}
