/**
 * Test/demo helpers for scripting deterministic games. Kept in `src` so tests and
 * later scripted demos can share one bootstrap, but not part of the public kernel
 * surface conceptually.
 */

import type { Command } from "../src/domain/commands.ts";
import type { GameEvent } from "../src/domain/events.ts";
import { foldAggregate, buildTurnId } from "../src/domain/aggregate.ts";
import type { AggregateState } from "../src/domain/aggregate.ts";
import { decide, type DecisionError } from "../src/domain/decide.ts";
import { RULES, TERRITORIES } from "../src/domain/map.ts";
import { createSeededRng, type Rng } from "../src/domain/rng.ts";

export type CommandOutcome =
  | {
      status: "accepted" | "duplicate";
      commandId: string;
      turnId?: string;
      sourceOffset: string;
      events: GameEvent[];
    }
  | { status: "rejected"; commandId: string; error: DecisionError };

export function applyCommand(
  events: readonly GameEvent[],
  command: Command,
  rng: Rng,
): { events: GameEvent[]; outcome: CommandOutcome } {
  const state = foldAggregate(events);
  const prior = state.commandIndex[command.commandId];
  if (prior) {
    return {
      events: events.slice(),
      outcome: {
        status: "duplicate",
        commandId: command.commandId,
        turnId: "turnId" in command ? command.turnId : undefined,
        sourceOffset: String(prior.lastOffset),
        events: prior.events.slice(),
      },
    };
  }
  const decision = decide(state, command, rng);
  if (decision.status === "rejected") {
    return {
      events: events.slice(),
      outcome: { status: "rejected", commandId: command.commandId, error: decision.error },
    };
  }
  const nextEvents = events.concat(decision.events);
  return {
    events: nextEvents,
    outcome: {
      status: "accepted",
      commandId: command.commandId,
      turnId: "turnId" in command ? command.turnId : undefined,
      sourceOffset: String(nextEvents.length - 1),
      events: decision.events,
    },
  };
}

export class RiskGame {
  private events: GameEvent[] = [];

  constructor(private readonly rng: Rng = createSeededRng(1)) {}

  get log(): readonly GameEvent[] {
    return this.events;
  }

  submit(command: Command): CommandOutcome {
    const result = applyCommand(this.events, command, this.rng);
    this.events = result.events;
    return result.outcome;
  }

  state() {
    return foldAggregate(this.events);
  }
}

let commandCounter = 0;

/** Monotonic command id so scripted commands never collide accidentally. */
export function nextCommandId(prefix = "cmd"): string {
  commandCounter += 1;
  return `${prefix}-${commandCounter}`;
}

export interface ScriptedGame {
  game: RiskGame;
  gameId: string;
  playerIds: string[];
  /** Ordered non-eliminated players as of the last fold. */
  state(): AggregateState;
  turnId(): string;
  submit(command: Command): CommandOutcome;
}

/** Create + join + start a game with `playerCount` players on a fixed seed. */
export function startGame(playerCount: number, seed = 42): ScriptedGame {
  const game = new RiskGame(createSeededRng(seed));
  const gameId = "game-1";
  const playerIds = Array.from({ length: playerCount }, (_, i) => `p${i + 1}`);
  const colors = ["red", "blue", "green", "yellow"];

  game.submit({
    type: "create-game",
    commandId: nextCommandId(),
    gameId,
    hostPlayerId: playerIds[0]!,
    hostName: "Player 1",
    hostColor: colors[0]!,
  });
  for (let i = 1; i < playerCount; i += 1) {
    game.submit({
      type: "join-game",
      commandId: nextCommandId(),
      playerId: playerIds[i]!,
      name: `Player ${i + 1}`,
      color: colors[i]!,
    });
  }
  game.submit({ type: "start-game", commandId: nextCommandId() });

  return {
    game,
    gameId,
    playerIds,
    state: () => foldAggregate(game.log),
    turnId() {
      const s = foldAggregate(game.log);
      return buildTurnId(s.round, s.activePlayerId!);
    },
    submit: (command) => game.submit(command),
  };
}

/**
 * Place all of the active player's reinforcements onto their first owned
 * territory, advancing the turn into the attack phase. Returns that territory.
 */
export function placeAllReinforcements(scripted: ScriptedGame): string {
  const s = scripted.state();
  const active = s.activePlayerId!;
  const target = Object.values(s.territories).find((t) => t.ownerId === active)!.id;
  const outcome = scripted.submit({
    type: "reinforce",
    commandId: nextCommandId(),
    turnId: scripted.turnId(),
    playerId: active,
    territoryId: target,
    armies: s.reinforcementsRemaining,
  });
  if (outcome.status === "rejected") {
    throw new Error(`reinforce rejected: ${outcome.error.code}`);
  }
  return target;
}

function enemyNeighbours(state: AggregateState, territoryId: string, ownerId: string): string[] {
  return TERRITORIES.find((t) => t.id === territoryId)!.adjacent.filter(
    (adj) => state.territories[adj]!.ownerId !== ownerId,
  );
}

/**
 * Drive a full two-player game to a win with a greedy deterministic auto-player
 * and return the canonical event log. Shared by kernel and materializer tests.
 */
export function recordFullGameEvents(seed = 1234): GameEvent[] {
  const rng = createSeededRng(seed);
  let events: GameEvent[] = [];
  const submit = (command: Command): void => {
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
    if (s.status === "finished") return events;
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

    submit({ type: "end-turn", commandId: nextCommandId(), turnId, playerId: active });
  }

  throw new Error("game did not converge");
}
