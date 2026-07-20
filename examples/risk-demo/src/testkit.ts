/**
 * Test/demo helpers for scripting deterministic games. Kept in `src` so tests and
 * later scripted demos can share one bootstrap, but not part of the public kernel
 * surface conceptually.
 */

import type { Command } from "./commands.ts";
import { RiskGame } from "./engine.ts";
import type { CommandOutcome } from "./engine.ts";
import { foldAggregate, buildTurnId } from "./aggregate.ts";
import type { AggregateState } from "./aggregate.ts";
import { createSeededRng } from "./rng.ts";

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
