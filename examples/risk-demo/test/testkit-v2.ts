/**
 * Scripting helpers for deterministic `risk-demo-v2` games.
 *
 * The v1 testkit drives a fixed six-territory board; v2 needs three extra things:
 * a procedurally generated map (so territory ids are only known at runtime), an
 * injected clock (so the defence deadline can be crossed without waiting 15 real
 * seconds), and a way to *rig* dice so capture / no-capture / elimination paths
 * are reachable on demand rather than by seed hunting.
 *
 * Everything here talks to the pure kernel only — `foldAggregateV2` + `decideV2`.
 * The durable command log, timers, and HTTP surface are exercised separately by
 * the integration tests.
 */

import type { AggregateStateV2, PendingInteraction } from "../src/domain/aggregate-v2.ts";
import { buildTurnIdV2, foldAggregateV2 } from "../src/domain/aggregate-v2.ts";
import type { CommandV2, RiskErrorCodeV2 } from "../src/domain/commands-v2.ts";
import type { DecideContextV2, DecisionErrorV2 } from "../src/domain/decide-v2.ts";
import { decideV2 } from "../src/domain/decide-v2.ts";
import type { GameEventV2 } from "../src/domain/events-v2.ts";
import type { GeneratedMap } from "../src/domain/map-v2.ts";
import { RULES_V2 } from "../src/domain/map-v2.ts";
import type { InitialTerritoryV2 } from "../src/domain/setup-v2.ts";
import type { Rng } from "../src/domain/rng.ts";
import { createSeededRng } from "../src/domain/rng.ts";
import { legalActionsV2, type LegalActionV2 } from "../src/application/legal-actions-v2.ts";

export type CommandOutcomeV2 =
  | {
      status: "accepted" | "duplicate";
      commandId: string;
      sourceOffset: string;
      events: GameEventV2[];
    }
  | { status: "rejected"; commandId: string; error: DecisionErrorV2 };

/**
 * An `Rng` that yields the given die faces in order, then falls back to a seeded
 * stream. Faces are 1..6; the value is converted back to the `[0, bound)` form
 * `nextInt` promises, so rigged and seeded rolls are indistinguishable to the
 * kernel.
 */
export function riggedRng(faces: readonly number[], fallbackSeed = 99): Rng {
  const queue = faces.slice();
  const fallback = createSeededRng(fallbackSeed);
  return {
    nextInt(bound: number): number {
      if (queue.length === 0) return fallback.nextInt(bound);
      const face = queue.shift()!;
      return (face - 1) % bound;
    },
  };
}

export interface MutableClock {
  now: number;
}

export interface ScriptedGameV2 {
  readonly gameId: string;
  readonly playerIds: readonly string[];
  readonly log: readonly GameEventV2[];
  readonly clock: MutableClock;
  state(): AggregateStateV2;
  turnId(): string;
  actions(playerId?: string): LegalActionV2[];
  submit(command: CommandV2): CommandOutcomeV2;
  /** Submit and throw on rejection — for steps a test treats as setup, not subject. */
  must(command: CommandV2): CommandOutcomeV2;
  /** Replace the dice source for the next commands (rig a specific outcome). */
  rig(faces: readonly number[]): void;
  advanceClock(ms: number): void;
}

let counter = 0;

/** Monotonic command id so scripted commands never collide accidentally. */
export function nextCommandIdV2(prefix = "c"): string {
  counter += 1;
  return `${prefix}-${counter}`;
}

export interface StartGameV2Options {
  players?: number;
  mapSeed?: string;
  rngSeed?: number;
  defenseTimeoutMs?: number;
  /** Player controllers, defaulting to all `human`. */
  controllers?: ReadonlyArray<"human" | "bot" | "external-agent">;
  /**
   * Rewrite the canonical `GameStarted` allocation so a test can start from a
   * specific board — a near-final position, a disconnected holding, a continent
   * one country short. The result is still a normal canonical event, so the fold
   * and every rule below it are exercised exactly as in a real game.
   */
  board?: (allocation: {
    map: GeneratedMap;
    turnOrder: readonly string[];
    initialTerritories: readonly InitialTerritoryV2[];
  }) => { turnOrder?: readonly string[]; initialTerritories: readonly InitialTerritoryV2[] };
}

const COLORS = ["red", "blue", "green", "yellow"];

/** Create + join + start a v2 game on a fixed seed, with a controllable clock. */
export function startGameV2(options: StartGameV2Options = {}): ScriptedGameV2 {
  const playerCount = options.players ?? 2;
  const gameId = "game-v2";
  const playerIds = Array.from({ length: playerCount }, (_, i) => `p${i + 1}`);
  const clock: MutableClock = { now: 1_700_000_000_000 };
  let rng: Rng = createSeededRng(options.rngSeed ?? 7);
  let events: GameEventV2[] = [];

  const ctx: DecideContextV2 = {
    rng: { nextInt: (bound) => rng.nextInt(bound) },
    now: () => clock.now,
    defenseTimeoutMs: options.defenseTimeoutMs ?? RULES_V2.defenseTimeoutMs,
  };

  const submit = (command: CommandV2): CommandOutcomeV2 => {
    const state = foldAggregateV2(events);
    const prior = state.commandIndex[command.commandId];
    if (prior) {
      return {
        status: "duplicate",
        commandId: command.commandId,
        sourceOffset: String(prior.lastOffset),
        events: prior.events.slice(),
      };
    }
    const decision = decideV2(state, command, ctx);
    if (decision.status === "rejected") {
      return { status: "rejected", commandId: command.commandId, error: decision.error };
    }
    events = events.concat(decision.events);
    return {
      status: "accepted",
      commandId: command.commandId,
      sourceOffset: String(events.length - 1),
      events: decision.events,
    };
  };

  const must = (command: CommandV2): CommandOutcomeV2 => {
    const outcome = submit(command);
    if (outcome.status === "rejected") {
      throw new Error(`${command.type} rejected: ${outcome.error.code} — ${outcome.error.message}`);
    }
    return outcome;
  };

  const game: ScriptedGameV2 = {
    gameId,
    playerIds,
    get log() {
      return events;
    },
    clock,
    state: () => foldAggregateV2(events),
    turnId() {
      const state = foldAggregateV2(events);
      return buildTurnIdV2(state.round, state.activePlayerId!);
    },
    actions(playerId?: string) {
      const state = foldAggregateV2(events);
      return legalActionsV2(state, playerId ?? state.activePlayerId!);
    },
    submit,
    must,
    rig(faces) {
      rng = riggedRng(faces, options.rngSeed ?? 7);
    },
    advanceClock(ms) {
      clock.now += ms;
    },
  };

  must({
    type: "create-game",
    commandId: nextCommandIdV2(),
    gameId,
    hostPlayerId: playerIds[0]!,
    hostName: "Player 1",
    hostColor: COLORS[0]!,
    hostController: options.controllers?.[0] ?? "human",
    mapSeed: options.mapSeed ?? "seed-v2-fixture",
  });
  for (let i = 1; i < playerCount; i += 1) {
    must({
      type: "join-game",
      commandId: nextCommandIdV2(),
      playerId: playerIds[i]!,
      name: `Player ${i + 1}`,
      color: COLORS[i]!,
      controller: options.controllers?.[i] ?? "human",
    });
  }
  must({ type: "start-game", commandId: nextCommandIdV2() });

  if (options.board) {
    const started = events.at(-1)!;
    if (started.type !== "GameStarted") throw new Error("start-game did not append GameStarted");
    const rewritten = options.board({
      map: started.map,
      turnOrder: started.turnOrder,
      initialTerritories: started.initialTerritories,
    });
    events = events.slice(0, -1).concat({
      ...started,
      turnOrder: (rewritten.turnOrder ?? started.turnOrder).slice(),
      initialTerritories: rewritten.initialTerritories.map((t) => ({ ...t })),
    });
  }

  return game;
}

/** Place the whole reinforcement pool on one owned country, entering `attack`. */
export function placeAllReinforcementsV2(game: ScriptedGameV2, territoryId?: string): string {
  const state = game.state();
  const active = state.activePlayerId!;
  const target =
    territoryId ?? Object.values(state.territories).find((t) => t.ownerId === active)!.id;
  game.must({
    type: "reinforce",
    commandId: nextCommandIdV2(),
    turnId: game.turnId(),
    playerId: active,
    placements: [{ territoryId: target, armies: state.reinforcement.remaining }],
  });
  return target;
}

export interface AttackSetup {
  from: string;
  to: string;
  attackerId: string;
  defenderId: string;
}

/**
 * Stack the active player's whole pool onto a border country and return the
 * attack it can now make — the standard preamble for every combat test.
 */
export function armForAttack(game: ScriptedGameV2): AttackSetup {
  const state = game.state();
  const active = state.activePlayerId!;
  const index = state.index!;
  const border = Object.values(state.territories).find(
    (t) =>
      t.ownerId === active &&
      index.territoryById
        .get(t.id)!
        .adjacentTerritoryIds.some((adj) => state.territories[adj]!.ownerId !== active),
  )!;
  placeAllReinforcementsV2(game, border.id);
  const after = game.state();
  const to = index.territoryById
    .get(border.id)!
    .adjacentTerritoryIds.find((adj) => after.territories[adj]!.ownerId !== active)!;
  return { from: border.id, to, attackerId: active, defenderId: after.territories[to]!.ownerId! };
}

/** Declare an attack with the maximum legal dice and return its `attackId`. */
export function declareAttackV2(
  game: ScriptedGameV2,
  setup: AttackSetup,
  attackerDice?: number,
): string {
  const state = game.state();
  const armies = state.territories[setup.from]!.armies;
  const commandId = nextCommandIdV2("atk");
  game.must({
    type: "declare-attack",
    commandId,
    turnId: game.turnId(),
    playerId: setup.attackerId,
    from: setup.from,
    to: setup.to,
    attackerDice: attackerDice ?? Math.min(RULES_V2.maxAttackerDice, armies - 1),
  });
  return commandId;
}

/**
 * Declare and resolve one throw the attacker wins outright: every attacker die
 * is a 6 and every defender die a 1, so the attacker sweeps all compared pairs.
 * Rigging each half separately matters — the two rolls are drawn at different
 * times, so a single queue would hand the attacker's leftovers to the defender.
 */
export function winThrow(game: ScriptedGameV2, setup: AttackSetup, attackerDice?: number): string {
  const armies = game.state().territories[setup.from]!.armies;
  const dice = attackerDice ?? Math.min(RULES_V2.maxAttackerDice, armies - 1);
  game.rig(Array.from({ length: dice }, () => 6));
  const attackId = declareAttackV2(game, setup, dice);
  game.rig([1, 1]);
  game.must({
    type: "roll-defense",
    commandId: nextCommandIdV2(),
    turnId: game.turnId(),
    playerId: setup.defenderId,
    attackId,
  });
  return attackId;
}

/** Throw winning dice until `setup.to` falls; returns the pending occupation. */
export function throwUntilCapture(
  game: ScriptedGameV2,
  setup: AttackSetup,
): Extract<PendingInteraction, { type: "occupation" }> {
  for (let guard = 0; guard < 16; guard += 1) {
    if (game.state().territories[setup.from]!.armies < 2) break;
    winThrow(game, setup);
    const pending = game.state().pendingInteraction;
    if (pending?.type === "occupation") return pending;
  }
  throw new Error(`${setup.to} never fell`);
}

/** Take the pending occupation with the given garrison (defaulting to the minimum). */
export function occupyPending(game: ScriptedGameV2, armies?: number): CommandOutcomeV2 {
  const pending = game.state().pendingInteraction;
  if (pending?.type !== "occupation") throw new Error("no pending occupation");
  return game.must({
    type: "occupy-territory",
    commandId: nextCommandIdV2(),
    turnId: game.turnId(),
    playerId: pending.playerId,
    attackId: pending.attackId,
    armies: armies ?? pending.minArmies,
  });
}

export type { RiskErrorCodeV2 };
