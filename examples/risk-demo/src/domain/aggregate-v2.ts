/**
 * Pure authoritative `risk-demo-v2` aggregate fold (design spec §4).
 *
 * Like its v1 counterpart this is total and deterministic: the same events always
 * produce the same state, and it never consults an `Rng` or a clock. Every die is
 * read from a recorded `AttackDeclared`/`AttackResolved`, and the defence
 * deadline is read from the declaration rather than recomputed from "now".
 *
 * Two things distinguish it from v1:
 *
 *  - The board is *data*. `GameStarted` carries the whole `GeneratedMap`, so
 *    adjacency, continents, and territory identity come from the snapshot rather
 *    than a compile-time table. Nothing here imports the generator.
 *  - Combat is an *interrupt*, not a phase. The ordinary turn phase stays
 *    `reinforce | attack | fortify`; a pending defence or occupation is modelled
 *    as {@link PendingInteraction} sitting on top of it. While one exists the turn
 *    and `turnId` do not change, and only the one command that closes it is legal.
 *
 * Derived (event-free) transitions kept here so the event log stays minimal:
 *  - reinforce -> attack once the reinforcement pool is fully placed;
 *  - the reinforcement pool and its continent breakdown are recomputed whenever a
 *    turn begins — which is why capturing a continent's last country mid-turn
 *    grants nothing until the *next* reinforcement phase.
 *
 * A successful fortify records `ArmiesFortified` followed by `TurnEnded` in the
 * same command batch. The fold still understands the intermediate `fortify` phase
 * for old logs and event-by-event replay, but it is no longer a player decision
 * boundary.
 */

import { compareRolls } from "./dice-v2.ts";
import type { DefenseResolutionSource, GameEventV2, PlayerController } from "./events-v2.ts";
import type { ContinentDef, GeneratedMap, MapIndex } from "./map-v2.ts";
import { baseReinforcement, indexMap } from "./map-v2.ts";

export type GamePhaseV2 = "reinforce" | "attack" | "fortify";
export type GameStatusV2 = "lobby" | "playing" | "finished";

export interface PlayerStateV2 {
  id: string;
  name: string;
  color: string;
  controller: PlayerController;
  eliminated: boolean;
}

export interface TerritoryStateV2 {
  id: string;
  ownerId?: string;
  armies: number;
}

/** The continent bonuses folded into the active player's current pool. */
export interface ContinentBonus {
  continentId: string;
  bonus: number;
}

/**
 * The reinforcement accounting for the active turn, kept as a breakdown rather
 * than a single number so the decision API and the turn rail can explain the
 * total without re-deriving it.
 */
export interface ReinforcementState {
  base: number;
  continents: ContinentBonus[];
  total: number;
  remaining: number;
}

export type PendingInteraction =
  | {
      type: "defense";
      attackId: string;
      turnId: string;
      attackerId: string;
      defenderId: string;
      from: string;
      to: string;
      attackerDice: number;
      attackerRolls: number[];
      defenderDice: number;
      declaredAt: number;
      defenseDeadlineAt: number;
    }
  | {
      type: "occupation";
      attackId: string;
      turnId: string;
      playerId: string;
      from: string;
      to: string;
      minArmies: number;
      maxArmies: number;
    };

/** Lifecycle of one declared attack, so a late resolver can tell *why* it lost. */
export type AttackStatus = "awaiting-defense" | "resolved" | "occupied";

export interface AttackRecord {
  attackId: string;
  turnId: string;
  status: AttackStatus;
  resolutionSource?: DefenseResolutionSource;
}

export interface CommandRecordV2 {
  /** Events appended for this command, in order. */
  events: GameEventV2[];
  /** Positional index of the command's last event in the log. */
  lastOffset: number;
}

export interface AggregateStateV2 {
  gameId?: string;
  ruleset?: string;
  mapVersion?: string;
  generatorVersion?: string;
  mapSeed?: string;
  status: GameStatusV2;
  players: PlayerStateV2[];
  turnOrder: string[];
  activePlayerId?: string;
  phase?: GamePhaseV2;
  round: number;
  /** The canonical board snapshot recorded in `GameStarted`; absent in the lobby. */
  map?: GeneratedMap;
  /** Indexed view of {@link map}, rebuilt by the fold — never a separate truth. */
  index?: MapIndex;
  territories: Record<string, TerritoryStateV2>;
  reinforcement: ReinforcementState;
  /** The open combat interrupt, if any. Blocks every other command. */
  pendingInteraction?: PendingInteraction;
  /** Every declared attack by id, so a stale resolver gets an accurate rejection. */
  attacks: Record<string, AttackRecord>;
  winnerId?: string;
  /** commandId -> the outcome it produced, for idempotent replay/dedup. */
  commandIndex: Record<string, CommandRecordV2>;
  /** Number of events folded so far; equals the next event's offset. */
  eventCount: number;
}

/**
 * Thrown when canonical history contradicts itself — for example an
 * `AttackResolved` whose repeated attacker rolls do not match the declaration it
 * claims to resolve. The repetition in §5.1 exists so the move feed is
 * self-contained, not so consumers can trust it blindly; a mismatch means the
 * stream is corrupt and must fail loudly rather than fold into a plausible board.
 */
export class AggregateIntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AggregateIntegrityError";
  }
}

export function initialStateV2(): AggregateStateV2 {
  return {
    status: "lobby",
    players: [],
    turnOrder: [],
    round: 0,
    territories: {},
    reinforcement: { base: 0, continents: [], total: 0, remaining: 0 },
    attacks: {},
    commandIndex: {},
    eventCount: 0,
  };
}

/** Turn identity a play command must observe: `round-<n>:<playerId>`. */
export function buildTurnIdV2(round: number, playerId: string): string {
  return `round-${round}:${playerId}`;
}

export function currentTurnIdV2(state: AggregateStateV2): string | undefined {
  if (state.status !== "playing" || !state.activePlayerId) return undefined;
  return buildTurnIdV2(state.round, state.activePlayerId);
}

export function ownedByV2(state: AggregateStateV2, playerId: string): string[] {
  return Object.values(state.territories)
    .filter((t) => t.ownerId === playerId)
    .map((t) => t.id)
    .toSorted();
}

export function playerV2(state: AggregateStateV2, playerId: string): PlayerStateV2 | undefined {
  return state.players.find((p) => p.id === playerId);
}

/** Non-eliminated players in `turnOrder`, preserving order. */
function activePlayers(state: AggregateStateV2): string[] {
  return state.turnOrder.filter((id) => !playerV2(state, id)?.eliminated);
}

/** Continents whose every territory is currently owned by `playerId`. */
export function controlledContinents(
  state: AggregateStateV2,
  playerId: string,
): readonly ContinentDef[] {
  if (!state.map) return [];
  return state.map.continents.filter((continent) =>
    continent.territoryIds.every((id) => state.territories[id]?.ownerId === playerId),
  );
}

/**
 * The reinforcement a player would receive if their turn began right now.
 *
 * Exposed because the pool is a *snapshot taken at the start of the reinforcement
 * phase*: continent bonuses are evaluated once, here, and a continent completed
 * later in the same turn changes nothing until the player's next turn.
 */
export function computeReinforcement(
  state: AggregateStateV2,
  playerId: string,
): ReinforcementState {
  const base = baseReinforcement(ownedByV2(state, playerId).length);
  const continents = controlledContinents(state, playerId).map((continent) => ({
    continentId: continent.id,
    bonus: continent.reinforcementBonus,
  }));
  const total = continents.reduce((sum, c) => sum + c.bonus, base);
  return { base, continents, total, remaining: total };
}

function beginTurn(state: AggregateStateV2, playerId: string): void {
  state.activePlayerId = playerId;
  state.phase = "reinforce";
  state.pendingInteraction = undefined;
  state.reinforcement = computeReinforcement(state, playerId);
  // A player who owes no reinforcements (impossible under the current floor of 3,
  // but a rule change should not strand a turn) proceeds straight to attacking.
  if (state.reinforcement.remaining <= 0) state.phase = "attack";
}

function requireDefense(
  state: AggregateStateV2,
  attackId: string,
  context: string,
): Extract<PendingInteraction, { type: "defense" }> {
  const pending = state.pendingInteraction;
  if (!pending || pending.type !== "defense" || pending.attackId !== attackId) {
    throw new AggregateIntegrityError(`${context} does not match a pending declaration`);
  }
  return pending;
}

function applyEvent(state: AggregateStateV2, event: GameEventV2): void {
  switch (event.type) {
    case "GameCreated": {
      state.gameId = event.gameId;
      state.ruleset = event.ruleset;
      state.mapVersion = event.mapVersion;
      state.generatorVersion = event.generatorVersion;
      state.mapSeed = event.mapSeed;
      state.players.push({
        id: event.hostPlayerId,
        name: event.hostName,
        color: event.hostColor,
        controller: event.hostController,
        eliminated: false,
      });
      break;
    }
    case "PlayerJoined": {
      state.players.push({
        id: event.playerId,
        name: event.name,
        color: event.color,
        controller: event.controller,
        eliminated: false,
      });
      break;
    }
    case "PlayerControllerChanged": {
      const player = state.players.find((candidate) => candidate.id === event.playerId);
      if (player) player.controller = event.controller;
      break;
    }
    case "GameStarted": {
      state.status = "playing";
      state.map = event.map;
      state.index = indexMap(event.map);
      state.turnOrder = event.turnOrder.slice();
      state.round = event.round;
      state.territories = {};
      for (const t of event.initialTerritories) {
        state.territories[t.territoryId] = {
          id: t.territoryId,
          ownerId: t.ownerId,
          armies: t.armies,
        };
      }
      beginTurn(state, event.turnOrder[0]!);
      break;
    }
    case "ArmiesReinforced": {
      const territory = state.territories[event.territoryId];
      if (territory) territory.armies += event.armies;
      state.reinforcement.remaining -= event.armies;
      if (state.reinforcement.remaining <= 0) state.phase = "attack";
      break;
    }
    case "AttackDeclared": {
      state.pendingInteraction = {
        type: "defense",
        attackId: event.attackId,
        turnId: event.turnId,
        attackerId: event.attackerId,
        defenderId: event.defenderId,
        from: event.from,
        to: event.to,
        attackerDice: event.attackerDice,
        attackerRolls: event.attackerRolls.slice(),
        defenderDice: event.defenderDice,
        declaredAt: event.declaredAt,
        defenseDeadlineAt: event.defenseDeadlineAt,
      };
      state.attacks[event.attackId] = {
        attackId: event.attackId,
        turnId: event.turnId,
        status: "awaiting-defense",
      };
      break;
    }
    case "AttackResolved": {
      const pending = requireDefense(state, event.attackId, "AttackResolved");
      // The repeated attacker rolls and the whole combat framing must match the
      // declaration; a divergence would silently rewrite a recorded throw.
      const sameRolls =
        pending.attackerRolls.length === event.attackerRolls.length &&
        pending.attackerRolls.every((roll, i) => roll === event.attackerRolls[i]);
      if (
        !sameRolls ||
        pending.turnId !== event.turnId ||
        pending.attackerId !== event.attackerId ||
        pending.defenderId !== event.defenderId ||
        pending.from !== event.from ||
        pending.to !== event.to ||
        pending.defenderDice !== event.defenderRolls.length
      ) {
        throw new AggregateIntegrityError(
          `AttackResolved ${event.attackId} contradicts its declaration`,
        );
      }

      const from = state.territories[event.from]!;
      const to = state.territories[event.to]!;
      from.armies -= event.attackerLosses;
      to.armies -= event.defenderLosses;
      state.attacks[event.attackId] = {
        attackId: event.attackId,
        turnId: event.turnId,
        status: "resolved",
        resolutionSource: event.resolutionSource,
      };

      if (event.territoryCaptured) {
        state.pendingInteraction = {
          type: "occupation",
          attackId: event.attackId,
          turnId: event.turnId,
          playerId: event.attackerId,
          from: event.from,
          to: event.to,
          // A capture always wins every compared pair it needed, so the attacker
          // took no losses and `attackerDice` armies can always be moved.
          minArmies: pending.attackerDice,
          maxArmies: from.armies - 1,
        };
      } else {
        state.pendingInteraction = undefined;
      }
      break;
    }
    case "TerritoryOccupied": {
      const from = state.territories[event.from]!;
      const to = state.territories[event.to]!;
      from.armies -= event.armies;
      to.ownerId = event.playerId;
      to.armies = event.armies;
      const record = state.attacks[event.attackId];
      if (record) record.status = "occupied";
      state.pendingInteraction = undefined;
      break;
    }
    case "ArmiesFortified": {
      const from = state.territories[event.from];
      const to = state.territories[event.to];
      if (from) from.armies -= event.armies;
      if (to) to.armies += event.armies;
      state.phase = "fortify";
      break;
    }
    case "PlayerEliminated": {
      const eliminated = playerV2(state, event.playerId);
      if (eliminated) eliminated.eliminated = true;
      break;
    }
    case "TurnEnded": {
      state.round = event.round;
      beginTurn(state, event.nextPlayerId);
      break;
    }
    case "GameWon": {
      state.status = "finished";
      state.winnerId = event.playerId;
      state.activePlayerId = undefined;
      state.phase = undefined;
      state.pendingInteraction = undefined;
      state.reinforcement = { base: 0, continents: [], total: 0, remaining: 0 };
      break;
    }
  }
}

export function foldAggregateV2(events: readonly GameEventV2[]): AggregateStateV2 {
  const state = initialStateV2();
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index]!;
    applyEvent(state, event);
    state.eventCount = index + 1;
    const record = state.commandIndex[event.commandId];
    if (record) {
      record.events.push(event);
      record.lastOffset = index;
    } else {
      state.commandIndex[event.commandId] = { events: [event], lastOffset: index };
    }
  }
  return state;
}

/**
 * Determine the next active player after `TurnEnded` and whether the round wraps.
 * Exposed for the decision layer so `TurnEnded` records the same values the fold
 * will re-derive.
 */
export function nextTurnV2(
  state: AggregateStateV2,
  finishingPlayerId: string,
): { nextPlayerId: string; round: number } {
  const order = activePlayers(state);
  const fromIndex = order.indexOf(finishingPlayerId);
  const nextIndex = (fromIndex + 1) % order.length;
  const nextPlayerId = order[nextIndex]!;
  const wrapped = nextIndex <= fromIndex;
  return { nextPlayerId, round: wrapped ? state.round + 1 : state.round };
}

/**
 * Territories reachable from `from` through a path of countries owned by
 * `playerId` (design spec §4.6). The source itself is excluded: a fortify moves
 * armies somewhere else.
 *
 * The path is derived from the canonical territory adjacency graph and current
 * ownership; the command records only source, destination, and count, so nothing
 * about the route needs to be trusted from the client.
 */
export function friendlyReachable(
  state: AggregateStateV2,
  playerId: string,
  from: string,
): string[] {
  if (!state.index || state.territories[from]?.ownerId !== playerId) return [];
  const seen = new Set([from]);
  const queue = [from];
  const reachable: string[] = [];
  for (let head = 0; head < queue.length; head += 1) {
    for (const next of state.index.territoryById.get(queue[head]!)?.adjacentTerritoryIds ?? []) {
      if (seen.has(next) || state.territories[next]?.ownerId !== playerId) continue;
      seen.add(next);
      queue.push(next);
      reachable.push(next);
    }
  }
  return reachable.toSorted();
}

/** Re-export the pure comparison so consumers do not reach past the aggregate. */
export { compareRolls };
