/**
 * Pure `risk-demo-v2` board projection reducer — the query-shaped read model
 * (design spec §7).
 *
 * As in v1 this is deliberately a *second, independent* reduction over the same
 * canonical events: nothing here imports `foldAggregateV2`, and the equivalence
 * check in `projection-v2.test.ts` is only meaningful because of that. If either
 * reducer drifts, {@link boardsEqualV2} catches it.
 *
 * Three v2-specific properties are load-bearing:
 *
 *  - **The map is data.** Every static row — hexes, territories, continents —
 *    comes from `GameStarted.map`. The projection never imports the generator and
 *    never derives adjacency itself, so a projected board cannot disagree with the
 *    canonical snapshot about geometry.
 *  - **Combat is a row, not a phase.** A declared attack inserts one `combat` row
 *    (`awaiting-defense`), the resolution either clears it or moves it to
 *    `awaiting-occupation` with the exact occupation bounds, and the occupation
 *    clears it. Zero or one row exists at any time.
 *  - **No clock, no dice.** Faces and deadlines are read from the recorded events.
 *    That is also why the `meta` row carries no wall-clock update time: the source
 *    offset *is* the projection's notion of time.
 */

import type {
  AttackRecord,
  ContinentBonus,
  GamePhaseV2,
  GameStatusV2,
  PendingInteraction,
  ReinforcementState,
} from "../domain/aggregate-v2.ts";
import type {
  DefenseResolutionSource,
  GameEventV2,
  GameEventV2Type,
  PlayerController,
} from "../domain/events-v2.ts";
import type { ContinentPalette, GeneratedMap, Terrain } from "../domain/map-v2.ts";
import { baseReinforcement } from "../domain/map-v2.ts";
import type { Axial } from "../domain/hex.ts";
import { compareRolls } from "../domain/dice-v2.ts";

export interface ProjectedGameV2 {
  id: string;
  hostPlayerId?: string;
  status: GameStatusV2;
  ruleset?: string;
  mapVersion?: string;
  generatorVersion?: string;
  mapSeed?: string;
  round: number;
  activePlayerId?: string;
  phase?: GamePhaseV2;
  winnerId?: string;
}

export interface ProjectedPlayerV2 {
  id: string;
  name: string;
  color: string;
  controller: PlayerController;
  eliminated: boolean;
  /** Derived totals, so a roster does not have to aggregate territory rows. */
  territoryCount: number;
  armyCount: number;
}

/** Static after `GameStarted`: the hex tiles the renderer draws. */
export interface ProjectedHexV2 {
  id: string;
  q: number;
  r: number;
  territoryId: string;
  terrain: Terrain;
}

export interface ProjectedTerritoryV2 {
  id: string;
  name: string;
  continentId: string;
  ownerId?: string;
  armies: number;
  hexIds: string[];
  adjacentTerritoryIds: string[];
  labelAnchor: Axial;
}

export interface ProjectedContinentV2 {
  id: string;
  name: string;
  territoryIds: string[];
  reinforcementBonus: number;
  /** The player who currently owns every member country, if any. */
  controllerId?: string;
  palette: ContinentPalette;
}

/** The latest completed throw, kept on the turn row so the rail can replay it. */
export interface ProjectedDiceV2 {
  attackId: string;
  from: string;
  to: string;
  attackerRolls: number[];
  defenderRolls: number[];
  attackerLosses: number;
  defenderLosses: number;
  territoryCaptured: boolean;
  resolutionSource: DefenseResolutionSource;
}

/**
 * One current-turn display summary. Purely derived: the bounded `moves` feed
 * remains the longer history, and this row answers "what is happening now?".
 */
export interface ProjectedTurnV2 {
  id: string;
  turnId: string;
  round: number;
  playerId: string;
  phase?: GamePhaseV2;
  reinforcement: ReinforcementState;
  /** Armies placed so far this turn; `total - remaining` made explicit. */
  reinforcementsPlaced: number;
  attacksDeclared: number;
  throwsResolved: number;
  captures: number;
  eliminations: number;
  latestDice?: ProjectedDiceV2;
}

export type CombatRowStatus = "awaiting-defense" | "awaiting-occupation";

/** Zero or one pending-combat presentation row (design spec §7.1). */
export interface ProjectedCombatV2 {
  id: string;
  attackId: string;
  turnId: string;
  status: CombatRowStatus;
  attackerId: string;
  defenderId: string;
  from: string;
  to: string;
  attackerDice: number;
  attackerRolls: number[];
  defenderDice: number;
  declaredAt: number;
  defenseDeadlineAt: number;
  defenderRolls?: number[];
  attackerLosses?: number;
  defenderLosses?: number;
  territoryCaptured?: boolean;
  resolutionSource?: DefenseResolutionSource;
  /** Present only while `awaiting-occupation`. */
  minArmies?: number;
  maxArmies?: number;
}

export interface ProjectedMoveV2 {
  id: string;
  commandId: string;
  kind: GameEventV2Type;
  playerId?: string;
  sourceOffset: string;
  turnId?: string;
  attackId?: string;
  territoryId?: string;
  from?: string;
  to?: string;
  armies?: number;
  attackerRolls?: number[];
  defenderRolls?: number[];
  attackerLosses?: number;
  defenderLosses?: number;
  territoryCaptured?: boolean;
  resolutionSource?: DefenseResolutionSource;
  nextPlayerId?: string;
}

/** A compact demo feed: bounded so projection checkpoints stay O(1) in game length. */
export const MOVE_FEED_LIMIT_V2 = 40;

/** Singleton row keys for the zero-or-one collections. */
export const TURN_ROW_KEY = "turn";
export const COMBAT_ROW_KEY = "combat";

export interface ProjectionStateV2 {
  game: ProjectedGameV2;
  players: ProjectedPlayerV2[];
  hexes: ProjectedHexV2[];
  territories: ProjectedTerritoryV2[];
  continents: ProjectedContinentV2[];
  turn: ProjectedTurnV2 | null;
  combat: ProjectedCombatV2 | null;
  moves: ProjectedMoveV2[];
  /** Source offset (as a string) through which this projection is valid, or null. */
  sourceThroughOffset: string | null;
}

/**
 * Thrown when canonical history contradicts itself, mirroring the aggregate's
 * `AggregateIntegrityError`. The runtime treats a throwing reducer as a poison
 * event and halts the projection at the previous watermark, which is the right
 * outcome: a corrupt stream must not fold into a plausible-looking board.
 */
export class ProjectionIntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProjectionIntegrityError";
  }
}

export function initialProjectionV2(gameId = ""): ProjectionStateV2 {
  return {
    game: { id: gameId, status: "lobby", round: 0 },
    players: [],
    hexes: [],
    territories: [],
    continents: [],
    turn: null,
    combat: null,
    moves: [],
    sourceThroughOffset: null,
  };
}

function findPlayer(state: ProjectionStateV2, id: string): ProjectedPlayerV2 | undefined {
  return state.players.find((p) => p.id === id);
}

function findTerritory(state: ProjectionStateV2, id: string): ProjectedTerritoryV2 {
  const territory = state.territories.find((t) => t.id === id);
  if (!territory) throw new ProjectionIntegrityError(`unknown territory ${id}`);
  return territory;
}

/** Recompute the roster totals and continent controllers after any ownership change. */
function refreshDerivedTotals(state: ProjectionStateV2): void {
  for (const player of state.players) {
    const owned = state.territories.filter((t) => t.ownerId === player.id);
    player.territoryCount = owned.length;
    player.armyCount = owned.reduce((sum, t) => sum + t.armies, 0);
  }
  for (const continent of state.continents) {
    const owners = new Set(
      continent.territoryIds.map((id) => state.territories.find((t) => t.id === id)?.ownerId),
    );
    const [only] = [...owners];
    continent.controllerId = owners.size === 1 && only ? only : undefined;
  }
}

/**
 * The reinforcement a player receives when their turn begins — recomputed here
 * from projected ownership rather than copied from the aggregate. The continent
 * bonuses are a *snapshot*: completing a continent later in the same turn changes
 * nothing until the player's next reinforcement phase.
 */
function reinforcementFor(state: ProjectionStateV2, playerId: string): ReinforcementState {
  const base = baseReinforcement(state.territories.filter((t) => t.ownerId === playerId).length);
  const continents: ContinentBonus[] = state.continents
    .filter((continent) => continent.controllerId === playerId)
    .map((continent) => ({ continentId: continent.id, bonus: continent.reinforcementBonus }));
  const total = continents.reduce((sum, c) => sum + c.bonus, base);
  return { base, continents, total, remaining: total };
}

function beginTurn(state: ProjectionStateV2, playerId: string, turnId: string): void {
  const reinforcement = reinforcementFor(state, playerId);
  state.game.activePlayerId = playerId;
  state.game.phase = reinforcement.remaining > 0 ? "reinforce" : "attack";
  state.combat = null;
  state.turn = {
    id: TURN_ROW_KEY,
    turnId,
    round: state.game.round,
    playerId,
    phase: state.game.phase,
    reinforcement,
    reinforcementsPlaced: 0,
    attacksDeclared: 0,
    throwsResolved: 0,
    captures: 0,
    eliminations: 0,
  };
}

/** Turn identity, derived independently of the aggregate's `buildTurnIdV2`. */
function turnIdOf(round: number, playerId: string): string {
  return `round-${round}:${playerId}`;
}

function requirePendingCombat(state: ProjectionStateV2, attackId: string): ProjectedCombatV2 {
  const combat = state.combat;
  if (!combat || combat.attackId !== attackId || combat.status !== "awaiting-defense") {
    throw new ProjectionIntegrityError(
      `AttackResolved ${attackId} does not match a pending declaration`,
    );
  }
  return combat;
}

function applyEvent(state: ProjectionStateV2, event: GameEventV2): void {
  switch (event.type) {
    case "GameCreated": {
      state.game.id = event.gameId;
      state.game.hostPlayerId = event.hostPlayerId;
      state.game.ruleset = event.ruleset;
      state.game.mapVersion = event.mapVersion;
      state.game.generatorVersion = event.generatorVersion;
      state.game.mapSeed = event.mapSeed;
      state.players.push({
        id: event.hostPlayerId,
        name: event.hostName,
        color: event.hostColor,
        controller: event.hostController,
        eliminated: false,
        territoryCount: 0,
        armyCount: 0,
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
        territoryCount: 0,
        armyCount: 0,
      });
      break;
    }
    case "PlayerControllerChanged": {
      const player = state.players.find((candidate) => candidate.id === event.playerId);
      if (player) player.controller = event.controller;
      break;
    }
    case "GameStarted": {
      state.game.status = "playing";
      state.game.round = event.round;
      insertMapRows(state, event.map);
      const armiesById = new Map(event.initialTerritories.map((t) => [t.territoryId, t]));
      for (const territory of state.territories) {
        const initial = armiesById.get(territory.id);
        if (!initial) {
          throw new ProjectionIntegrityError(`GameStarted omits territory ${territory.id}`);
        }
        territory.ownerId = initial.ownerId;
        territory.armies = initial.armies;
      }
      refreshDerivedTotals(state);
      const first = event.turnOrder[0]!;
      beginTurn(state, first, turnIdOf(event.round, first));
      break;
    }
    case "ArmiesReinforced": {
      findTerritory(state, event.territoryId).armies += event.armies;
      if (state.turn) {
        state.turn.reinforcement.remaining -= event.armies;
        state.turn.reinforcementsPlaced += event.armies;
        if (state.turn.reinforcement.remaining <= 0) {
          state.game.phase = "attack";
          state.turn.phase = "attack";
        }
      }
      refreshDerivedTotals(state);
      break;
    }
    case "AttackDeclared": {
      state.combat = {
        id: COMBAT_ROW_KEY,
        attackId: event.attackId,
        turnId: event.turnId,
        status: "awaiting-defense",
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
      if (state.turn) state.turn.attacksDeclared += 1;
      break;
    }
    case "AttackResolved": {
      const combat = requirePendingCombat(state, event.attackId);
      assertResolutionMatches(combat, event);

      const from = findTerritory(state, event.from);
      const to = findTerritory(state, event.to);
      from.armies -= event.attackerLosses;
      to.armies -= event.defenderLosses;
      refreshDerivedTotals(state);

      if (state.turn) {
        state.turn.throwsResolved += 1;
        state.turn.latestDice = {
          attackId: event.attackId,
          from: event.from,
          to: event.to,
          attackerRolls: event.attackerRolls.slice(),
          defenderRolls: event.defenderRolls.slice(),
          attackerLosses: event.attackerLosses,
          defenderLosses: event.defenderLosses,
          territoryCaptured: event.territoryCaptured,
          resolutionSource: event.resolutionSource,
        };
      }

      if (!event.territoryCaptured) {
        state.combat = null;
        break;
      }
      state.combat = {
        ...combat,
        status: "awaiting-occupation",
        defenderRolls: event.defenderRolls.slice(),
        attackerLosses: event.attackerLosses,
        defenderLosses: event.defenderLosses,
        territoryCaptured: true,
        resolutionSource: event.resolutionSource,
        // A capture wins every pair it needed, so the attacker took no losses and
        // the declared dice count is always a legal minimum garrison.
        minArmies: combat.attackerDice,
        maxArmies: from.armies - 1,
      };
      break;
    }
    case "TerritoryOccupied": {
      const from = findTerritory(state, event.from);
      const to = findTerritory(state, event.to);
      from.armies -= event.armies;
      to.ownerId = event.playerId;
      to.armies = event.armies;
      refreshDerivedTotals(state);
      if (state.turn) state.turn.captures += 1;
      // The canonical projection clears combat immediately; a client may keep the
      // last resolved throw locally for its reveal animation (design spec §7.2).
      state.combat = null;
      break;
    }
    case "ArmiesFortified": {
      findTerritory(state, event.from).armies -= event.armies;
      findTerritory(state, event.to).armies += event.armies;
      state.game.phase = "fortify";
      if (state.turn) state.turn.phase = "fortify";
      refreshDerivedTotals(state);
      break;
    }
    case "PlayerEliminated": {
      const eliminated = findPlayer(state, event.playerId);
      if (eliminated) eliminated.eliminated = true;
      if (state.turn) state.turn.eliminations += 1;
      break;
    }
    case "TurnEnded": {
      state.game.round = event.round;
      beginTurn(state, event.nextPlayerId, turnIdOf(event.round, event.nextPlayerId));
      break;
    }
    case "GameWon": {
      state.game.status = "finished";
      state.game.winnerId = event.playerId;
      state.game.activePlayerId = undefined;
      state.game.phase = undefined;
      state.combat = null;
      if (state.turn) {
        state.turn.phase = undefined;
        state.turn.reinforcement = { base: 0, continents: [], total: 0, remaining: 0 };
      }
      break;
    }
  }
}

/**
 * The repeated attacker rolls in `AttackResolved` exist so a combat result is
 * self-contained in the move feed — not so consumers can trust them blindly.
 */
function assertResolutionMatches(
  combat: ProjectedCombatV2,
  event: Extract<GameEventV2, { type: "AttackResolved" }>,
): void {
  const sameRolls =
    combat.attackerRolls.length === event.attackerRolls.length &&
    combat.attackerRolls.every((roll, i) => roll === event.attackerRolls[i]);
  const { attackerLosses, defenderLosses } = compareRolls(event.attackerRolls, event.defenderRolls);
  if (
    !sameRolls ||
    combat.turnId !== event.turnId ||
    combat.attackerId !== event.attackerId ||
    combat.defenderId !== event.defenderId ||
    combat.from !== event.from ||
    combat.to !== event.to ||
    combat.defenderDice !== event.defenderRolls.length ||
    attackerLosses !== event.attackerLosses ||
    defenderLosses !== event.defenderLosses
  ) {
    throw new ProjectionIntegrityError(
      `AttackResolved ${event.attackId} contradicts its declaration`,
    );
  }
}

/** Static map rows, taken verbatim from the canonical `GameStarted` snapshot. */
function insertMapRows(state: ProjectionStateV2, map: GeneratedMap): void {
  state.hexes = map.tiles.map((tile) => ({
    id: tile.id,
    q: tile.q,
    r: tile.r,
    territoryId: tile.territoryId,
    terrain: tile.terrain,
  }));
  state.territories = map.territories.map((territory) => ({
    id: territory.id,
    name: territory.name,
    continentId: territory.continentId,
    armies: 0,
    hexIds: [...territory.hexIds],
    adjacentTerritoryIds: [...territory.adjacentTerritoryIds],
    labelAnchor: { ...territory.labelAnchor },
  }));
  state.continents = map.continents.map((continent) => ({
    id: continent.id,
    name: continent.name,
    territoryIds: [...continent.territoryIds],
    reinforcementBonus: continent.reinforcementBonus,
    palette: { ...continent.palette },
  }));
}

function movePlayerIdV2(event: GameEventV2): string | undefined {
  switch (event.type) {
    case "GameCreated":
      return event.hostPlayerId;
    case "GameStarted":
      return undefined;
    case "AttackDeclared":
      return event.attackerId;
    case "AttackResolved":
      return event.defenderId;
    default:
      return "playerId" in event ? event.playerId : undefined;
  }
}

function moveDetail(event: GameEventV2): Partial<ProjectedMoveV2> {
  switch (event.type) {
    case "ArmiesReinforced":
      return { turnId: event.turnId, territoryId: event.territoryId, armies: event.armies };
    case "AttackDeclared":
      return {
        turnId: event.turnId,
        attackId: event.attackId,
        from: event.from,
        to: event.to,
        attackerRolls: event.attackerRolls.slice(),
      };
    case "AttackResolved":
      return {
        turnId: event.turnId,
        attackId: event.attackId,
        from: event.from,
        to: event.to,
        attackerRolls: event.attackerRolls.slice(),
        defenderRolls: event.defenderRolls.slice(),
        attackerLosses: event.attackerLosses,
        defenderLosses: event.defenderLosses,
        territoryCaptured: event.territoryCaptured,
        resolutionSource: event.resolutionSource,
      };
    case "TerritoryOccupied":
      return {
        turnId: event.turnId,
        attackId: event.attackId,
        from: event.from,
        to: event.to,
        armies: event.armies,
      };
    case "ArmiesFortified":
      return { turnId: event.turnId, from: event.from, to: event.to, armies: event.armies };
    case "TurnEnded":
      return { turnId: event.turnId, nextPlayerId: event.nextPlayerId };
    default:
      return {};
  }
}

/**
 * Pure projection step: apply one event observed at `sourceOffset` and return a
 * new projection state (the previous state is not mutated).
 */
export function projectEventV2(
  previous: ProjectionStateV2,
  event: GameEventV2,
  sourceOffset: string,
): ProjectionStateV2 {
  const state = structuredClone(previous);
  applyEvent(state, event);

  state.moves.push({
    id: sourceOffset,
    commandId: event.commandId,
    kind: event.type,
    playerId: movePlayerIdV2(event),
    sourceOffset,
    ...moveDetail(event),
  });
  if (state.moves.length > MOVE_FEED_LIMIT_V2) {
    state.moves.splice(0, state.moves.length - MOVE_FEED_LIMIT_V2);
  }
  state.sourceThroughOffset = sourceOffset;
  return state;
}

export function projectEventsV2(events: readonly GameEventV2[]): ProjectionStateV2 {
  let state = initialProjectionV2();
  for (let index = 0; index < events.length; index += 1) {
    state = projectEventV2(state, events[index]!, String(index));
  }
  return state;
}

// ---------------------------------------------------------------------------
// Aggregate / projection equivalence
// ---------------------------------------------------------------------------

/**
 * Normalised, order-independent view of the logical board both reducers agree
 * on — including the map they each read from `GameStarted`, the current turn's
 * reinforcement accounting, and the open combat interrupt.
 *
 * Deliberately excluded: presentation-only counters (`attacksDeclared`, dice
 * replays), the bounded move feed, and anything a client animates. Those are the
 * projection's job alone, so requiring the aggregate to mirror them would make
 * the equivalence check test the wrong thing.
 */
export interface BoardViewV2 {
  status: GameStatusV2;
  phase?: GamePhaseV2;
  activePlayerId?: string;
  turnId?: string;
  round: number;
  winnerId?: string;
  players: Array<{ id: string; controller: PlayerController; eliminated: boolean }>;
  territories: Array<{
    id: string;
    ownerId?: string;
    armies: number;
    continentId: string;
    adjacentTerritoryIds: string[];
  }>;
  continents: Array<{
    id: string;
    territoryIds: string[];
    reinforcementBonus: number;
    controllerId?: string;
  }>;
  reinforcement: ReinforcementState;
  pending?: PendingInteraction;
}

const byId = (a: { id: string }, b: { id: string }): number => a.id.localeCompare(b.id);

const EMPTY_REINFORCEMENT: ReinforcementState = {
  base: 0,
  continents: [],
  total: 0,
  remaining: 0,
};

/** The aggregate-side shape of {@link BoardViewV2}. */
export interface AggregateViewSourceV2 {
  status: GameStatusV2;
  phase?: GamePhaseV2;
  activePlayerId?: string;
  round: number;
  winnerId?: string;
  players: ReadonlyArray<{ id: string; controller: PlayerController; eliminated: boolean }>;
  map?: GeneratedMap;
  territories: Record<string, { id: string; ownerId?: string; armies: number }>;
  reinforcement: ReinforcementState;
  pendingInteraction?: PendingInteraction;
  attacks: Record<string, AttackRecord>;
}

function continentControllerOf(
  territoryIds: readonly string[],
  ownerOf: (id: string) => string | undefined,
): string | undefined {
  const owners = new Set(territoryIds.map(ownerOf));
  const [only] = [...owners];
  return owners.size === 1 && only ? only : undefined;
}

export function aggregateBoardViewV2(state: AggregateViewSourceV2): BoardViewV2 {
  const ownerOf = (id: string): string | undefined => state.territories[id]?.ownerId;
  return {
    status: state.status,
    phase: state.phase,
    activePlayerId: state.activePlayerId,
    turnId:
      state.status === "playing" && state.activePlayerId
        ? turnIdOf(state.round, state.activePlayerId)
        : undefined,
    round: state.round,
    winnerId: state.winnerId,
    players: state.players
      .map((p) => ({ id: p.id, controller: p.controller, eliminated: p.eliminated }))
      .toSorted(byId),
    territories: (state.map?.territories ?? [])
      .map((territory) => ({
        id: territory.id,
        ownerId: ownerOf(territory.id),
        armies: state.territories[territory.id]?.armies ?? 0,
        continentId: territory.continentId,
        adjacentTerritoryIds: [...territory.adjacentTerritoryIds],
      }))
      .toSorted(byId),
    continents: (state.map?.continents ?? [])
      .map((continent) => ({
        id: continent.id,
        territoryIds: [...continent.territoryIds],
        reinforcementBonus: continent.reinforcementBonus,
        controllerId: continentControllerOf(continent.territoryIds, ownerOf),
      }))
      .toSorted(byId),
    reinforcement: state.reinforcement,
    pending: state.pendingInteraction,
  };
}

/**
 * The same view rebuilt from projected rows. The pending interaction is
 * reconstructed from the `combat` row, which is what proves the row carries
 * everything the interrupt needs — including the occupation bounds.
 */
export function projectionBoardViewV2(state: ProjectionStateV2): BoardViewV2 {
  const combat = state.combat;
  const pending: PendingInteraction | undefined =
    combat?.status === "awaiting-defense"
      ? {
          type: "defense",
          attackId: combat.attackId,
          turnId: combat.turnId,
          attackerId: combat.attackerId,
          defenderId: combat.defenderId,
          from: combat.from,
          to: combat.to,
          attackerDice: combat.attackerDice,
          attackerRolls: combat.attackerRolls.slice(),
          defenderDice: combat.defenderDice,
          declaredAt: combat.declaredAt,
          defenseDeadlineAt: combat.defenseDeadlineAt,
        }
      : combat?.status === "awaiting-occupation"
        ? {
            type: "occupation",
            attackId: combat.attackId,
            turnId: combat.turnId,
            playerId: combat.attackerId,
            from: combat.from,
            to: combat.to,
            minArmies: combat.minArmies!,
            maxArmies: combat.maxArmies!,
          }
        : undefined;

  return {
    status: state.game.status,
    phase: state.game.phase,
    activePlayerId: state.game.activePlayerId,
    turnId: state.game.status === "playing" && state.turn ? state.turn.turnId : undefined,
    round: state.game.round,
    winnerId: state.game.winnerId,
    players: state.players
      .map((p) => ({ id: p.id, controller: p.controller, eliminated: p.eliminated }))
      .toSorted(byId),
    territories: state.territories
      .map((t) => ({
        id: t.id,
        ownerId: t.ownerId,
        armies: t.armies,
        continentId: t.continentId,
        adjacentTerritoryIds: [...t.adjacentTerritoryIds],
      }))
      .toSorted(byId),
    continents: state.continents
      .map((c) => ({
        id: c.id,
        territoryIds: [...c.territoryIds],
        reinforcementBonus: c.reinforcementBonus,
        controllerId: c.controllerId,
      }))
      .toSorted(byId),
    reinforcement: state.turn?.reinforcement ?? EMPTY_REINFORCEMENT,
    pending,
  };
}

/** True when the aggregate fold and the board projection agree on the logical board. */
export function boardsEqualV2(a: BoardViewV2, b: BoardViewV2): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
