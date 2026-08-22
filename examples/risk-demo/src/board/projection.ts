/* oxlint-disable typescript/no-unsafe-type-assertion, typescript/consistent-return, typescript/no-unnecessary-type-conversion, unicorn/consistent-function-scoping, effecttsgo/extends-native-error -- Remaining assertions are confined to caller-owned generic codecs, framework-generated structural types, or test-owned fixtures; native errors are synchronous Promise/domain exceptions rather than Effect failure-channel values, and exhaustive switches are protected by closed unions. */
/**
 * Pure `Hex Domination` board projection reducer — the query-shaped read model.
 *
 * This is deliberately a *second, independent* reduction over the same
 * canonical events: nothing here imports `foldAggregate`, and the equivalence
 * check in `projection.test.ts` is only meaningful because of that. If either
 * reducer drifts, {@link boardsEqual} catches it.
 *
 * Three properties are load-bearing:
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
  GamePhase,
  GameStatus,
  PendingInteraction,
  ReinforcementState,
} from "../domain/aggregate.ts";
import type {
  DefenseResolutionSource,
  GameEvent,
  GameEventType,
  PlayerController,
} from "../domain/events.ts";
import type { ContinentPalette, GeneratedMap, Terrain } from "../domain/map.ts";
import { baseReinforcement } from "../domain/map.ts";
import type { Axial } from "../domain/hex.ts";
import { compareRolls } from "../domain/dice.ts";

export interface ProjectedGame {
  id: string;
  hostPlayerId?: string;
  status: GameStatus;
  mapVersion?: string;
  generatorVersion?: string;
  mapSeed?: string;
  round: number;
  activePlayerId?: string;
  phase?: GamePhase;
  winnerId?: string;
}

export interface ProjectedPlayer {
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
export interface ProjectedHex {
  id: string;
  q: number;
  r: number;
  territoryId: string;
  terrain: Terrain;
}

export interface ProjectedTerritory {
  id: string;
  name: string;
  continentId: string;
  ownerId?: string;
  armies: number;
  hexIds: string[];
  adjacentTerritoryIds: string[];
  labelAnchor: Axial;
}

export interface ProjectedContinent {
  id: string;
  name: string;
  territoryIds: string[];
  reinforcementBonus: number;
  /** The player who currently owns every member country, if any. */
  controllerId?: string;
  palette: ContinentPalette;
}

/** The latest completed throw, kept on the turn row so the rail can replay it. */
export interface ProjectedDice {
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
export interface ProjectedTurn {
  id: string;
  turnId: string;
  round: number;
  playerId: string;
  phase?: GamePhase;
  reinforcement: ReinforcementState;
  /** Armies placed so far this turn; `total - remaining` made explicit. */
  reinforcementsPlaced: number;
  attacksDeclared: number;
  throwsResolved: number;
  captures: number;
  eliminations: number;
  latestDice?: ProjectedDice;
}

export type CombatRowStatus = "awaiting-defense" | "awaiting-occupation";

/** Zero or one pending-combat presentation row. */
export interface ProjectedCombat {
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

export interface ProjectedMove {
  id: string;
  commandId: string;
  kind: GameEventType;
  playerId?: string;
  /** Seat name carried by roster moves, so a departure survives its own row delete. */
  name?: string;
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
export const MOVE_FEED_LIMIT = 40;

/** Singleton row keys for the zero-or-one collections. */
export const TURN_ROW_KEY = "turn";
export const COMBAT_ROW_KEY = "combat";

export interface ProjectionState {
  game: ProjectedGame;
  players: ProjectedPlayer[];
  hexes: ProjectedHex[];
  territories: ProjectedTerritory[];
  continents: ProjectedContinent[];
  turn: ProjectedTurn | null;
  combat: ProjectedCombat | null;
  moves: ProjectedMove[];
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

export function initialProjection(gameId = ""): ProjectionState {
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

function findPlayer(state: ProjectionState, id: string): ProjectedPlayer | undefined {
  return state.players.find((p) => p.id === id);
}

function findTerritory(state: ProjectionState, id: string): ProjectedTerritory {
  const territory = state.territories.find((t) => t.id === id);
  if (!territory) throw new ProjectionIntegrityError(`unknown territory ${id}`);
  return territory;
}

/** Recompute the roster totals and continent controllers after any ownership change. */
function refreshDerivedTotals(state: ProjectionState): void {
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
function reinforcementFor(state: ProjectionState, playerId: string): ReinforcementState {
  const base = baseReinforcement(state.territories.filter((t) => t.ownerId === playerId).length);
  const continents: ContinentBonus[] = state.continents
    .filter((continent) => continent.controllerId === playerId)
    .map((continent) => ({ continentId: continent.id, bonus: continent.reinforcementBonus }));
  const total = continents.reduce((sum, c) => sum + c.bonus, base);
  return { base, continents, total, remaining: total };
}

function beginTurn(state: ProjectionState, playerId: string, turnId: string): void {
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

/** Turn identity, derived independently of the aggregate's `buildTurnId`. */
function turnIdOf(round: number, playerId: string): string {
  return `round-${round}:${playerId}`;
}

function requirePendingCombat(state: ProjectionState, attackId: string): ProjectedCombat {
  const combat = state.combat;
  if (!combat || combat.attackId !== attackId || combat.status !== "awaiting-defense") {
    throw new ProjectionIntegrityError(
      `AttackResolved ${attackId} does not match a pending declaration`,
    );
  }
  return combat;
}

function applyEvent(state: ProjectionState, event: GameEvent): void {
  switch (event.type) {
    case "GameCreated": {
      state.game.id = event.gameId;
      state.game.hostPlayerId = event.hostPlayerId;
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
    case "PlayerRenamed": {
      const player = state.players.find((candidate) => candidate.id === event.playerId);
      if (player) player.name = event.name;
      break;
    }
    case "PlayerLeft": {
      // Dropping the row from `players` is what makes the adapter emit a delete
      // for that seat, the same mechanism the zero-or-one turn/combat rows use.
      state.players = state.players.filter((candidate) => candidate.id !== event.playerId);
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
      // last resolved throw locally for its reveal animation.
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
  combat: ProjectedCombat,
  event: Extract<GameEvent, { type: "AttackResolved" }>,
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
function insertMapRows(state: ProjectionState, map: GeneratedMap): void {
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

function movePlayerId(event: GameEvent): string | undefined {
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

/**
 * `before` is the projection as it stood *prior* to this event, which is the only
 * place a departing seat's name still exists — `PlayerLeft` deletes the row that
 * carries it, so the feed has to capture it on the way past.
 */
function moveDetail(event: GameEvent, before: ProjectionState): Partial<ProjectedMove> {
  switch (event.type) {
    case "PlayerRenamed":
      return { name: event.name };
    case "PlayerLeft": {
      const departing = before.players.find((player) => player.id === event.playerId);
      return departing ? { name: departing.name } : {};
    }
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
 *
 * `ordinal` is the event's 0-based position in canonical history and is what
 * gives its move row a primary key. A source offset cannot: a delivery boundary
 * carries one offset for every message in it, so several events of one command
 * share a position, and keying moves by that position would collide — the
 * snapshot would keep every move while the row set kept only the last, and the
 * browser mirror would silently disagree with the board it mirrors. The ordinal
 * is durable (it is recorded in the projection's own checkpoint row) and depends
 * only on canonical order, so replay and incremental catch-up produce identical
 * keys however the reader happened to batch. `sourceOffset` stays on the row as
 * the coarse causal watermark it always was.
 */
export function projectEvent(
  previous: ProjectionState,
  event: GameEvent,
  sourceOffset: string,
  ordinal: number,
): ProjectionState {
  const state = structuredClone(previous);
  applyEvent(state, event);

  state.moves.push({
    id: moveId(ordinal),
    commandId: event.commandId,
    kind: event.type,
    playerId: movePlayerId(event),
    sourceOffset,
    ...moveDetail(event, previous),
  });
  if (state.moves.length > MOVE_FEED_LIMIT) {
    state.moves.splice(0, state.moves.length - MOVE_FEED_LIMIT);
  }
  state.sourceThroughOffset = sourceOffset;
  return state;
}

/** Lexicographically ordered move key, so ids sort in canonical event order. */
export function moveId(ordinal: number): string {
  return String(ordinal).padStart(16, "0");
}

export function projectEvents(events: readonly GameEvent[]): ProjectionState {
  let state = initialProjection();
  for (let index = 0; index < events.length; index += 1) {
    state = projectEvent(state, events[index]!, String(index), index);
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
export interface BoardView {
  status: GameStatus;
  phase?: GamePhase;
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

/** The aggregate-side shape of {@link BoardView}. */
export interface AggregateViewSource {
  status: GameStatus;
  phase?: GamePhase;
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

export function aggregateBoardView(state: AggregateViewSource): BoardView {
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
export function projectionBoardView(state: ProjectionState): BoardView {
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
export function boardsEqual(a: BoardView, b: BoardView): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}
