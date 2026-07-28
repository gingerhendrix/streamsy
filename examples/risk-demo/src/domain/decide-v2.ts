/**
 * Pure `risk-demo-v2` command decision + validation.
 *
 * `decideV2` evaluates a *fresh* command against a folded aggregate and returns
 * either the canonical events to append or a stable rejection. It is the only
 * place randomness and the clock are consumed, and only on the path that would
 * actually commit — the command log dedupes by `commandId` first, so a retry of
 * an accepted command never re-rolls and never re-reads the clock.
 *
 * The two-stage combat adds one shape the v1 kernel does not have: a *pending
 * interaction* that suspends the ordinary command set. While a defence is pending
 * only the named defender's `roll-defense` (or the internal timeout resolver) is
 * legal; while an occupation is pending only the attacker's `occupy-territory`
 * is. Everything else is rejected with `PENDING_DEFENSE`/`PENDING_OCCUPATION`
 * rather than quietly interleaving with an unresolved throw.
 *
 * Map generation is injected as {@link DecideContextV2.planStart} rather than
 * imported, so this module stays a pure function of its inputs and the service
 * keeps control of *when* the generator runs (once, after dedup, before append).
 */

import type {
  CommandV2,
  DelegateAgentSeatCommandV2,
  DeclareAttackCommandV2,
  FortifyCommandV2,
  JoinGameCommandV2,
  OccupyTerritoryCommandV2,
  ReinforceCommandV2,
  ResolveDefenseTimeoutCommandV2,
  RiskErrorCodeV2,
  RollDefenseCommandV2,
  SkipFortificationsCommandV2,
  StartGameCommandV2,
} from "./commands-v2.ts";
import type { AggregateStateV2 } from "./aggregate-v2.ts";
import { assignPlayerColorV2 } from "./colors-v2.ts";
import {
  currentTurnIdV2,
  friendlyReachable,
  nextTurnV2,
  ownedByV2,
  playerV2,
} from "./aggregate-v2.ts";
import { compareRolls, legalDefenderDice, maxAttackerDice, rollSorted } from "./dice-v2.ts";
import type { DefenseResolutionSource, GameEventV2 } from "./events-v2.ts";
import { MapGenerationError } from "./hex-generator.ts";
import {
  GENERATOR_VERSION_V2,
  MAP_VERSION_V2,
  RULES_V2,
  RULESET_V2,
  areAdjacentV2,
  isTerritoryV2,
} from "./map-v2.ts";
import type { Rng } from "./rng.ts";
import type { GameStartPlan } from "./setup-v2.ts";
import { planGameStart } from "./setup-v2.ts";

export interface DecisionErrorV2 {
  code: RiskErrorCodeV2;
  message: string;
  currentTurnId?: string;
  attackId?: string;
}

export type DecisionV2 =
  | { status: "accepted"; events: GameEventV2[] }
  | { status: "rejected"; error: DecisionErrorV2 };

export interface DecideContextV2 {
  /** Authoritative randomness. Consumed exactly once per accepted throw. */
  rng: Rng;
  /** Injected clock, recorded into `declaredAt`/`defenseDeadlineAt` as facts. */
  now: () => number;
  /** Defence interrupt window; injectable so tests need not wait 15 real seconds. */
  defenseTimeoutMs?: number;
  /** Map generation, injected so this module stays pure. */
  planStart?: (request: { mapSeed: string; playerIds: readonly string[] }) => GameStartPlan;
}

function reject(
  code: RiskErrorCodeV2,
  message: string,
  extra?: { currentTurnId?: string; attackId?: string },
): DecisionV2 {
  return { status: "rejected", error: { code, message, ...extra } };
}

function accept(...events: GameEventV2[]): DecisionV2 {
  return { status: "accepted", events };
}

function territoryLabel(state: AggregateStateV2, territoryId: string): string {
  const name = state.index?.territoryById.get(territoryId)?.name;
  return name ? `${name} (${territoryId})` : territoryId;
}

function playerLabel(state: AggregateStateV2, playerId: string | undefined): string {
  if (!playerId) return "the current player";
  const player = playerV2(state, playerId);
  return player ? `${player.name} (${player.id})` : playerId;
}

interface TurnScopedCommand {
  turnId: string;
  playerId: string;
}

/**
 * Shared preconditions for every *in-turn* play command: the game is running, the
 * caller is the active player, the observed turn is still current, and no combat
 * interrupt is open. `roll-defense` deliberately does not go through here — it is
 * the one legal out-of-turn command.
 */
function ensureTurn(
  state: AggregateStateV2,
  command: TurnScopedCommand,
): DecisionErrorV2 | undefined {
  if (state.status === "finished") {
    return { code: "GAME_FINISHED", message: "The game has already finished." };
  }
  if (state.status !== "playing") {
    return { code: "GAME_NOT_STARTED", message: "The game has not started." };
  }
  const pending = state.pendingInteraction;
  if (pending?.type === "defense") {
    return {
      code: "PENDING_DEFENSE",
      message: `Attack ${pending.attackId} is waiting for ${playerLabel(state, pending.defenderId)} to roll defense.`,
      attackId: pending.attackId,
    };
  }
  if (pending?.type === "occupation") {
    return {
      code: "PENDING_OCCUPATION",
      message: `${territoryLabel(state, pending.to)} was captured and must be occupied before any other move.`,
      attackId: pending.attackId,
    };
  }
  if (command.playerId !== state.activePlayerId) {
    return {
      code: "NOT_YOUR_TURN",
      message: `It is ${playerLabel(state, state.activePlayerId)}'s turn, not ${playerLabel(state, command.playerId)}'s.`,
    };
  }
  const turnId = currentTurnIdV2(state);
  if (command.turnId !== turnId) {
    return {
      code: "STALE_TURN",
      message: `Turn ${command.turnId} is stale; the current turn is ${turnId}.`,
      currentTurnId: turnId,
    };
  }
  return undefined;
}

function decideJoin(state: AggregateStateV2, command: JoinGameCommandV2): DecisionV2 {
  if (!state.gameId) return reject("GAME_NOT_FOUND", "No game to join.");
  if (state.status !== "lobby") {
    return reject("GAME_ALREADY_STARTED", "Cannot join a game in progress.");
  }
  if (state.players.length >= RULES_V2.maxPlayers) {
    return reject("TOO_MANY_PLAYERS", `A game seats at most ${RULES_V2.maxPlayers} players.`);
  }
  if (state.players.some((p) => p.id === command.playerId)) {
    return reject("PLAYER_ID_TAKEN", "That player id is already in the game.");
  }
  return accept({
    type: "PlayerJoined",
    playerId: command.playerId,
    name: command.name,
    // Assigned here — after dedupe and fold — so simultaneous joins can never
    // seat two players on one colour. A free requested colour is honoured; a
    // taken or absent one is replaced by the first available palette colour.
    color: assignPlayerColorV2(
      state.players.map((player) => player.color),
      command.color,
    ),
    controller: command.controller,
    commandId: command.commandId,
  });
}

function decideDelegateAgent(
  state: AggregateStateV2,
  command: DelegateAgentSeatCommandV2,
): DecisionV2 {
  if (!state.gameId) return reject("GAME_NOT_FOUND", "No game to delegate.");
  if (state.status !== "lobby") {
    return reject("GAME_ALREADY_STARTED", "Agent seats must be delegated before the game starts.");
  }
  if (!state.players.some((player) => player.id === command.playerId)) {
    return reject("UNKNOWN_PLAYER", "That player is not part of this game.");
  }
  return accept({
    type: "PlayerControllerChanged",
    playerId: command.playerId,
    controller: "external-agent",
    commandId: command.commandId,
  });
}

/**
 * Generate the board and the whole starting allocation, once.
 *
 * The generator runs here — after the command log has deduped `commandId` and
 * before the canonical append — so randomness is consumed only on the path that
 * commits. If the append then loses its source-head CAS, the log refolds and this
 * function is reached again only for a game that is now `playing`, so it rejects
 * as already started instead of generating a second, different board.
 */
function decideStart(
  state: AggregateStateV2,
  command: StartGameCommandV2,
  ctx: DecideContextV2,
): DecisionV2 {
  if (!state.gameId) return reject("GAME_NOT_FOUND", "No game to start.");
  if (state.status !== "lobby") {
    return reject("GAME_ALREADY_STARTED", "The game has already started.");
  }
  if (state.players.length < RULES_V2.minPlayers) {
    return reject("NOT_ENOUGH_PLAYERS", `A game needs at least ${RULES_V2.minPlayers} players.`);
  }
  if (!state.mapSeed) {
    return reject("MAP_GENERATION_FAILED", "The game was created without a map seed.");
  }

  let plan: GameStartPlan;
  try {
    plan = (ctx.planStart ?? planGameStart)({
      mapSeed: state.mapSeed,
      playerIds: state.players.map((p) => p.id),
    });
  } catch (error) {
    // A generation cap failure rejects game start rather than silently switching
    // algorithms.
    if (error instanceof MapGenerationError) {
      return reject("MAP_GENERATION_FAILED", error.message);
    }
    throw error;
  }

  return accept({
    type: "GameStarted",
    map: plan.map,
    turnOrder: plan.turnOrder.slice(),
    initialTerritories: plan.initialTerritories.map((t) => ({ ...t })),
    round: 1,
    commandId: command.commandId,
  });
}

function decideReinforce(state: AggregateStateV2, command: ReinforceCommandV2): DecisionV2 {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase !== "reinforce") {
    return reject("INVALID_PHASE", "Reinforcements can only be placed in the reinforce phase.");
  }

  const remaining = state.reinforcement.remaining;
  if (command.placements.length === 0) {
    return reject("INSUFFICIENT_ARMIES", `Place all ${remaining} reinforcements in one command.`);
  }

  const seen = new Set<string>();
  let total = 0;
  for (const placement of command.placements) {
    if (!state.index || !isTerritoryV2(state.index, placement.territoryId)) {
      return reject("UNKNOWN_TERRITORY", `Unknown territory: ${placement.territoryId}.`);
    }
    if (seen.has(placement.territoryId)) {
      return reject(
        "ILLEGAL_ACTION",
        `${territoryLabel(state, placement.territoryId)} appears more than once in the reinforcement allocation.`,
      );
    }
    seen.add(placement.territoryId);
    if (state.territories[placement.territoryId]?.ownerId !== command.playerId) {
      return reject(
        "ILLEGAL_ACTION",
        `${territoryLabel(state, placement.territoryId)} is owned by ${playerLabel(state, state.territories[placement.territoryId]?.ownerId)}, so ${playerLabel(state, command.playerId)} cannot reinforce it.`,
      );
    }
    if (!Number.isInteger(placement.armies) || placement.armies < 1) {
      return reject(
        "INSUFFICIENT_ARMIES",
        `Every reinforcement placement must contain a positive whole number of armies.`,
      );
    }
    total += placement.armies;
  }

  if (total !== remaining) {
    return reject(
      "INSUFFICIENT_ARMIES",
      `Place all ${remaining} reinforcements in one command; the submitted allocation contains ${total}.`,
    );
  }

  return {
    status: "accepted",
    events: command.placements.map((placement) => ({
      type: "ArmiesReinforced",
      turnId: command.turnId,
      playerId: command.playerId,
      territoryId: placement.territoryId,
      armies: placement.armies,
      commandId: command.commandId,
    })),
  };
}

/**
 * Stage one of a throw. The attacker's dice are rolled and recorded now, and the
 * defence interrupt opens with a canonical deadline; the defender's dice are not
 * rolled until something resolves the interrupt.
 */
function decideDeclareAttack(
  state: AggregateStateV2,
  command: DeclareAttackCommandV2,
  ctx: DecideContextV2,
): DecisionV2 {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase !== "attack") {
    return reject("INVALID_PHASE", "Attacks are only allowed in the attack phase.");
  }
  if (
    !state.index ||
    !isTerritoryV2(state.index, command.from) ||
    !isTerritoryV2(state.index, command.to)
  ) {
    return reject("UNKNOWN_TERRITORY", "Unknown attack territory.");
  }
  const from = state.territories[command.from]!;
  const to = state.territories[command.to]!;
  if (from.ownerId !== command.playerId) {
    return reject(
      "ILLEGAL_ACTION",
      `${territoryLabel(state, command.from)} is owned by ${playerLabel(state, from.ownerId)} and cannot be used to attack by ${playerLabel(state, command.playerId)}.`,
    );
  }
  if (to.ownerId === command.playerId) {
    return reject(
      "ILLEGAL_ACTION",
      `${territoryLabel(state, command.to)} is already yours and cannot be attacked from ${territoryLabel(state, command.from)}.`,
    );
  }
  if (!areAdjacentV2(state.index, command.from, command.to)) {
    return reject(
      "NOT_ADJACENT",
      `${territoryLabel(state, command.to)} cannot be attacked from ${territoryLabel(state, command.from)} because they are not neighbours.`,
    );
  }
  if (
    !Number.isInteger(command.attackerDice) ||
    command.attackerDice < 1 ||
    command.attackerDice > RULES_V2.maxAttackerDice
  ) {
    return reject("ILLEGAL_ACTION", `Attacker dice must be 1..${RULES_V2.maxAttackerDice}.`);
  }
  if (command.attackerDice > maxAttackerDice(from.armies)) {
    return reject(
      "INSUFFICIENT_ARMIES",
      `${territoryLabel(state, command.from)} has ${from.armies} armies, so it cannot attack with ${command.attackerDice} dice while leaving one army behind.`,
    );
  }

  const declaredAt = ctx.now();
  return accept({
    type: "AttackDeclared",
    // The declaration's commandId *is* the attack identity, so a defender or a
    // timer can name one attack unambiguously without a second id scheme.
    attackId: command.commandId,
    turnId: command.turnId,
    attackerId: command.playerId,
    defenderId: to.ownerId!,
    from: command.from,
    to: command.to,
    attackerDice: command.attackerDice,
    attackerRolls: rollSorted(ctx.rng, command.attackerDice),
    defenderDice: legalDefenderDice(to.armies),
    declaredAt,
    defenseDeadlineAt: declaredAt + (ctx.defenseTimeoutMs ?? RULES_V2.defenseTimeoutMs),
    commandId: command.commandId,
  });
}

/**
 * Shared resolution for all defence paths (human, bot, external agent, and the
 * internal timeout job). Randomness is consumed only after this has confirmed,
 * against a fresh fold, that the named attack is still the pending one — so a
 * CAS-race loser refolds and is rejected without ever having committed a roll.
 */
function resolveDefense(
  state: AggregateStateV2,
  input: {
    commandId: string;
    turnId: string;
    attackId: string;
    /** Absent for the timeout resolver, which owns the post-deadline window. */
    playerId?: string;
    source: DefenseResolutionSource;
  },
  ctx: DecideContextV2,
): DecisionV2 {
  if (state.status === "finished") {
    return reject("GAME_FINISHED", "The game has already finished.");
  }
  if (state.status !== "playing") {
    return reject("GAME_NOT_STARTED", "The game has not started.");
  }

  const pending = state.pendingInteraction;
  if (!pending || pending.type !== "defense" || pending.attackId !== input.attackId) {
    // Distinguish "you are too late" from "that attack never existed", so a
    // duplicate timer delivery and a genuine mismatch are not the same signal.
    const known = state.attacks[input.attackId];
    if (known && known.status !== "awaiting-defense") {
      return reject("ATTACK_ALREADY_RESOLVED", "That attack has already been resolved.", {
        attackId: input.attackId,
      });
    }
    return reject("ATTACK_ID_MISMATCH", "No pending attack with that id.", {
      attackId: input.attackId,
    });
  }
  if (input.playerId !== undefined && input.playerId !== pending.defenderId) {
    return reject("NOT_DEFENDING_PLAYER", "Only the defending player may roll.");
  }
  if (input.turnId !== pending.turnId) {
    return reject("STALE_TURN", "The observed turn is no longer active.", {
      currentTurnId: currentTurnIdV2(state),
    });
  }
  // A player command that arrives after the canonical deadline loses to the
  // timeout resolver, which owns resolution from that instant on.
  if (input.source !== "timeout" && ctx.now() > pending.defenseDeadlineAt) {
    return reject("DEFENSE_DEADLINE_EXPIRED", "The defence window has closed.", {
      attackId: input.attackId,
    });
  }

  const defenderRolls = rollSorted(ctx.rng, pending.defenderDice);
  const { attackerLosses, defenderLosses } = compareRolls(pending.attackerRolls, defenderRolls);
  const defendingArmies = state.territories[pending.to]!.armies;

  return accept({
    type: "AttackResolved",
    attackId: pending.attackId,
    turnId: pending.turnId,
    attackerId: pending.attackerId,
    defenderId: pending.defenderId,
    from: pending.from,
    to: pending.to,
    attackerRolls: pending.attackerRolls.slice(),
    defenderRolls,
    attackerLosses,
    defenderLosses,
    territoryCaptured: defendingArmies - defenderLosses <= 0,
    resolutionSource: input.source,
    commandId: input.commandId,
  });
}

/**
 * A player-submitted roll. The recorded {@link DefenseResolutionSource} is
 * derived from the defending seat's canonical controller — declared once in
 * `GameCreated`/`PlayerJoined` — rather than from anything the client sends, so
 * bot/agent attribution can never be spoofed by a browser or vice versa.
 */
function decideRollDefense(
  state: AggregateStateV2,
  command: RollDefenseCommandV2,
  ctx: DecideContextV2,
): DecisionV2 {
  const controller = playerV2(state, command.playerId)?.controller;
  const source: DefenseResolutionSource =
    controller === "bot" ? "bot" : controller === "external-agent" ? "agent" : "human";
  return resolveDefense(
    state,
    {
      commandId: command.commandId,
      turnId: command.turnId,
      attackId: command.attackId,
      playerId: command.playerId,
      source,
    },
    ctx,
  );
}

function decideResolveTimeout(
  state: AggregateStateV2,
  command: ResolveDefenseTimeoutCommandV2,
  ctx: DecideContextV2,
): DecisionV2 {
  return resolveDefense(
    state,
    {
      commandId: command.commandId,
      turnId: command.turnId,
      attackId: command.attackId,
      source: "timeout",
    },
    ctx,
  );
}

/**
 * The attacker's required move into a captured country. Elimination and victory
 * are evaluated here and appended in the same atomic batch, so no projected board
 * ever shows a conquered game before the garrison has been chosen.
 */
function decideOccupy(state: AggregateStateV2, command: OccupyTerritoryCommandV2): DecisionV2 {
  if (state.status === "finished") {
    return reject("GAME_FINISHED", "The game has already finished.");
  }
  if (state.status !== "playing") {
    return reject("GAME_NOT_STARTED", "The game has not started.");
  }
  const pending = state.pendingInteraction;
  if (!pending || pending.type !== "occupation") {
    if (pending?.type === "defense") {
      return reject("PENDING_DEFENSE", "An attack is waiting for the defender to roll.", {
        attackId: pending.attackId,
      });
    }
    return reject("ATTACK_ID_MISMATCH", "There is no country waiting to be occupied.", {
      attackId: command.attackId,
    });
  }
  if (pending.attackId !== command.attackId) {
    return reject("ATTACK_ID_MISMATCH", "That attack is not the one awaiting occupation.", {
      attackId: pending.attackId,
    });
  }
  if (command.playerId !== pending.playerId) {
    return reject("NOT_YOUR_TURN", "Only the attacking player may occupy.");
  }
  if (command.turnId !== pending.turnId) {
    return reject("STALE_TURN", "The observed turn is no longer active.", {
      currentTurnId: currentTurnIdV2(state),
    });
  }
  if (
    !Number.isInteger(command.armies) ||
    command.armies < pending.minArmies ||
    command.armies > pending.maxArmies
  ) {
    return reject(
      "INVALID_OCCUPATION",
      `Move ${pending.minArmies}..${pending.maxArmies} armies from ${territoryLabel(state, pending.from)} into ${territoryLabel(state, pending.to)}; ${command.armies} is not allowed.`,
    );
  }

  const previousOwnerId = state.territories[pending.to]!.ownerId!;
  const events: GameEventV2[] = [
    {
      type: "TerritoryOccupied",
      attackId: pending.attackId,
      turnId: pending.turnId,
      playerId: command.playerId,
      from: pending.from,
      to: pending.to,
      armies: command.armies,
      previousOwnerId,
      commandId: command.commandId,
    },
  ];

  const defenderTerritoriesAfter = ownedByV2(state, previousOwnerId).length - 1;
  if (defenderTerritoriesAfter <= 0) {
    events.push({
      type: "PlayerEliminated",
      playerId: previousOwnerId,
      byPlayerId: command.playerId,
      commandId: command.commandId,
    });
  }
  const attackerTerritoriesAfter = ownedByV2(state, command.playerId).length + 1;
  if (attackerTerritoriesAfter >= Object.keys(state.territories).length) {
    events.push({
      type: "GameWon",
      playerId: command.playerId,
      commandId: command.commandId,
    });
  }

  return { status: "accepted", events };
}

function decideFortify(state: AggregateStateV2, command: FortifyCommandV2): DecisionV2 {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase !== "attack") {
    return reject("INVALID_PHASE", "May fortify once, from the attack phase.");
  }
  if (
    !state.index ||
    !isTerritoryV2(state.index, command.from) ||
    !isTerritoryV2(state.index, command.to)
  ) {
    return reject("UNKNOWN_TERRITORY", "Unknown fortify territory.");
  }
  const from = state.territories[command.from]!;
  const to = state.territories[command.to]!;
  if (from.ownerId !== command.playerId || to.ownerId !== command.playerId) {
    return reject(
      "ILLEGAL_ACTION",
      `Fortification requires two owned territories; ${territoryLabel(state, command.from)} is owned by ${playerLabel(state, from.ownerId)} and ${territoryLabel(state, command.to)} by ${playerLabel(state, to.ownerId)}.`,
    );
  }
  if (command.from === command.to) {
    return reject("ILLEGAL_ACTION", "Source and destination must differ.");
  }
  if (!friendlyReachable(state, command.playerId, command.from).includes(command.to)) {
    return reject(
      "NO_FRIENDLY_PATH",
      `${territoryLabel(state, command.to)} cannot be fortified from ${territoryLabel(state, command.from)} because no path of your territories connects them.`,
    );
  }
  if (!Number.isInteger(command.armies) || command.armies < 1 || command.armies > from.armies - 1) {
    return reject(
      "INSUFFICIENT_ARMIES",
      `${territoryLabel(state, command.from)} has ${from.armies} armies; cannot move ${command.armies} while leaving one behind.`,
    );
  }
  const { nextPlayerId, round } = nextTurnV2(state, command.playerId);
  return accept(
    {
      type: "ArmiesFortified",
      turnId: command.turnId,
      playerId: command.playerId,
      from: command.from,
      to: command.to,
      armies: command.armies,
      commandId: command.commandId,
    },
    {
      type: "TurnEnded",
      turnId: command.turnId,
      playerId: command.playerId,
      nextPlayerId,
      round,
      commandId: command.commandId,
    },
  );
}

function decideSkipFortifications(
  state: AggregateStateV2,
  command: SkipFortificationsCommandV2,
): DecisionV2 {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase !== "attack") {
    return reject("INVALID_PHASE", "May skip fortifications only after reinforcing.");
  }
  const { nextPlayerId, round } = nextTurnV2(state, command.playerId);
  return accept({
    type: "TurnEnded",
    turnId: command.turnId,
    playerId: command.playerId,
    nextPlayerId,
    round,
    commandId: command.commandId,
  });
}

export function decideV2(
  state: AggregateStateV2,
  command: CommandV2,
  ctx: DecideContextV2,
): DecisionV2 {
  switch (command.type) {
    case "create-game": {
      if (state.gameId) return reject("GAME_ALREADY_EXISTS", "A game already exists.");
      return accept({
        type: "GameCreated",
        gameId: command.gameId,
        hostPlayerId: command.hostPlayerId,
        hostName: command.hostName,
        hostColor: assignPlayerColorV2([], command.hostColor),
        hostController: command.hostController,
        ruleset: RULESET_V2,
        mapVersion: MAP_VERSION_V2,
        generatorVersion: GENERATOR_VERSION_V2,
        mapSeed: command.mapSeed,
        commandId: command.commandId,
      });
    }
    case "join-game":
      return decideJoin(state, command);
    case "delegate-agent-seat":
      return decideDelegateAgent(state, command);
    case "start-game":
      return decideStart(state, command, ctx);
    case "reinforce":
      return decideReinforce(state, command);
    case "declare-attack":
      return decideDeclareAttack(state, command, ctx);
    case "roll-defense":
      return decideRollDefense(state, command, ctx);
    case "resolve-defense-timeout":
      return decideResolveTimeout(state, command, ctx);
    case "occupy-territory":
      return decideOccupy(state, command);
    case "fortify":
      return decideFortify(state, command);
    case "skip-fortifications":
      return decideSkipFortifications(state, command);
  }
}
