/**
 * Pure command decision + validation.
 *
 * `decide` evaluates a *fresh* command against a folded aggregate state and
 * returns either the canonical events to append or a stable rejection. It is the
 * only place randomness is consumed, and only for `attack`. Idempotent replay of
 * already-accepted commands is handled by the command-log layer, so `decide`
 * never rolls dice for a duplicate.
 */

import type {
  AttackCommand,
  Command,
  EndTurnCommand,
  FortifyCommand,
  JoinGameCommand,
  PlayCommand,
  ReinforceCommand,
  RiskErrorCode,
  StartGameCommand,
} from "./commands.ts";
import type { GameEvent } from "./events.ts";
import type { AggregateState } from "./aggregate.ts";
import { currentTurnId, nextTurn, ownedBy } from "./aggregate.ts";
import { RULES, TERRITORY_IDS, areAdjacent, isTerritory } from "./map.ts";
import type { Rng } from "./rng.ts";
import { shuffle } from "./rng.ts";
import { resolveAttack } from "./dice.ts";

export interface DecisionError {
  code: RiskErrorCode;
  message: string;
  currentTurnId?: string;
}

export type Decision =
  | { status: "accepted"; events: GameEvent[] }
  | { status: "rejected"; error: DecisionError };

function reject(
  code: RiskErrorCode,
  message: string,
  extra?: { currentTurnId?: string },
): Decision {
  return { status: "rejected", error: { code, message, ...extra } };
}

function accept(...events: GameEvent[]): Decision {
  return { status: "accepted", events };
}

/** Shared turn preconditions for every play command. */
function ensureTurn(state: AggregateState, command: PlayCommand): DecisionError | undefined {
  if (state.status === "finished") {
    return { code: "GAME_FINISHED", message: "The game has already finished." };
  }
  if (state.status !== "playing") {
    return { code: "GAME_NOT_STARTED", message: "The game has not started." };
  }
  if (command.playerId !== state.activePlayerId) {
    return { code: "NOT_YOUR_TURN", message: "It is not this player's turn." };
  }
  const turnId = currentTurnId(state);
  if (command.turnId !== turnId) {
    return {
      code: "STALE_TURN",
      message: "The observed turn is no longer active.",
      currentTurnId: turnId,
    };
  }
  return undefined;
}

function decideJoin(state: AggregateState, command: JoinGameCommand): Decision {
  if (!state.gameId) return reject("GAME_NOT_FOUND", "No game to join.");
  if (state.status !== "lobby") {
    return reject("GAME_ALREADY_STARTED", "Cannot join a game in progress.");
  }
  if (state.players.length >= RULES.maxPlayers) {
    return reject("TOO_MANY_PLAYERS", `A game seats at most ${RULES.maxPlayers} players.`);
  }
  if (state.players.some((p) => p.id === command.playerId)) {
    return reject("PLAYER_ID_TAKEN", "That player id is already in the game.");
  }
  return accept({
    type: "PlayerJoined",
    playerId: command.playerId,
    name: command.name,
    color: command.color,
    commandId: command.commandId,
  });
}

function decideStart(state: AggregateState, command: StartGameCommand, rng: Rng): Decision {
  if (!state.gameId) return reject("GAME_NOT_FOUND", "No game to start.");
  if (state.status !== "lobby") {
    return reject("GAME_ALREADY_STARTED", "The game has already started.");
  }
  if (state.players.length < RULES.minPlayers) {
    return reject("NOT_ENOUGH_PLAYERS", `A game needs at least ${RULES.minPlayers} players.`);
  }

  const turnOrder = shuffle(
    rng,
    state.players.map((p) => p.id),
  );
  const dealtTerritories = shuffle(rng, TERRITORY_IDS);
  const initialTerritories = dealtTerritories.map((territoryId, index) => ({
    territoryId,
    ownerId: turnOrder[index % turnOrder.length]!,
    armies: RULES.initialArmiesPerTerritory,
  }));

  return accept({
    type: "GameStarted",
    turnOrder,
    initialTerritories,
    round: 1,
    commandId: command.commandId,
  });
}

function decideReinforce(state: AggregateState, command: ReinforceCommand): Decision {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase !== "reinforce") {
    return reject("INVALID_PHASE", "Reinforcements can only be placed in the reinforce phase.");
  }
  if (!isTerritory(command.territoryId)) {
    return reject("UNKNOWN_TERRITORY", `Unknown territory: ${command.territoryId}.`);
  }
  if (state.territories[command.territoryId]?.ownerId !== command.playerId) {
    return reject("ILLEGAL_ACTION", "Can only reinforce an owned territory.");
  }
  if (
    !Number.isInteger(command.armies) ||
    command.armies < 1 ||
    command.armies > state.reinforcementsRemaining
  ) {
    return reject(
      "INSUFFICIENT_ARMIES",
      `Must place between 1 and ${state.reinforcementsRemaining} armies.`,
    );
  }
  return accept({
    type: "ArmiesReinforced",
    playerId: command.playerId,
    territoryId: command.territoryId,
    armies: command.armies,
    commandId: command.commandId,
  });
}

function decideAttack(state: AggregateState, command: AttackCommand, rng: Rng): Decision {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase !== "attack") {
    return reject("INVALID_PHASE", "Attacks are only allowed in the attack phase.");
  }
  if (!isTerritory(command.from) || !isTerritory(command.to)) {
    return reject("UNKNOWN_TERRITORY", "Unknown attack territory.");
  }
  const from = state.territories[command.from]!;
  const to = state.territories[command.to]!;
  if (from.ownerId !== command.playerId) {
    return reject("ILLEGAL_ACTION", "Can only attack from an owned territory.");
  }
  if (to.ownerId === command.playerId) {
    return reject("ILLEGAL_ACTION", "Cannot attack your own territory.");
  }
  if (!areAdjacent(command.from, command.to)) {
    return reject("NOT_ADJACENT", "Territories are not adjacent.");
  }
  if (
    !Number.isInteger(command.attackerDice) ||
    command.attackerDice < 1 ||
    command.attackerDice > RULES.maxAttackerDice
  ) {
    return reject("ILLEGAL_ACTION", `Attacker dice must be 1..${RULES.maxAttackerDice}.`);
  }
  if (from.armies < command.attackerDice + 1) {
    return reject(
      "INSUFFICIENT_ARMIES",
      "Need at least one more army than dice, leaving one behind.",
    );
  }

  const resolution = resolveAttack(to.armies, command.attackerDice, rng);
  const events: GameEvent[] = [
    {
      type: "AttackResolved",
      playerId: command.playerId,
      from: command.from,
      to: command.to,
      attackerRolls: resolution.attackerRolls,
      defenderRolls: resolution.defenderRolls,
      attackerLosses: resolution.attackerLosses,
      defenderLosses: resolution.defenderLosses,
      territoryCaptured: resolution.territoryCaptured,
      occupyingArmies: resolution.occupyingArmies,
      commandId: command.commandId,
    },
  ];

  if (resolution.territoryCaptured && to.ownerId) {
    const defenderId = to.ownerId;
    const defenderTerritoriesAfter = ownedBy(state, defenderId).length - 1;
    if (defenderTerritoriesAfter <= 0) {
      events.push({
        type: "PlayerEliminated",
        playerId: defenderId,
        byPlayerId: command.playerId,
        commandId: command.commandId,
      });
    }
    const attackerTerritoriesAfter = ownedBy(state, command.playerId).length + 1;
    if (attackerTerritoriesAfter >= TERRITORY_IDS.length) {
      events.push({
        type: "GameWon",
        playerId: command.playerId,
        commandId: command.commandId,
      });
    }
  }

  return { status: "accepted", events };
}

function decideFortify(state: AggregateState, command: FortifyCommand): Decision {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase !== "attack") {
    return reject("INVALID_PHASE", "May fortify once, from the attack phase.");
  }
  if (!isTerritory(command.from) || !isTerritory(command.to)) {
    return reject("UNKNOWN_TERRITORY", "Unknown fortify territory.");
  }
  const from = state.territories[command.from]!;
  const to = state.territories[command.to]!;
  if (from.ownerId !== command.playerId || to.ownerId !== command.playerId) {
    return reject("ILLEGAL_ACTION", "Can only fortify between two owned territories.");
  }
  if (!areAdjacent(command.from, command.to)) {
    return reject("NOT_ADJACENT", "Territories are not adjacent.");
  }
  if (!Number.isInteger(command.armies) || command.armies < 1 || command.armies > from.armies - 1) {
    return reject("INSUFFICIENT_ARMIES", "Must move 1..(armies-1), leaving one behind.");
  }
  return accept({
    type: "ArmiesFortified",
    playerId: command.playerId,
    from: command.from,
    to: command.to,
    armies: command.armies,
    commandId: command.commandId,
  });
}

function decideEndTurn(state: AggregateState, command: EndTurnCommand): Decision {
  const turnError = ensureTurn(state, command);
  if (turnError) return { status: "rejected", error: turnError };
  if (state.phase === "reinforce") {
    return reject("INVALID_PHASE", "Place all reinforcements before ending the turn.");
  }
  const { nextPlayerId, round } = nextTurn(state, command.playerId);
  return accept({
    type: "TurnEnded",
    playerId: command.playerId,
    nextPlayerId,
    round,
    commandId: command.commandId,
  });
}

export function decide(state: AggregateState, command: Command, rng: Rng): Decision {
  switch (command.type) {
    case "create-game": {
      if (state.gameId) return reject("GAME_ALREADY_EXISTS", "A game already exists.");
      return accept({
        type: "GameCreated",
        gameId: command.gameId,
        hostPlayerId: command.hostPlayerId,
        hostName: command.hostName,
        hostColor: command.hostColor,
        ruleset: state.ruleset,
        mapVersion: state.mapVersion,
        commandId: command.commandId,
      });
    }
    case "join-game":
      return decideJoin(state, command);
    case "start-game":
      return decideStart(state, command, rng);
    case "reinforce":
      return decideReinforce(state, command);
    case "attack":
      return decideAttack(state, command, rng);
    case "fortify":
      return decideFortify(state, command);
    case "end-turn":
      return decideEndTurn(state, command);
  }
}
