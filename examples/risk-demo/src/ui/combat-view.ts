/**
 * One combat, as the dice card needs to see it.
 *
 * The projection clears the `combat` row the instant an attack stops being open —
 * on a bounce it clears at resolution, on a capture it clears at occupation (design
 * spec §7.2). That is right for canonical state and wrong for a player, who wants
 * to see the throw they just made. So the reveal is held *client-side*, and this
 * function is where the two sources are reconciled:
 *
 *  - a live `combat` row is authoritative while an attack is open;
 *  - once a non-capturing throw clears, `turn.latestDice` still carries the
 *    recorded faces, losses, and resolution source for the newest throw of this
 *    turn;
 *  - once a captured country has been occupied, there is no combat card: the
 *    attacker returns to the phase's initial country-selection view.
 *
 * Both are recorded values. Nothing here invents a face, a loss, or a deadline —
 * the fallback path simply has less identity to work with, which is why the
 * attacker and defender seats are recovered from the bounded move feed rather than
 * guessed. The newest throw is always inside that window even when the turn is long.
 */

import type { ProjectedCombat, ProjectedMove, ProjectedTurn } from "../board/projection.ts";
import type { DefenseResolutionSource } from "../domain/events.ts";

export type CombatViewStatus = "awaiting-defense" | "awaiting-occupation" | "resolved";

export interface CombatView {
  attackId: string;
  status: CombatViewStatus;
  /** Absent only on the reconnect fallback, when the move feed no longer names the seat. */
  attackerId?: string;
  defenderId?: string;
  from: string;
  to: string;
  attackerDice: number;
  attackerRolls: number[];
  defenderDice: number;
  defenderRolls?: number[];
  attackerLosses?: number;
  defenderLosses?: number;
  territoryCaptured?: boolean;
  resolutionSource?: DefenseResolutionSource;
  /** Present while the defence window is open; a closed combat has no deadline. */
  defenseDeadlineAt?: number;
  /** Recorded declaration time, so the countdown ring knows the true window width. */
  declaredAt?: number;
  minArmies?: number;
  maxArmies?: number;
}

export interface CombatSources {
  combat: ProjectedCombat | null;
  turn: ProjectedTurn | null;
  moves: readonly ProjectedMove[];
}

function seatFromMoves(
  moves: readonly ProjectedMove[],
  attackId: string,
  kind: "AttackDeclared" | "AttackResolved",
): string | undefined {
  // `AttackDeclared` records the attacker as its actor and `AttackResolved` the
  // defender, so one lookup each recovers both seats.
  return moves.find((move) => move.attackId === attackId && move.kind === kind)?.playerId;
}

export function combatView(sources: CombatSources): CombatView | null {
  const { combat } = sources;
  if (combat) {
    const view: CombatView = {
      attackId: combat.attackId,
      status: combat.status,
      attackerId: combat.attackerId,
      defenderId: combat.defenderId,
      from: combat.from,
      to: combat.to,
      attackerDice: combat.attackerDice,
      attackerRolls: combat.attackerRolls,
      defenderDice: combat.defenderDice,
    };
    if (combat.defenderRolls) view.defenderRolls = combat.defenderRolls;
    if (combat.attackerLosses !== undefined) view.attackerLosses = combat.attackerLosses;
    if (combat.defenderLosses !== undefined) view.defenderLosses = combat.defenderLosses;
    if (combat.territoryCaptured !== undefined) view.territoryCaptured = combat.territoryCaptured;
    if (combat.resolutionSource) view.resolutionSource = combat.resolutionSource;
    if (combat.status === "awaiting-defense") {
      view.defenseDeadlineAt = combat.defenseDeadlineAt;
      view.declaredAt = combat.declaredAt;
    }
    if (combat.minArmies !== undefined) view.minArmies = combat.minArmies;
    if (combat.maxArmies !== undefined) view.maxArmies = combat.maxArmies;
    return view;
  }

  const dice = sources.turn?.latestDice;
  if (!dice || dice.territoryCaptured) return null;
  // The attacker is the active player by definition, so that seat is always
  // recoverable; the defender is only named by the move feed.
  const attackerId =
    seatFromMoves(sources.moves, dice.attackId, "AttackDeclared") ?? sources.turn?.playerId;
  const defenderId = seatFromMoves(sources.moves, dice.attackId, "AttackResolved");
  const view: CombatView = {
    attackId: dice.attackId,
    status: "resolved",
    from: dice.from,
    to: dice.to,
    attackerDice: dice.attackerRolls.length,
    attackerRolls: dice.attackerRolls,
    defenderDice: dice.defenderRolls.length,
    defenderRolls: dice.defenderRolls,
    attackerLosses: dice.attackerLosses,
    defenderLosses: dice.defenderLosses,
    territoryCaptured: dice.territoryCaptured,
    resolutionSource: dice.resolutionSource,
  };
  if (attackerId) view.attackerId = attackerId;
  if (defenderId) view.defenderId = defenderId;
  return view;
}
