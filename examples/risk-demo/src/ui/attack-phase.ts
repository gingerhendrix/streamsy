import type { LegalActionV2 } from "../application/legal-actions-v2.ts";
import type { GameActionV2 } from "../domain/commands-v2.ts";
import type { CombatView } from "./combat-view.ts";

export type DeclareAttackAction = Extract<LegalActionV2, { type: "declare-attack" }>;

/**
 * Map affordances for the attack flow come only from the player-relative decision.
 * Before a source is selected this returns unique legal sources; afterwards it
 * returns that source plus only its legal enemy neighbours.
 */
export function attackTerritoryIds(
  action: DeclareAttackAction | undefined,
  selectedFrom?: string,
): Set<string> {
  const ids = new Set<string>();
  if (!action) return ids;
  if (selectedFrom === undefined) {
    for (const choice of action.choices) ids.add(choice.from);
    return ids;
  }
  ids.add(selectedFrom);
  for (const choice of action.choices) {
    if (choice.from === selectedFrom) ids.add(choice.to);
  }
  return ids;
}

/**
 * A repeat attack is a new canonical declaration using the fresh legal-action
 * resource. A captured territory, depleted source, or changed ownership therefore
 * cannot be repeated from stale combat data.
 */
export function attackAgainAction(
  action: DeclareAttackAction | undefined,
  combat: CombatView | null,
): Extract<GameActionV2, { type: "declare-attack" }> | null {
  if (!action || !combat || combat.status !== "resolved" || combat.territoryCaptured) return null;
  const choice = action.choices.find(
    (candidate) => candidate.from === combat.from && candidate.to === combat.to,
  );
  return choice
    ? {
        type: "declare-attack",
        from: choice.from,
        to: choice.to,
        attackerDice: choice.maxAttackerDice,
      }
    : null;
}

export function fortifyAction(
  from: string,
  to: string,
  armies: number,
): Extract<GameActionV2, { type: "fortify" }> {
  return { type: "fortify", from, to, armies };
}

/** Starting a fresh map composition dismisses the previous pair's dice report. */
export function startsAttackSelection(
  action: DeclareAttackAction | undefined,
  territoryId: string,
): boolean {
  return action?.choices.some((choice) => choice.from === territoryId) ?? false;
}

export function shouldDismissAttackSummary(
  combat: CombatView | null,
  action: DeclareAttackAction | undefined,
  territoryId: string,
): boolean {
  return combat?.status === "resolved" && startsAttackSelection(action, territoryId);
}
