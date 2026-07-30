/**
 * The newest resolved throw, as the *map* needs to see it.
 *
 * The dice card explains a throw to the player who made it. The map answers a
 * different question — *where did that just happen, and what did it cost?* — and it
 * is the only surface a spectator of an agent-versus-agent match is watching, so a
 * throw that leaves no mark on the map effectively did not happen for them.
 *
 * Nothing new is recorded to make this possible. `AttackResolved` already carries
 * the route and both losses into the bounded `moves` feed, so the overlay is a pure
 * read of state the board projection already holds. The newest throw is always
 * inside that window: the feed keeps 40 rows and a single throw is two of them.
 *
 * Ordering follows the feed's own rule (`boardRowsFromQueries` sorts moves by
 * `sourceOffset`), but the comparison is made here rather than assumed, so the
 * derivation is correct whichever way round the caller holds the feed.
 */

import type { ProjectedMove } from "../board/projection.ts";

/** One resolved throw, reduced to what the map draws. */
export interface AttackTrace {
  /** Unique per throw — "attack again" declares a new attack — so it keys the fade. */
  attackId: string;
  from: string;
  to: string;
  attackerLosses: number;
  defenderLosses: number;
  /** A capture is drawn differently from a bounce: the target changed hands. */
  captured: boolean;
}

export function latestAttackTrace(moves: readonly ProjectedMove[]): AttackTrace | null {
  let newest: ProjectedMove | undefined;
  for (const move of moves) {
    if (move.kind !== "AttackResolved") continue;
    // A row missing its route cannot be drawn; skipping it is better than guessing
    // at coordinates the projection did not record.
    if (move.attackId === undefined || move.from === undefined || move.to === undefined) continue;
    if (!newest || move.sourceOffset.localeCompare(newest.sourceOffset) > 0) newest = move;
  }
  if (!newest) return null;
  return {
    attackId: newest.attackId!,
    from: newest.from!,
    to: newest.to!,
    attackerLosses: newest.attackerLosses ?? 0,
    defenderLosses: newest.defenderLosses ?? 0,
    captured: newest.territoryCaptured === true,
  };
}
