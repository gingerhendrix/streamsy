/**
 * The `Hex Domination` player palette and its conflict-safe assignment.
 *
 * Colour is game state, and the decider is the only race-safe place to choose it:
 * assignment happens after the command log has deduped `commandId` and folded the
 * current roster, so two simultaneous joins can never leave the lobby with a
 * duplicate. A caller may still *request* a colour — an existing client, a demo
 * fixture, or a deterministic test — and the request is honoured when it is free;
 * otherwise the seat is issued the first available palette colour instead of the
 * join being rejected.
 *
 * The palette has exactly `RULES.maxPlayers` entries, and every seat check runs
 * after the roster-size guard, so a free palette colour always exists.
 */

import { RULES } from "./map.ts";

/**
 * The palette is the seat budget: if the game ever seats more players than the
 * palette can issue, assignment could repeat a colour, so the length is checked
 * against `RULES.maxPlayers` at compile time.
 */
type SeatColorPalette = readonly string[] & { readonly length: typeof RULES.maxPlayers };

/** One issued colour per possible seat, in issue order. */
export const PLAYER_COLORS = [
  "#e05a47",
  "#3b82f6",
  "#d49b35",
  "#8b5cf6",
] as const satisfies SeatColorPalette;

/** Colour equality ignores case and surrounding whitespace, as the decider always has. */
export function normalizedPlayerColor(color: string): string {
  return color.trim().toLowerCase();
}

/**
 * The colour a new seat receives: the requested colour when it is present and
 * unclaimed, otherwise the first free palette colour.
 */
export function assignPlayerColor(takenColors: readonly string[], requestedColor?: string): string {
  const taken = new Set(takenColors.map(normalizedPlayerColor));
  const requested = requestedColor?.trim();
  if (requested && !taken.has(normalizedPlayerColor(requested))) return requested;
  const available = PLAYER_COLORS.find((color) => !taken.has(normalizedPlayerColor(color)));
  // Unreachable while the palette matches `maxPlayers` and assignment follows the
  // roster-size guard; the first palette colour keeps the function total anyway.
  return available ?? PLAYER_COLORS[0];
}
