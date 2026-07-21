/** Canonical Streamsy stream names per game. */

export const BOARD_GENERATION = "v1";

export function eventStreamId(gameId: string): string {
  return `games/${gameId}/events`;
}

export function boardStreamId(gameId: string, generation = BOARD_GENERATION): string {
  return `games/${gameId}/projections/board/${generation}`;
}

export function turnStreamId(gameId: string, playerId: string): string {
  return `games/${gameId}/players/${playerId}/turns`;
}

/**
 * The next board-projection generation id after `current`. `v<N>` bumps the
 * integer (`v1` → `v2`); any other shape gets a `-next` suffix so a rebuild
 * always targets a fresh, separate stream and never overwrites the active one.
 */
export function nextGeneration(current: string): string {
  const match = /^v(\d+)$/.exec(current);
  if (match) return `v${Number(match[1]) + 1}`;
  return `${current}-next`;
}
