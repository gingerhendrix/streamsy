/** Canonical Streamsy stream names per game. */

/** Initial generation of the independently rebuildable board projection. */
export const BOARD_GENERATION = "board1";

export function eventStreamId(gameId: string): string {
  return `games/${gameId}/events`;
}

export function boardStreamId(gameId: string, generation = BOARD_GENERATION): string {
  return `games/${gameId}/projections/board/${generation}`;
}

export function actionStreamId(gameId: string, playerId: string): string {
  return `games/${gameId}/players/${playerId}/actions`;
}

/**
 * The next board-projection generation id after `current`. A trailing integer is
 * bumped (`board1` → `board2`); any other shape gets a `-next` suffix so
 * a rebuild always targets a fresh, separate stream and never overwrites the
 * active one.
 */
export function nextGeneration(current: string): string {
  const match = /^(.*?)(\d+)$/.exec(current);
  if (match) return `${match[1]}${Number(match[2]) + 1}`;
  return `${current}-next`;
}
