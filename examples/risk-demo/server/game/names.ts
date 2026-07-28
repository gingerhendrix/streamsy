/** Canonical Streamsy stream names per game. */

export const BOARD_GENERATION = "v1";

/**
 * The first `risk-demo-v2` board generation. V2 games start on their own
 * generation lineage rather than reusing `v1`, so a v2 board is always a new
 * projection stream under a new reducer version and no v1 projection history is
 * ever reinterpreted in place.
 */
export const BOARD_GENERATION_V2 = "hex1";
export const ACTIONS_GENERATION_V2 = "actions1";

export function eventStreamId(gameId: string): string {
  return `games/${gameId}/events`;
}

export function boardStreamId(gameId: string, generation = BOARD_GENERATION): string {
  return `games/${gameId}/projections/board/${generation}`;
}

export function actionStreamId(
  gameId: string,
  playerId: string,
  generation = ACTIONS_GENERATION_V2,
): string {
  return `games/${gameId}/players/${playerId}/actions/${generation}`;
}

/**
 * The next board-projection generation id after `current`. A trailing integer is
 * bumped (`v1` → `v2`, `hex1` → `hex2`); any other shape gets a `-next` suffix so
 * a rebuild always targets a fresh, separate stream and never overwrites the
 * active one.
 */
export function nextGeneration(current: string): string {
  const match = /^(.*?)(\d+)$/.exec(current);
  if (match) return `${match[1]}${Number(match[2]) + 1}`;
  return `${current}-next`;
}
