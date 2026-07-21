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
