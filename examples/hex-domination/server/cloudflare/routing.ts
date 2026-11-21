/** Game routing rules shared by the edge Worker and the game object. */
import type { DurableObjectNamespace, Fetcher } from "@cloudflare/workers-types";

/** The edge sets this header. The game object checks it against its bound game. */
export const GAME_ID_HEADER = "x-risk-game-id";

/** A 96-bit game id minted by the edge. */
export const GAME_ID_PATTERN = /^game_[0-9a-f]{24}$/;

export interface GameWorkerEnv {
  readonly GAME: DurableObjectNamespace;
  readonly ASSETS: Fetcher;
  /** Optional defence window in milliseconds. Local tests shorten it. */
  readonly DEFENSE_TIMEOUT_MS?: string;
}

export function randomGameId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(12));
  return `game_${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

/**
 * The game id of a game-scoped API path or public board path, or null.
 * Only well-formed edge ids route to an object.
 */
export function gameIdFromPath(pathname: string): string | null {
  const match =
    /^\/v1\/games\/([^/]+)(?:\/|$)/.exec(pathname) ??
    /^\/streams\/games\/([^/]+)\/projections\/board\/[^/]+$/.exec(pathname);
  if (!match) return null;
  return GAME_ID_PATTERN.test(match[1]!) ? match[1]! : null;
}

export function jsonError(status: number, code: string, message: string): Response {
  return new Response(JSON.stringify({ status: "rejected", error: { code, message } }), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}
