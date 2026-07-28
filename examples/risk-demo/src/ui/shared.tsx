/**
 * Pieces both ruleset surfaces share: the player session, the typed fetch helper,
 * the identity fields, and the compact sync pill.
 *
 * The renderer is chosen from the game's canonical `ruleset` (design spec §11), so
 * a v1 game keeps its fixed-map board for as long as it exists while every new game
 * gets the hex map. These helpers are what the two screens have in common — not a
 * shared abstraction over two different games.
 */

import type { CSSProperties, ReactNode } from "react";

import { friendlyError, type ApiErrorCode, type ApiErrorResponse } from "../application/api.ts";
import { PLAYER_COLORS_V2 } from "../domain/colors-v2.ts";
import type { SyncStatus } from "./board-stream-db.ts";

export interface Identity {
  gameId: string;
  playerId: string;
  token: string;
  role: "host" | "player" | "agent";
}

export interface ApiResult<T> {
  status: number;
  body: T | ApiErrorResponse;
}

export const STORAGE_KEY = "risk-demo-identity";
/** The canonical v2 palette; the v1 surface offers the same four swatches. */
export const COLORS: readonly string[] = PLAYER_COLORS_V2;

export function normalizedColor(color: string): string {
  return color.trim().toLowerCase();
}

export type BoardRenderer = "risk-demo-v1" | "risk-demo-v2";

/**
 * Which board surface a game gets, decided by its canonical ruleset and nothing
 * else (design spec §11).
 *
 * `null` while the game resource is still loading: with no ruleset there is no
 * answer yet, and guessing would open the wrong projection stream. An older game
 * that predates the field is v1 — the version is never inferred from which rows
 * happen to be missing from a projection.
 */
export function rendererForGame(game: { ruleset?: string } | null): BoardRenderer | null {
  if (!game) return null;
  return game.ruleset === "risk-demo-v2" ? "risk-demo-v2" : "risk-demo-v1";
}

export function loadIdentity(): Identity | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as Identity) : null;
  } catch {
    return null;
  }
}

export function gameFromPath(pathname: string): string {
  const match = /^\/game\/([^/]+)\/?$/.exec(pathname);
  if (!match) return "";
  try {
    return decodeURIComponent(match[1]!);
  } catch {
    return "";
  }
}

export function gameFromUrl(): string {
  return gameFromPath(window.location.pathname);
}

export function gamePath(gameId: string): string {
  return `/game/${encodeURIComponent(gameId)}`;
}

export async function api<T>(
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<ApiResult<T>> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
  const response = await fetch(path, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  return {
    status: response.status,
    body: (await response.json().catch(() => ({
      status: "rejected",
      error: { code: "INTERNAL", message: "Invalid server response." },
    }))) as T | ApiErrorResponse,
  };
}

export function isError(body: unknown): body is ApiErrorResponse {
  return Boolean(
    body &&
    typeof body === "object" &&
    "status" in body &&
    body.status === "rejected" &&
    "error" in body,
  );
}

export function errorMessage(body: unknown, fallback: string): string {
  if (!isError(body)) return fallback;
  return friendlyError(body.error.code as ApiErrorCode, body.error.message);
}

export function acknowledgementNotice(actionType: string): string {
  const action = actionType.replaceAll("-", " ");
  return `${action.charAt(0).toUpperCase()}${action.slice(1)} committed to the stream.`;
}

export function playerRoleLabel(hostPlayerId: string | undefined, playerId: string): string {
  return playerId === hostPlayerId ? "Host" : "Player";
}

const compactOffsetPart = (part: string): string => part.replace(/^0+(?=\d)/, "");

/**
 * The one place an offset is still shown, and only inside the sync pill. The game
 * surface itself carries no offsets, generations, or watermarks (design spec §8.4).
 */
export function shortOffset(offset: string | null | undefined): string {
  if (!offset) return "awaiting first event";
  const [commit, item] = offset.split("_");
  return item === undefined
    ? compactOffsetPart(offset)
    : `#${compactOffsetPart(commit!)}·${compactOffsetPart(item)}`;
}

export function SyncPill(props: {
  status: SyncStatus;
  offset: string | null;
  error: string | null;
}) {
  return (
    <div className={`sync-pill ${props.status}`} title={props.error ?? undefined}>
      <span className="sync-light" />
      <span>
        <b>{props.status === "live" ? "Live" : props.status.replace("-", " ")}</b>
        <small>{shortOffset(props.offset)}</small>
      </span>
    </div>
  );
}

export function TopBar(props: { gameId: string; children: ReactNode }) {
  return (
    <header className="topbar">
      <div>
        <a className="brand" href="/" aria-label="Hex Domination home">
          <span className="brand-mark">S</span> Streamsy <b>Hex Domination</b>
        </a>
        <div className="game-code">
          Game <code>{props.gameId}</code>
        </div>
      </div>
      {props.children}
    </header>
  );
}

export function PlayerFields(props: { name: string; onName(value: string): void }) {
  return (
    <div className="player-fields">
      <label>
        <span>Your name</span>
        <input
          value={props.name}
          maxLength={24}
          onChange={(event) => props.onName(event.target.value)}
        />
      </label>
    </div>
  );
}

export function PlayerChip(props: { name: string; color: string; label?: string }) {
  return (
    <span className="active-chip" style={{ "--player": props.color } as CSSProperties}>
      {props.label ?? props.name}
    </span>
  );
}
