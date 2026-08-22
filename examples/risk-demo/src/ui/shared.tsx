/** Player session, typed fetch, identity fields, and sync presentation helpers. */

import type { CSSProperties, ReactNode } from "react";

import { friendlyError, type ApiErrorResponse } from "../application/api.ts";
import type { PlayerController } from "../domain/events.ts";
import type { SyncStatus } from "./board-stream-db.ts";

export interface Identity {
  gameId: string;
  playerId: string;
  token: string;
  role: "host" | "player" | "agent";
  /**
   * Set once this capability's own seat has been handed to an agent. The role
   * stays `host` because the host capability is still what starts the game and
   * opens further seats; this records that the seat behind it is no longer the
   * reader's to play, for the window before the projection reports the
   * delegation.
   */
  spectator?: boolean;
}

/** Seat-name limit, shared by the join field and every agent-seat field. */
export const MAX_SEAT_NAME = 24;

/* oxlint-disable effecttsgo/global-fetch -- This shared React browser adapter performs the typed Web request used by UI event handlers. */
/**
 * The name to send when opening or delegating an agent seat.
 *
 * A blank name must never reach the server: the muster roll and the move feed are
 * both read as lists of names, so an unnamed seat makes the history ambiguous.
 * Falling back to the seat's ordinal keeps the defaults distinct from each other.
 */
export function agentSeatName(input: string | undefined, seat: number): string {
  const trimmed = (input ?? "").trim().slice(0, MAX_SEAT_NAME);
  return trimmed || `Agent ${seat}`;
}

/**
 * Whether this browser watches the game rather than plays it.
 *
 * Three ways to be a spectator, and all three matter: no stored identity, an
 * identity whose seat is not (or not yet) on the board, and an identity whose seat
 * is driven by something other than a human. The last is why an agent-versus-agent
 * game reads as a spectator's game — the creator delegates its own seat to an agent
 * that plays through its own capability, so this UI must never compose that seat's
 * moves even though the same browser still holds the host capability.
 *
 * Note the precedence: the persisted `spectator` marker is checked *before* the seat,
 * so it deliberately outranks canonical state. That is only safe while delegation is
 * one-way. Anyone adding an un-delegate, takeover, or disconnect-recovery flow that
 * returns a seat to `human` must clear the marker from the stored identity at the same
 * time — otherwise that browser stays a spectator forever with no UI path back, a
 * stale local flag overriding the board, which is backwards for this codebase.
 */
export function spectatingSeat(
  identity: Identity | null,
  seat: { controller: PlayerController } | undefined,
): boolean {
  if (!identity || identity.spectator === true) return true;
  if (!seat) return true;
  return seat.controller !== "human";
}

export interface ApiResult<T> {
  status: number;
  body: T | ApiErrorResponse;
}

export const STORAGE_KEY = "risk-demo-identity";

export function loadIdentity(): Identity | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- React CSSProperties omits application-defined CSS custom properties; this object contains only locally declared style values.
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

// oxlint-disable-next-line effecttsgo/async-function -- React event handlers consume this Promise-native browser fetch facade directly.
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
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- React CSSProperties omits application-defined CSS custom properties; this object contains only locally declared style values.
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
  return friendlyError(body.error.code, body.error.message);
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
 * surface itself carries no offsets, generations, or watermarks.
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
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- React CSSProperties omits application-defined CSS custom properties; this object contains only locally declared style values.
    <span className="active-chip" style={{ "--player": props.color } as CSSProperties}>
      {props.label ?? props.name}
    </span>
  );
}
