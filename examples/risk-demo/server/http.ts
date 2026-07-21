/**
 * JSON HTTP helpers and a tiny path router (web-standard `Request`/`Response`,
 * so the whole app is testable without starting a Bun server).
 */

import type { RiskErrorCode } from "../src/commands.ts";

export function json(data: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

/** Domain/transport error body with a stable machine-readable code. */
export type ErrorCode =
  | RiskErrorCode
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "WRONG_GAME"
  | "NOT_FOUND"
  | "BAD_REQUEST"
  | "PROJECTION_UNAVAILABLE"
  | "INTERNAL";

export function error(
  status: number,
  code: ErrorCode,
  message: string,
  extra: Record<string, unknown> = {},
): Response {
  return json({ status: "rejected", error: { code, message, ...extra } }, status);
}

/** HTTP status appropriate for a domain rejection code. */
export function statusForCode(code: string): number {
  switch (code) {
    case "NOT_YOUR_TURN":
    case "STALE_TURN":
    case "INVALID_PHASE":
    case "ILLEGAL_ACTION":
    case "INSUFFICIENT_ARMIES":
    case "NOT_ADJACENT":
    case "UNKNOWN_TERRITORY":
    case "GAME_FINISHED":
    case "GAME_ALREADY_STARTED":
    case "GAME_NOT_STARTED":
    case "NOT_ENOUGH_PLAYERS":
    case "TOO_MANY_PLAYERS":
    case "PLAYER_ID_TAKEN":
      return 409;
    case "COMMAND_ID_REUSED":
      return 409;
    case "UNAUTHORIZED":
      return 401;
    case "FORBIDDEN":
    case "WRONG_GAME":
      return 403;
    case "GAME_NOT_FOUND":
    case "NOT_FOUND":
      return 404;
    default:
      return 400;
  }
}

export interface Route {
  method: string;
  /** Path pattern with `:param` segments, e.g. `/v1/games/:gameId/players`. */
  pattern: string;
  handler: (request: Request, params: Record<string, string>) => Promise<Response> | Response;
}

function matchPath(pattern: string, path: string): Record<string, string> | null {
  const p = pattern.split("/");
  const a = path.split("/");
  if (p.length !== a.length) return null;
  const params: Record<string, string> = {};
  for (let i = 0; i < p.length; i += 1) {
    const seg = p[i]!;
    const val = a[i]!;
    if (seg.startsWith(":")) params[seg.slice(1)] = decodeURIComponent(val);
    else if (seg !== val) return null;
  }
  return params;
}

/** Build a fetch handler from a route table. */
export function createRouter(routes: Route[]): (request: Request) => Promise<Response> {
  return async (request) => {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";
    let methodMismatch = false;
    for (const route of routes) {
      const params = matchPath(route.pattern, path);
      if (!params) continue;
      if (route.method !== request.method) {
        methodMismatch = true;
        continue;
      }
      try {
        return await route.handler(request, params);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return error(500, "INTERNAL", message);
      }
    }
    return error(methodMismatch ? 405 : 404, "NOT_FOUND", `No route for ${request.method} ${path}`);
  };
}

export async function readJsonBody<T>(request: Request): Promise<T | null> {
  try {
    return (await request.json()) as T;
  } catch {
    return null;
  }
}
