/* oxlint-disable effecttsgo/async-function -- Web-standard fetch handlers are Promise-native framework adapters; they delegate game and projection work to the existing application services and runtime. */
/**
 * JSON HTTP helpers and a tiny path router (web-standard `Request`/`Response`,
 * so the whole app is testable without starting a Bun server).
 */

import {
  isApiErrorCode,
  statusForErrorCode,
  type ApiErrorCode,
} from "../../src/application/api.ts";

export function json(data: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function text(data: string, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(data, {
    status,
    headers: { "content-type": "text/plain; charset=utf-8", ...headers },
  });
}

/** Domain/transport error body with a stable machine-readable code. */
export type ErrorCode = ApiErrorCode;

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
  return isApiErrorCode(code) ? statusForErrorCode(code) : 400;
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

export async function readJsonBody(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const value: unknown = await request.json();
    return value !== null && typeof value === "object" && !Array.isArray(value)
      ? Object.fromEntries(Object.entries(value))
      : null;
  } catch {
    return null;
  }
}
