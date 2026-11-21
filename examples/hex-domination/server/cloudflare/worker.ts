/* oxlint-disable effecttsgo/async-function -- The Workers fetch handler is a Promise-native platform boundary. */
/**
 * Edge Worker for Hex Domination.
 *
 * `POST /v1/games` mints a game id and sends the request to that game's object.
 * Every game-scoped API path and the public board path route by the id in the
 * path. The game object exposes only the active board generation. Canonical
 * events and private action streams have no stream route. Other API and stream
 * paths answer 404 here, and everything else is the bundled SPA.
 */
import { openApiDocument } from "../http/openapi.ts";
import {
  GAME_ID_HEADER,
  gameIdFromPath,
  jsonError,
  randomGameId,
  type GameWorkerEnv,
} from "./routing.ts";

export { GameDurableObject } from "./game-object.ts";
export type { GameWorkerEnv } from "./routing.ts";

const RESERVED_PREFIXES = ["/v1/", "/streams/", "/agent/", "/agent-seat/", "/__"];

async function forwardToGame(
  request: Request,
  env: GameWorkerEnv,
  gameId: string,
): Promise<Response> {
  // The edge owns this header. A client value is always replaced.
  const headers = new Headers(request.headers);
  headers.set(GAME_ID_HEADER, gameId);
  const routed = new Request(request, { headers });
  try {
    return await env.GAME.get(env.GAME.idFromName(gameId)).fetch(routed);
  } catch {
    return jsonError(503, "INTERNAL", "Game unavailable; retry.");
  }
}

export default {
  async fetch(request: Request, env: GameWorkerEnv): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/v1/games" && request.method === "POST") {
      return forwardToGame(request, env, randomGameId());
    }
    if (url.pathname === "/openapi.json") return Response.json(openApiDocument);
    if (url.pathname === "/healthz") return Response.json({ ok: true });

    const gameId = gameIdFromPath(url.pathname);
    if (gameId !== null) return forwardToGame(request, env, gameId);

    if (RESERVED_PREFIXES.some((prefix) => url.pathname.startsWith(prefix))) {
      return jsonError(404, "NOT_FOUND", "Unknown route.");
    }
    return env.ASSETS.fetch(request);
  },
};
