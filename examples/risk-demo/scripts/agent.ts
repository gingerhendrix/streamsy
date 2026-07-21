/**
 * Runnable coding-agent harness (real HTTP).
 *
 * Blocks on its per-player turn stream, resumes from a file-persisted cursor,
 * fetches fresh `/decision`, and plays with the deterministic strategy in
 * `server/agent.ts` until control passes / the game ends. Kill it and re-run with
 * the same `CURSOR_FILE` to prove cursor-persisted resume across process restart.
 *
 *   BASE_URL=http://localhost:1339 GAME_ID=game_xxx PLAYER_ID=p_xxx \
 *   PLAYER_TOKEN=rsk_... CURSOR_FILE=./p1.cursor bun run scripts/agent.ts
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";

import { createAgent, type AgentState, type HttpCall } from "../server/agent.ts";

const baseUrl = process.env.BASE_URL ?? "http://localhost:1339";
const gameId = process.env.GAME_ID ?? "";
const playerId = process.env.PLAYER_ID ?? "";
const token = process.env.PLAYER_TOKEN ?? "";
const cursorFile = process.env.CURSOR_FILE ?? "";
const longPollMs = Number.parseInt(process.env.POLL_MS ?? "5000", 10);

if (!gameId || !playerId || !token) {
  console.error("Set GAME_ID, PLAYER_ID, and PLAYER_TOKEN.");
  process.exit(2);
}

const httpCall: HttpCall = async (method, path, opts = {}) => {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.token) headers.authorization = `Bearer ${opts.token}`;
  const init: RequestInit = { method, headers };
  if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
  const res = await fetch(`${baseUrl}${path}`, init);
  return { status: res.status, body: await res.json().catch(() => ({})) };
};

function loadState(): AgentState {
  if (cursorFile && existsSync(cursorFile)) {
    try {
      return JSON.parse(readFileSync(cursorFile, "utf8")) as AgentState;
    } catch {
      // ignore malformed cursor file
    }
  }
  return {};
}

function saveState(state: AgentState): void {
  if (cursorFile) writeFileSync(cursorFile, JSON.stringify(state));
}

async function main(): Promise<void> {
  const state = loadState();
  const agent = createAgent({ call: httpCall, gameId, playerId, token, state });

  for (;;) {
    const meta = await httpCall("GET", `/v1/games/${gameId}`);
    if (meta.status !== 200) {
      console.error(`game unavailable: ${meta.status}`);
      return;
    }
    if (meta.body.status === "finished") {
      const won = meta.body.winnerId === playerId;
      console.log(`game over — ${won ? "I won" : `winner ${meta.body.winnerId}`}`);
      return;
    }

    if (meta.body.activePlayerId === playerId) {
      await agent.awaitTurn(0); // consume my wake
      saveState(state);
      console.log(`playing turn (round ${meta.body.round})`);
      await agent.playTurn();
      saveState(state);
    } else {
      // Block on the turn stream until control passes to me.
      await agent.awaitTurn(longPollMs);
      saveState(state);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
