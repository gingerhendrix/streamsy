/* oxlint-disable effecttsgo/async-function -- This executable script is a bounded Promise-native Bun/Node adapter over the demo's public HTTP and application APIs. */
/* oxlint-disable effecttsgo/global-console, effecttsgo/global-fetch, effecttsgo/node-builtin-import, effecttsgo/process-env -- This executable Bun bot directly owns HTTP, terminal output, process configuration, and file-backed checkpoint boundaries. */
/**
 * Runnable scripted-bot harness (real HTTP).
 *
 * Blocks on its per-player turn stream, resumes from a file-persisted cursor,
 * fetches fresh `/decision`, and plays with the deterministic strategy in
 * `server/demo/bot.ts` until control passes / the game ends. Kill it and re-run with
 * the same `CURSOR_FILE` to prove cursor-persisted resume across process restart.
 *
 *   BASE_URL=http://localhost:1339 GAME_ID=game_xxx PLAYER_ID=p_xxx \
 *   PLAYER_TOKEN=rsk_... CURSOR_FILE=./p1.cursor bun run scripts/bot.ts
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { Schema } from "effect";

import { ACTIONS_STREAM_CLIENT_TIMEOUT_MS } from "../src/application/actions-stream.ts";
import { GameResponse } from "../src/application/api.ts";
import {
  createBot,
  type BotState,
  type HttpCall,
  type OpenActionsStream,
} from "../server/demo/bot.ts";

const baseUrl = process.env.BASE_URL ?? "http://localhost:1339";
const gameId = process.env.GAME_ID ?? "";
const playerId = process.env.PLAYER_ID ?? "";
const token = process.env.PLAYER_TOKEN ?? "";
const cursorFile = process.env.CURSOR_FILE ?? "";
const BotStateSchema = Schema.Struct({
  cursor: Schema.optionalKey(Schema.String),
  inflight: Schema.optionalKey(
    Schema.Struct({ body: Schema.String, cursorAfter: Schema.optionalKey(Schema.String) }),
  ),
});

if (!gameId || !playerId || !token) {
  console.error("Set GAME_ID, PLAYER_ID, and PLAYER_TOKEN.");
  process.exit(2);
}

const httpCall: HttpCall = async (method, path, opts = {}) => {
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: opts.accept ?? "application/json",
  };
  if (opts.token) headers.authorization = `Bearer ${opts.token}`;
  const init: RequestInit = { method, headers };
  if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
  const res = await fetch(`${baseUrl}${path}`, init);
  return { status: res.status, body: await res.json().catch(() => ({})) };
};

/** The actions resource is SSE; `EventSource` cannot carry the capability. */
const openStream: OpenActionsStream = (path, opts) =>
  fetch(`${baseUrl}${path}`, {
    headers: { accept: "text/event-stream", authorization: `Bearer ${opts.token}` },
    signal: opts.signal,
  });

function loadState(): BotState {
  if (cursorFile && existsSync(cursorFile)) {
    try {
      return Schema.decodeUnknownSync(BotStateSchema)(JSON.parse(readFileSync(cursorFile, "utf8")));
    } catch {
      // ignore malformed cursor file
    }
  }
  return {};
}

function saveState(state: BotState): void {
  if (cursorFile) writeFileSync(cursorFile, JSON.stringify(state));
}

async function main(): Promise<void> {
  const state = loadState();
  // Persist on every durable change, not just between turns: the in-flight
  // record exists precisely for a crash mid-command, and would be worthless if
  // it were only written after the command had already settled.
  const bot = createBot({
    call: httpCall,
    openStream,
    gameId,
    playerId,
    token,
    state,
    onStateChanged: saveState,
  });

  for (;;) {
    const meta = await httpCall("GET", `/v1/games/${gameId}`);
    if (meta.status !== 200) {
      console.error(`game unavailable: ${meta.status}`);
      return;
    }
    const game = Schema.decodeUnknownSync(GameResponse)(meta.body);
    if (game.status === "finished") {
      const won = game.winnerId === playerId;
      console.log(`game over — ${won ? "I won" : `winner ${game.winnerId}`}`);
      return;
    }

    if (game.activePlayerId === playerId) {
      await bot.awaitTurn(0); // consume my wake
      saveState(state);
      console.log(`playing turn (round ${game.round})`);
      await bot.playTurn();
      saveState(state);
    } else {
      // Block on the SSE turn stream until control passes to me. The server
      // closes the connection on its own bound, so this guard carries the
      // transport slack that keeps it — not the client's timer — the party that
      // ends an idle connection.
      const wake = await bot.awaitTurn(ACTIONS_STREAM_CLIENT_TIMEOUT_MS);
      saveState(state);
      // `Hex Domination` asks this seat to act out of turn too. Defence is attempted
      // whenever canonical state says one is open, not only on a `DefenseAvailable`
      // wake: the wake is a hint, and a missed one must not leave a human attacker
      // watching the full 15-second timeout.
      if (
        (wake?.type === "ActionRequired" && wake.reason === "defense-required") ||
        game.pendingInteraction?.type === "defense"
      ) {
        if (await bot.defend()) console.log("rolled defence");
      }
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
