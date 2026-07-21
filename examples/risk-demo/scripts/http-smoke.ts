/**
 * End-to-end HTTP smoke for the durable Risk API.
 *
 * Spawns the real Bun server against a temp SQLite file, drives create → join →
 * start → decision → command → board → recovery over HTTP, then kills the server,
 * respawns it against the SAME database file, and proves events, board projection,
 * command recovery, and capability verifiers all survived the restart.
 *
 *   bun run scripts/http-smoke.ts
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const packageDir = new URL("..", import.meta.url).pathname;

class SmokeError extends Error {}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new SmokeError(message);
}

interface Server {
  baseUrl: string;
  stop: () => Promise<void>;
}

async function startServer(port: number, dbPath: string): Promise<Server> {
  const proc = Bun.spawn(["bun", "server/index.ts"], {
    cwd: packageDir,
    env: { ...process.env, PORT: String(port), DB_PATH: dbPath },
    stdout: "pipe",
    stderr: "pipe",
  });
  const baseUrl = `http://localhost:${port}`;
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${baseUrl}/healthz`);
      if (res.status === 200) break;
    } catch {
      // not up yet
    }
    await Bun.sleep(100);
  }
  return {
    baseUrl,
    stop: async () => {
      proc.kill();
      await proc.exited.catch(() => {});
    },
  };
}

async function api(
  baseUrl: string,
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<{ status: number; body: any }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
  const res = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  return { status: res.status, body: await res.json() };
}

async function main(): Promise<void> {
  const dir = mkdtempSync(join(tmpdir(), "risk-smoke-"));
  const dbPath = join(dir, "risk.sqlite");
  const port = 20_000 + Math.floor(Math.random() * 20_000);

  let server = await startServer(port, dbPath);
  try {
    const created = await api(server.baseUrl, "POST", "/v1/games", {
      body: { name: "Alice", color: "red" },
    });
    assert(created.status === 201, `create game: ${created.status}`);
    const gameId: string = created.body.game.id;
    const hostId: string = created.body.player.id;
    const tokenByPlayer: Record<string, string> = { [hostId]: created.body.capability };

    const joined = await api(server.baseUrl, "POST", `/v1/games/${gameId}/players`, {
      body: { name: "Bob", color: "blue" },
    });
    assert(joined.status === 201, `join: ${joined.status}`);
    tokenByPlayer[joined.body.player.id] = joined.body.capability;

    const started = await api(server.baseUrl, "POST", `/v1/games/${gameId}/start`, {
      token: tokenByPlayer[hostId],
      body: {},
    });
    assert(started.status === 200, `start: ${started.status}`);

    const meta = await api(server.baseUrl, "GET", `/v1/games/${gameId}`);
    const active: string = meta.body.activePlayerId;
    const decision = await api(server.baseUrl, "GET", `/v1/games/${gameId}/decision`, {
      token: tokenByPlayer[active],
    });
    assert(decision.status === 200, `decision: ${decision.status}`);
    const reinforce = decision.body.legalActions.find((a: any) => a.type === "reinforce");
    assert(reinforce, "expected a reinforce action");

    const ack = await api(server.baseUrl, "POST", `/v1/games/${gameId}/commands`, {
      token: tokenByPlayer[active],
      body: {
        commandId: "smoke-cmd",
        turnId: decision.body.turn.id,
        action: { type: "reinforce", territoryId: reinforce.territoryIds[0], armies: 2 },
      },
    });
    assert(ack.status === 200 && ack.body.status === "accepted", `command: ${ack.status}`);
    const committedOffset: string = ack.body.sourceOffset;

    const board = await api(server.baseUrl, "GET", `/v1/games/${gameId}/board`);
    assert(board.body.sourceThroughOffset === committedOffset, "board watermark != ack offset");

    // Turn stream: the active player has a durable TurnAvailable wake.
    const turns = await api(server.baseUrl, "GET", `/v1/games/${gameId}/players/me/turns`, {
      token: tokenByPlayer[active],
    });
    assert(
      turns.body.notifications.some((n: any) => n.type === "TurnAvailable"),
      "no TurnAvailable wake for active player",
    );

    // The React board SPA is served and mounts on #root.
    const spa = await fetch(`${server.baseUrl}/`);
    const html = await spa.text();
    assert(spa.status === 200 && html.includes('id="root"'), "board SPA did not render");

    // Authorization: another player's token cannot play the active turn.
    const inactive = active === hostId ? joined.body.player.id : hostId;
    const forbidden = await api(server.baseUrl, "POST", `/v1/games/${gameId}/commands`, {
      token: tokenByPlayer[inactive],
      body: { commandId: "nope", turnId: decision.body.turn.id, action: { type: "end-turn" } },
    });
    assert(forbidden.status === 409, `expected NOT_YOUR_TURN, got ${forbidden.status}`);

    // --- restart against the same database file ---
    await server.stop();
    server = await startServer(port, dbPath);

    const metaAfter = await api(server.baseUrl, "GET", `/v1/games/${gameId}`);
    assert(metaAfter.body.status === "playing", "status lost across restart");

    const recovered = await api(server.baseUrl, "GET", `/v1/games/${gameId}/commands/smoke-cmd`, {
      token: tokenByPlayer[hostId],
    });
    assert(recovered.body.sourceOffset === committedOffset, "command recovery lost across restart");

    const boardAfter = await api(server.baseUrl, "GET", `/v1/games/${gameId}/board`);
    assert(
      boardAfter.body.sourceThroughOffset === committedOffset,
      "board watermark lost across restart",
    );

    // Original capability still authenticates after restart.
    const decisionAfter = await api(server.baseUrl, "GET", `/v1/games/${gameId}/decision`, {
      token: tokenByPlayer[hostId],
    });
    assert(decisionAfter.status === 200, "capability verifier lost across restart");

    console.log(
      "✓ risk-demo HTTP smoke passed (create/join/start/command/board/authz + SQLite restart)",
    );
  } finally {
    await server.stop();
    rmSync(dir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error instanceof SmokeError ? `✗ smoke failed: ${error.message}` : error);
  process.exit(1);
});
