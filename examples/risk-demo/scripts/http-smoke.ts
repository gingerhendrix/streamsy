/**
 * End-to-end HTTP smoke for the durable Risk API.
 *
 * Spawns the real Bun server against a temp SQLite file, drives create → join →
 * start → decision → command → board → agent routes → duplicate retry over HTTP,
 * then kills the server, respawns it against the SAME database file, and proves events, board projection,
 * idempotent command retries and capability verifiers all survived the restart.
 *
 *   bun run scripts/http-smoke.ts
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { readActionsBatches, type ActionsBatch } from "../src/application/actions-stream.ts";

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
  options: { token?: string; body?: unknown; accept?: string } = {},
): Promise<{ status: number; contentType: string; body: any }> {
  // The actions resource streams unless a caller negotiates the JSON reading.
  const headers: Record<string, string> = {
    "content-type": "application/json",
    accept: options.accept ?? "application/json",
  };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
  const res = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  return {
    status: res.status,
    contentType: res.headers.get("content-type") ?? "",
    body: await res.json(),
  };
}

/** Open the actions resource as what it is: a Server-Sent Events stream. */
function openActions(
  baseUrl: string,
  gameId: string,
  token: string,
  offset?: string,
): Promise<Response> {
  const query = offset ? `?offset=${encodeURIComponent(offset)}` : "";
  return fetch(`${baseUrl}/v1/games/${gameId}/players/me/actions${query}`, {
    headers: { accept: "text/event-stream", authorization: `Bearer ${token}` },
  });
}

/**
 * Batch-by-batch reader over one connection. `next` answers `null` when nothing
 * arrives within `timeoutMs`, which is how "the connection is still holding" is
 * observed from the outside.
 */
function actionsReader(response: Response) {
  const batches = readActionsBatches(response);
  return {
    async next(timeoutMs: number): Promise<ActionsBatch | null> {
      const timer = new Promise<null>((resolve) => setTimeout(() => resolve(null), timeoutMs));
      const arrival = batches.next().then((result) => (result.done ? null : result.value));
      return Promise.race([arrival, timer]);
    },
    async close(): Promise<void> {
      await batches.return(undefined as never).catch(() => {});
      await response.body?.cancel().catch(() => {});
    },
  };
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
    const reinforce = decision.body.legalMoves.find((a: any) => a.type === "reinforce");
    assert(reinforce, "expected a reinforce action");

    const commandBody = {
      commandId: "smoke-cmd",
      turnId: decision.body.turn.id,
      action: {
        type: "reinforce",
        placements: [{ territoryId: reinforce.territoryIds[0], armies: reinforce.pool }],
      },
    };
    const ack = await api(server.baseUrl, "POST", `/v1/games/${gameId}/commands`, {
      token: tokenByPlayer[active],
      body: commandBody,
    });
    assert(ack.status === 200 && ack.body.status === "accepted", `command: ${ack.status}`);
    const committedOffset: string = ack.body.eventOffset;

    const board = await api(server.baseUrl, "GET", `/v1/games/${gameId}/board`);
    assert(board.body.sourceThroughOffset === committedOffset, "board watermark != ack offset");

    // The current agent contract is header-only and action-stream driven.
    const agentGame = await api(server.baseUrl, "POST", "/v1/games", {
      body: { name: "Agent Host", mapSeed: "http-smoke-agent-routes" },
    });
    assert(agentGame.status === 201, `create agent game: ${agentGame.status}`);
    const agentGameId: string = agentGame.body.game.id;
    const firstAgent = await api(server.baseUrl, "POST", `/v1/games/${agentGameId}/agent-seats`, {
      token: agentGame.body.capability,
      body: { playerId: agentGame.body.player.id },
    });
    const agentJoin = await api(server.baseUrl, "POST", `/v1/games/${agentGameId}/agent-seats`, {
      token: agentGame.body.capability,
      body: { name: "Agent 2" },
    });
    assert(agentJoin.status === 201, `join agent: ${agentJoin.status}`);

    const agentStarted = await api(server.baseUrl, "POST", `/v1/games/${agentGameId}/start`, {
      token: agentGame.body.capability,
      body: {},
    });
    assert(agentStarted.status === 200, `start agent game: ${agentStarted.status}`);
    const map = await api(server.baseUrl, "GET", `/v1/games/${agentGameId}/map`);
    assert(map.status === 200 && map.body.territories.length > 0, "agent map route failed");
    const agentTokens = [firstAgent.body.seat.token, agentJoin.body.seat.token];
    const actionReads = await Promise.all(
      agentTokens.map((token) =>
        api(server.baseUrl, "GET", `/v1/games/${agentGameId}/players/me/actions`, { token }),
      ),
    );
    assert(
      actionReads.some((read) =>
        read.body.messages.some((message: any) => message.type === "ActionRequired"),
      ),
      "no actionable agent message after start",
    );

    // The published reading of that resource is SSE, over a real socket: the
    // backlog arrives at once, framed as data/control, and a reconnection from
    // the offset the control frame named holds open instead of answering.
    const seatToken = agentTokens[0]!;
    const opened = await openActions(server.baseUrl, agentGameId, seatToken);
    assert(opened.status === 200, `actions stream returned ${opened.status}`);
    assert(
      (opened.headers.get("content-type") ?? "").includes("text/event-stream"),
      `actions stream is ${opened.headers.get("content-type")}`,
    );
    assert(
      opened.headers.get("cache-control") === "no-store" &&
        opened.headers.get("referrer-policy") === "no-referrer",
      "the actions stream is cacheable or referable",
    );
    const first = actionsReader(opened);
    const backlog = await first.next(5_000);
    await first.close();
    assert(backlog !== null, "the actions stream delivered no opening batch");
    assert(
      backlog.messages.some((message: any) => message.type === "ActionRequired"),
      "the actions stream backlog carried no ActionRequired",
    );
    assert(typeof backlog.nextOffset === "string", "no control frame cursor");

    const resumed = actionsReader(
      await openActions(server.baseUrl, agentGameId, seatToken, backlog.nextOffset),
    );
    // Reconnecting re-states the cursor immediately and reports nothing new…
    const caughtUp = await resumed.next(5_000);
    assert(
      caughtUp !== null && caughtUp.messages.length === 0,
      "a resumed actions stream replayed or lost messages",
    );
    assert(caughtUp.nextOffset === backlog.nextOffset, "the resumed cursor moved");
    // …and then holds the connection instead of answering empty and closing.
    const held = await resumed.next(500);
    assert(held === null, "a caught-up actions stream returned instead of holding");
    await resumed.close();

    const waited = await api(
      server.baseUrl,
      "GET",
      `/v1/games/${agentGameId}/players/me/actions?wait=1000`,
      { token: seatToken },
    );
    assert(waited.status === 400, `the removed long poll returned ${waited.status}`);

    // Seat-scoped reads are never cached, and the private actions stream is not
    // reachable through the public spectator facade.
    const decisionHeaders = await fetch(`${server.baseUrl}/v1/games/${gameId}/decision`, {
      headers: { authorization: `Bearer ${tokenByPlayer[active]}` },
    });
    assert(
      decisionHeaders.headers.get("cache-control") === "no-store",
      "decision response was cacheable",
    );
    const leakedStream = await fetch(
      `${server.baseUrl}/streams/games/${agentGameId}/players/${agentGame.body.player.id}/actions`,
    );
    assert(leakedStream.status === 404, "the private actions stream is publicly readable");

    // A host may delegate its own seat and no other.
    const foreignDelegation = await api(
      server.baseUrl,
      "POST",
      `/v1/games/${agentGameId}/agent-seats`,
      { token: agentGame.body.capability, body: { playerId: "p_not_the_host" } },
    );
    assert(
      foreignDelegation.status === 403,
      `foreign seat delegation returned ${foreignDelegation.status}`,
    );

    const spa = await fetch(`${server.baseUrl}/`);
    const html = await spa.text();
    assert(spa.status === 200 && html.includes('id="root"'), "board SPA did not render");

    // Authorization: another player's token cannot play the active turn.
    const inactive = active === hostId ? joined.body.player.id : hostId;
    const forbidden = await api(server.baseUrl, "POST", `/v1/games/${gameId}/commands`, {
      token: tokenByPlayer[inactive],
      body: {
        commandId: "nope",
        turnId: decision.body.turn.id,
        action: { type: "skip-fortifications" },
      },
    });
    assert(forbidden.status === 409, `expected NOT_YOUR_TURN, got ${forbidden.status}`);

    // --- restart against the same database file ---
    await server.stop();
    server = await startServer(port, dbPath);

    const metaAfter = await api(server.baseUrl, "GET", `/v1/games/${gameId}`);
    assert(metaAfter.body.status === "playing", "status lost across restart");

    const retry = await api(server.baseUrl, "POST", `/v1/games/${gameId}/commands`, {
      token: tokenByPlayer[active],
      body: commandBody,
    });
    assert(retry.body.status === "duplicate", "command retry was not deduplicated after restart");
    assert(
      retry.body.eventOffset === committedOffset,
      "command retry offset changed after restart",
    );

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
      "✓ risk-demo HTTP smoke passed (create/join/start/command/board/agent routes/actions SSE/authz/seat-scoped headers + SQLite restart)",
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
