/* oxlint-disable effecttsgo/async-function -- This executable script is a bounded Promise-native Bun/Node adapter over the demo's public HTTP and application APIs. */
/* oxlint-disable effecttsgo/crypto-random-uuid, effecttsgo/global-console, effecttsgo/global-date, effecttsgo/global-fetch, effecttsgo/new-promise, effecttsgo/node-builtin-import -- This Bun demo executable directly owns random command ids, HTTP/timing, subprocess lifecycle, terminal output, and Node filesystem/process adapters. */
/**
 * One-command, watchable Risk demo.
 *
 * Bootstraps missing workspace builds, launches the real SQLite-backed server,
 * creates a two-player `Hex Domination` game, and lets the existing HTTP-only
 * scripted bots play it on a procedurally generated hex map. The spectator board remains
 * available until Ctrl-C, including after a winner is decided.
 *
 *  adds one shape the loop has to respect: an attack *stops* the attacker's
 * turn until the defender rolls. The orchestrator therefore follows canonical
 * state rather than assuming a turn is one actor's uninterrupted run — and if a
 * defending bot ever failed to answer, the server's own 15-second timeout would
 * close the combat without it.
 */

import { existsSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import type { BotAction } from "../server/demo/bot.ts";

const packageDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const rootDir = resolve(packageDir, "../..");

export const REQUIRED_WORKSPACE_DISTS = [
  "packages/core/dist/index.js",
  "packages/experimental/dist/command.js",
  "packages/experimental/dist/derived.js",
  "packages/experimental/dist/projection.js",
  "packages/json/dist/index.js",
  "packages/state/dist/index.js",
  "packages/storage-sqlite/dist/index.js",
] as const;

export function missingWorkspaceDists(
  workspaceRoot = rootDir,
  fileExists: (path: string) => boolean = existsSync,
): string[] {
  return REQUIRED_WORKSPACE_DISTS.filter((path) => !fileExists(join(workspaceRoot, path)));
}

export function spectatorUrl(baseUrl: string, gameId: string): string {
  const url = new URL(baseUrl);
  url.searchParams.set("game", gameId);
  return url.toString();
}

async function ensureWorkspaceDists(): Promise<void> {
  const missing = missingWorkspaceDists();
  if (missing.length === 0) return;

  console.log("Workspace build outputs are missing; building @streamsy/* packages…");
  const build = Bun.spawn(["bun", "run", "build"], {
    cwd: rootDir,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  });
  const exitCode = await build.exited;
  if (exitCode !== 0) throw new Error(`workspace build failed with exit code ${exitCode}`);

  const stillMissing = missingWorkspaceDists();
  if (stillMissing.length > 0) {
    throw new Error(`workspace build did not create: ${stillMissing.join(", ")}`);
  }
}

export async function findFreePort(): Promise<number> {
  return await new Promise((resolvePort, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      try {
        const { port } = Schema.decodeUnknownSync(Schema.Struct({ port: Schema.Finite }))(
          probe.address(),
        );
        probe.close((error) => (error ? reject(error) : resolvePort(port)));
      } catch {
        probe.close();
        reject(new Error("could not allocate a demo port"));
      }
    });
  });
}

interface HttpResult {
  status: number;
  body: any;
}

async function api(
  baseUrl: string,
  method: string,
  path: string,
  options: { token?: string; body?: unknown; accept?: string } = {},
): Promise<HttpResult> {
  // The actions resource streams by default; a JSON reader says so explicitly.
  const headers = new Headers({
    "content-type": "application/json",
    accept: options.accept ?? "application/json",
  });
  if (options.token) headers.set("authorization", `Bearer ${options.token}`);
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  return { status: response.status, body: await response.json().catch(() => ({})) };
}

function requireStatus(result: HttpResult, expected: number, operation: string): void {
  if (result.status !== expected) {
    throw new Error(`${operation} failed (${result.status}): ${JSON.stringify(result.body)}`);
  }
}

async function waitForServer(baseUrl: string, exited: Promise<number>): Promise<void> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const ready = await Promise.race([
      fetch(`${baseUrl}/healthz`)
        .then((response) => response.ok)
        .catch(() => false),
      exited.then((code) => {
        throw new Error(`server exited before becoming ready (${code})`);
      }),
    ]);
    if (ready) return;
    await Bun.sleep(100);
  }
  throw new Error("server did not become ready within 15 seconds");
}

async function forward(
  stream: ReadableStream<Uint8Array>,
  destination: { write(chunk: Uint8Array): boolean },
): Promise<void> {
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return;
    destination.write(value);
  }
}

interface DemoPlayer {
  id: string;
  token: string;
  name: string;
}

/**
 * Both demo seats are machine players: the one-command demo has
 * nobody at a keyboard, so a bot must also be able to answer the defence
 * interrupt, not merely take its own turn.
 */
export const DEMO_HOST_REQUEST = {
  name: "Ada",
  color: "#e05a47",
  controller: "bot",
} as const;
export const DEMO_GUEST_REQUEST = {
  name: "Bob",
  color: "#3b82f6",
  controller: "bot",
} as const;

export const DEMO_LEAD_IN_MS = 10_000;
/**
 * A game is roughly 250–550 commands, so a one-second pace keeps a
 * complete watchable game to something between five and ten minutes.
 */
export const DEMO_COMMAND_PACE_MS = 1_000;

function actionLabel(action: BotAction): string {
  return action.type.replaceAll("-", " ");
}

async function playGame(baseUrl: string, gameId: string, players: DemoPlayer[]): Promise<void> {
  const { createBot } = await import("../server/demo/bot.ts");
  const call = (method: string, path: string, options = {}) => api(baseUrl, method, path, options);
  const bots = new Map(
    players.map((player) => [
      player.id,
      createBot({
        call,
        gameId,
        playerId: player.id,
        token: player.token,
        state: {},
        onCommandCommitted: async (action) => {
          console.log(
            `  ✓ ${player.name}: ${actionLabel(action)} committed · next action in ${DEMO_COMMAND_PACE_MS / 1_000}s`,
          );
          await Bun.sleep(DEMO_COMMAND_PACE_MS);
        },
      }),
    ]),
  );

  console.log(`Bots start in ${DEMO_LEAD_IN_MS / 1_000} seconds — open the board now.\n`);
  await Bun.sleep(DEMO_LEAD_IN_MS);

  const started = Date.now();
  let announced = "";
  for (let step = 1; step <= 4_000; step += 1) {
    const meta = await api(baseUrl, "GET", `/v1/games/${gameId}`);
    requireStatus(meta, 200, "read game");
    if (meta.body.status === "finished") {
      const winner = players.find((player) => player.id === meta.body.winnerId)?.name ?? "unknown";
      const minutes = ((Date.now() - started) / 60_000).toFixed(1);
      console.log(`\n🏆 ${winner} won after ${meta.body.round} rounds (${minutes} minutes).`);
      console.log("The final board remains live. Press Ctrl-C when you are done observing.");
      return;
    }

    // A pending defence is the only out-of-turn decision, and it belongs to the
    // defender, not the player whose turn it is.
    const pending = meta.body.pendingInteraction;
    if (pending?.type === "defense") {
      const defender = bots.get(pending.defenderId);
      const name = players.find((player) => player.id === pending.defenderId)?.name ?? "defender";
      if (!defender) throw new Error(`no demo bot for defender ${pending.defenderId}`);
      await defender.awaitTurn(0);
      if (await defender.defend()) console.log(`  ⚄ ${name} rolled the defence`);
      continue;
    }

    const activeId: string = meta.body.activePlayerId;
    const active = players.find((player) => player.id === activeId);
    const bot = bots.get(activeId);
    if (!active || !bot) throw new Error(`no demo bot for active player ${activeId}`);

    await bot.awaitTurn(0);
    const heading = `${meta.body.round}:${activeId}`;
    if (heading !== announced) {
      announced = heading;
      console.log(`→ Round ${meta.body.round}: ${active.name} is playing`);
    }
    await bot.playTurn();
  }
  throw new Error("demo bots exceeded the 4000-step safety limit");
}

async function run(): Promise<void> {
  await ensureWorkspaceDists();

  const dbPath = join(tmpdir(), `streamsy-risk-demo-${crypto.randomUUID()}.sqlite`);
  const port = await findFreePort();
  const baseUrl = `http://127.0.0.1:${port}`;
  const server = Bun.spawn(["bun", "server/index.ts"], {
    cwd: packageDir,
    env: {
      ...process.env,
      NODE_ENV: "production",
      PORT: String(port),
      DB_PATH: dbPath,
      DELETE_DB_ON_EXIT: "1",
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  void forward(server.stdout, process.stdout);
  void forward(server.stderr, process.stderr);

  let stopping = false;
  let resolveSignal: (() => void) | undefined;
  const signal = new Promise<void>((resolveSignalPromise) => {
    resolveSignal = resolveSignalPromise;
  });
  const requestStop = (): void => {
    if (stopping) return;
    stopping = true;
    resolveSignal?.();
  };
  process.once("SIGINT", requestStop);
  process.once("SIGTERM", requestStop);

  try {
    await waitForServer(baseUrl, server.exited);
    const created = await api(baseUrl, "POST", "/v1/games", {
      body: DEMO_HOST_REQUEST,
    });
    requireStatus(created, 201, "create game");
    const gameId: string = created.body.game.id;
    const host: DemoPlayer = {
      id: created.body.player.id,
      token: created.body.capability,
      name: created.body.player.name,
    };

    const joined = await api(baseUrl, "POST", `/v1/games/${gameId}/players`, {
      body: DEMO_GUEST_REQUEST,
    });
    requireStatus(joined, 201, "join game");
    const guest: DemoPlayer = {
      id: joined.body.player.id,
      token: joined.body.capability,
      name: joined.body.player.name,
    };
    const started = await api(baseUrl, "POST", `/v1/games/${gameId}/start`, {
      token: host.token,
      body: {},
    });
    requireStatus(started, 200, "start game");

    const url = spectatorUrl(baseUrl, gameId);
    console.log("\n╭──────────────────────────────────────────────────────────────╮");
    console.log("│  STREAMSY RISK — LIVE SPECTATOR BOARD                       │");
    console.log(`│  ${url.padEnd(58)}│`);
    console.log("╰──────────────────────────────────────────────────────────────╯\n");
    console.log(
      "Ada and Bob are scripted HTTP bots playing Hex Domination on a seeded hex map:\n" +
        "declared attacks, recorded dice, and an out-of-turn defence roll each throw.\n" +
        "The server stays up until Ctrl-C.\n",
    );

    const playing = playGame(baseUrl, gameId, [host, guest]);
    const playFailure = playing.then(
      () => new Promise<never>(() => {}),
      (error) => Promise.reject(error),
    );
    await Promise.race([
      signal,
      playFailure,
      server.exited.then((code) => {
        if (!stopping) throw new Error(`server exited unexpectedly (${code})`);
      }),
    ]);
  } finally {
    stopping = true;
    process.removeListener("SIGINT", requestStop);
    process.removeListener("SIGTERM", requestStop);
    server.kill("SIGTERM");
    await server.exited.catch(() => {});
    for (const path of [dbPath, `${dbPath}-shm`, `${dbPath}-wal`]) rmSync(path, { force: true });
    console.log("\nStreamsy Risk demo stopped; temporary SQLite data removed.");
  }
}

if (import.meta.main) {
  run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
