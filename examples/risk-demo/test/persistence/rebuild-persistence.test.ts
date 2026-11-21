/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
/* oxlint-disable effecttsgo/node-builtin-import -- This Bun SQLite rebuild suite owns isolated temporary filesystem fixtures. */
/**
 * SQLite durability of a board-generation cutover across restart.
 *
 *   bun test test/persistence/rebuild-persistence.test.ts
 */

import { afterAll, expect, test } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildApp, type App } from "../../server/http/app.ts";
import { openBunStorage } from "../../server/persistence/bun.ts";
import { rebuildBoardGeneration } from "../../server/game/rebuild.ts";
import { createSeededRng } from "../../src/domain/rng.ts";

const dir = mkdtempSync(join(tmpdir(), "risk-rebuild-"));
const dbPath = join(dir, "risk.sqlite");

afterAll(() => rmSync(dir, { recursive: true, force: true }));

async function openApp() {
  const storage = await openBunStorage(dbPath);
  const app = buildApp({ ...storage, rng: createSeededRng(11) });
  return {
    ...storage,
    app,
    async close() {
      await app.close();
      await storage.close();
    },
  };
}

const BASE = "http://risk.test";

async function call(
  app: App,
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<{ status: number; body: any }> {
  const headers = new Headers({ "content-type": "application/json" });
  if (options.token) headers.set("authorization", `Bearer ${options.token}`);
  const res = await app.fetch(
    new Request(`${BASE}${path}`, {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    }),
  );
  return { status: res.status, body: await res.json() };
}

test("a board-generation cutover survives a SQLite restart and keeps the old generation", async () => {
  const first = await openApp();

  const created = await call(first.app, "POST", "/v1/games", {
    body: { name: "Alice", color: "red" },
  });
  const gameId: string = created.body.game.id;
  const hostToken: string = created.body.capability;
  const hostId: string = created.body.player.id;
  const joined = await call(first.app, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  const tokenByPlayer = new Map([
    [hostId, hostToken],
    [joined.body.player.id, joined.body.capability],
  ]);
  await call(first.app, "POST", `/v1/games/${gameId}/start`, { token: hostToken, body: {} });
  const active: string = (await call(first.app, "GET", `/v1/games/${gameId}`)).body.activePlayerId;
  const decision = await call(first.app, "GET", `/v1/games/${gameId}/decision`, {
    token: tokenByPlayer.get(active)!,
  });
  const reinforce = decision.body.legalMoves.find((a: any) => a.type === "reinforce");
  await call(first.app, "POST", `/v1/games/${gameId}/commands`, {
    token: tokenByPlayer.get(active)!,
    body: {
      commandId: "rein-1",
      turnId: decision.body.turn.id,
      action: {
        type: "reinforce",
        placements: [{ territoryId: reinforce.territoryIds[0], armies: reinforce.pool }],
      },
    },
  });

  const before = await call(first.app, "GET", `/v1/games/${gameId}/board`);
  expect(before.body.generation).toBe("board1");

  const result = await rebuildBoardGeneration(first, gameId);
  expect(result.status).toBe("cutover");
  expect(result.toGeneration).toBe("board2");
  const cutoverBoard = await call(first.app, "GET", `/v1/games/${gameId}/board`);
  expect(cutoverBoard.body.generation).toBe("board2");
  await first.close();

  // --- restart: reopen the same database file ---
  const second = await openApp();

  // The active pointer survived with the same board and watermark.
  const after = await call(second.app, "GET", `/v1/games/${gameId}/board`);
  expect(after.body.generation).toBe("board2");
  expect(after.body.sourceThroughOffset).toBe(before.body.sourceThroughOffset);
  expect(after.body.territories).toEqual(before.body.territories);

  // Both generations are recorded: the first retired, the second active.
  const gens = second.stores.generations.list(gameId);
  expect(gens.map((g) => g.generation)).toEqual(["board1", "board2"]);
  expect(gens.find((g) => g.generation === "board1")!.status).toBe("retired");
  expect(gens.find((g) => g.generation === "board2")!.status).toBe("active");
  expect(second.stores.games.get(gameId)!.generation).toBe("board2");
  expect(second.stores.games.get(gameId)!.projectionStreamId).toBe(after.body.boardStreamId);
  const live = await second.app.fetch(new Request(`${BASE}/streams/${after.body.boardStreamId}`));
  expect(live.status).toBe(200);
  expect(await live.json()).not.toHaveLength(0);
  const retired = await second.app.fetch(
    new Request(`${BASE}/streams/${before.body.boardStreamId}`),
  );
  expect(retired.status).toBe(404);

  await second.close();
});
