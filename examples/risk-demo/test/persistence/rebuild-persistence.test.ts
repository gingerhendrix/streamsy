/**
 * SQLite durability of a board-generation cutover across restart.
 *
 *   bun test test/persistence/rebuild-persistence.test.ts
 */

import { afterAll, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createStreamProtocol } from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";

import { buildApp, type App } from "../../server/http/app.ts";
import { createSqliteStores } from "../../server/persistence/sqlite-store.ts";
import type { Stores } from "../../server/persistence/stores.ts";
import { rebuildBoardGeneration } from "../../server/game/rebuild.ts";
import { createSeededRng } from "../../src/domain/rng.ts";

const dir = mkdtempSync(join(tmpdir(), "risk-rebuild-"));
const dbPath = join(dir, "risk.sqlite");

afterAll(() => rmSync(dir, { recursive: true, force: true }));

interface Opened {
  app: App;
  protocol: StreamProtocolFactory;
  stores: Stores;
  close: () => void;
}

function openApp(): Opened {
  const adapter = createSqliteStorageAdapter({ filename: dbPath });
  const protocol = createStreamProtocol({ storage: { adapter } });
  const stores = createSqliteStores(adapter.state.db);
  const app = buildApp({ protocol, stores, rng: createSeededRng(7) });
  return { app, protocol, stores, close: () => adapter.close() };
}

const BASE = "http://risk.test";

async function call(
  app: App,
  method: string,
  path: string,
  options: { token?: string; body?: unknown } = {},
): Promise<{ status: number; body: any }> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (options.token) headers.authorization = `Bearer ${options.token}`;
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
  const first = openApp();

  const created = await call(first.app, "POST", "/v1/games", {
    body: { name: "Alice", color: "red" },
  });
  const gameId: string = created.body.game.id;
  const hostToken: string = created.body.capability;
  const hostId: string = created.body.player.id;
  const joined = await call(first.app, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  const tokenByPlayer: Record<string, string> = {
    [hostId]: hostToken,
    [joined.body.player.id]: joined.body.capability,
  };
  await call(first.app, "POST", `/v1/games/${gameId}/start`, { token: hostToken, body: {} });
  const active: string = (await call(first.app, "GET", `/v1/games/${gameId}`)).body.activePlayerId;
  const decision = await call(first.app, "GET", `/v1/games/${gameId}/decision`, {
    token: tokenByPlayer[active]!,
  });
  const reinforce = decision.body.legalActions.find((a: any) => a.type === "reinforce");
  await call(first.app, "POST", `/v1/games/${gameId}/commands`, {
    token: tokenByPlayer[active]!,
    body: {
      commandId: "rein-1",
      turnId: decision.body.turn.id,
      action: {
        type: "reinforce",
        territoryId: reinforce.territoryIds[0],
        armies: reinforce.maxArmies,
      },
    },
  });

  const before = await call(first.app, "GET", `/v1/games/${gameId}/board`);
  expect(before.body.generation).toBe("v1");

  const result = await rebuildBoardGeneration(
    { protocol: first.protocol, stores: first.stores },
    gameId,
  );
  expect(result.status).toBe("cutover");
  expect(result.toGeneration).toBe("v2");
  const cutoverBoard = await call(first.app, "GET", `/v1/games/${gameId}/board`);
  expect(cutoverBoard.body.generation).toBe("v2");
  first.close();

  // --- restart: reopen the same database file ---
  const second = openApp();

  // The active pointer survived: reads use v2 with the same board + watermark.
  const after = await call(second.app, "GET", `/v1/games/${gameId}/board`);
  expect(after.body.generation).toBe("v2");
  expect(after.body.sourceThroughOffset).toBe(before.body.sourceThroughOffset);
  expect(after.body.territories).toEqual(before.body.territories);

  // Both generations are still recorded; v1 retired but retained, v2 active.
  const gens = second.stores.generations.list(gameId);
  expect(gens.map((g) => g.generation)).toEqual(["v1", "v2"]);
  expect(gens.find((g) => g.generation === "v1")!.status).toBe("retired");
  expect(gens.find((g) => g.generation === "v2")!.status).toBe("active");
  expect(second.stores.games.get(gameId)!.generation).toBe("v2");

  second.close();
});
