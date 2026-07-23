/**
 * SQLite durability + restart proof. Runs under Bun's test runner (`bun test`)
 * because it uses `bun:sqlite`; the vitest suite (`src/**`) stays storage-agnostic.
 *
 *   bun test server/persistence.test.ts
 */

import { afterAll, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createStreamProtocol } from "@streamsy/core";
import { createSqliteStorageAdapter } from "@streamsy/storage-sqlite";

import { buildApp, type App } from "./app.ts";
import { createSqliteStores } from "./sqlite-store.ts";
import { createSeededRng } from "../src/rng.ts";

const dir = mkdtempSync(join(tmpdir(), "risk-sqlite-"));
const dbPath = join(dir, "risk.sqlite");

afterAll(() => rmSync(dir, { recursive: true, force: true }));

function openApp(): { app: App; close: () => void; db: import("bun:sqlite").Database } {
  const adapter = createSqliteStorageAdapter({ filename: dbPath });
  const protocol = createStreamProtocol({ storage: { adapter } });
  const stores = createSqliteStores(adapter.state.db);
  const app = buildApp({ protocol, stores, rng: createSeededRng(11) });
  return { app, close: () => adapter.close(), db: adapter.state.db };
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

test("SQLite preserves events, projections, command retries, and capabilities across restart", async () => {
  const first = openApp();

  const created = await call(first.app, "POST", "/v1/games", {
    body: { name: "Alice", color: "red" },
  });
  const gameId: string = created.body.game.id;
  const hostId: string = created.body.player.id;
  const hostToken: string = created.body.capability;
  const secret = hostToken.split("_")[2]!;

  const joined = await call(first.app, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  const tokenByPlayer: Record<string, string> = {
    [hostId]: hostToken,
    [joined.body.player.id]: joined.body.capability,
  };

  await call(first.app, "POST", `/v1/games/${gameId}/start`, { token: hostToken, body: {} });

  const meta = await call(first.app, "GET", `/v1/games/${gameId}`);
  const active: string = meta.body.activePlayerId;
  const decision = await call(first.app, "GET", `/v1/games/${gameId}/decision`, {
    token: tokenByPlayer[active]!,
  });
  const reinforce = decision.body.legalActions.find((a: any) => a.type === "reinforce");
  const commandBody = {
    commandId: "persist-cmd",
    turnId: decision.body.turn.id,
    action: { type: "reinforce", territoryId: reinforce.territoryIds[0], armies: 2 },
  };
  const ack = await call(first.app, "POST", `/v1/games/${gameId}/commands`, {
    token: tokenByPlayer[active]!,
    body: commandBody,
  });
  expect(ack.status).toBe(200);
  const committedOffset: string = ack.body.sourceOffset;

  // Materialize the board so its projection stream is persisted too.
  const boardBefore = await call(first.app, "GET", `/v1/games/${gameId}/board`);
  expect(boardBefore.body.sourceThroughOffset).toBe(committedOffset);

  // No raw token material is present anywhere in the database.
  const dump = JSON.stringify(first.db.query("select * from risk_capabilities").all() as unknown[]);
  expect(dump.includes(secret)).toBe(false);
  expect(dump.includes(hostToken)).toBe(false);

  first.close();

  // --- restart: reopen the same file ---------------------------------------
  const second = openApp();

  // Canonical events survived.
  const metaAfter = await call(second.app, "GET", `/v1/games/${gameId}`);
  expect(metaAfter.body.status).toBe("playing");
  expect(metaAfter.body.activePlayerId).toBe(active);

  // The capability verifier survived: the original token still authenticates.
  const decisionAfter = await call(second.app, "GET", `/v1/games/${gameId}/decision`, {
    token: hostToken,
  });
  expect(decisionAfter.status).toBe(200);

  // The board projection rebuilds to the same watermark.
  const boardAfter = await call(second.app, "GET", `/v1/games/${gameId}/board`);
  expect(boardAfter.body.sourceThroughOffset).toBe(committedOffset);
  expect(boardAfter.body.territories).toEqual(boardBefore.body.territories);

  // A retry of the persisted command is still idempotent after restart.
  const retry = await call(second.app, "POST", `/v1/games/${gameId}/commands`, {
    token: tokenByPlayer[active]!,
    body: commandBody,
  });
  expect(retry.status).toBe(200);
  expect(retry.body.status).toBe("duplicate");
  expect(retry.body.sourceOffset).toBe(committedOffset);

  second.close();
});

test("turn-stream cursor resume and rebuild idempotency survive restart", async () => {
  const first = openApp();
  const created = await call(first.app, "POST", "/v1/games", { body: { name: "A", color: "red" } });
  const gameId: string = created.body.game.id;
  const hostToken: string = created.body.capability;
  const hostId: string = created.body.player.id;
  await call(first.app, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "B", color: "blue" },
  });
  await call(first.app, "POST", `/v1/games/${gameId}/start`, { token: hostToken, body: {} });

  // Read the host's own durable turn stream and remember the cursor. (The host
  // has one wake if it drew the first turn, otherwise zero — the invariant below
  // holds either way.)
  const initial = await call(first.app, "GET", `/v1/games/${gameId}/players/me/turns`, {
    token: hostToken,
  });
  const cursor: string = initial.body.cursor;
  const wakeCount: number = initial.body.notifications.length;
  first.close();

  // --- restart: reopen the same database file ---
  const second = openApp();

  // Resuming from the saved cursor yields nothing new (no missed/duplicate wake).
  const resumed = await call(
    second.app,
    "GET",
    `/v1/games/${gameId}/players/me/turns?offset=${cursor}`,
    { token: hostToken },
  );
  expect(resumed.body.notifications).toHaveLength(0);

  // Reading from the start after restart still yields the SAME wakes: the notifier
  // rebuilt from canonical history without appending duplicates.
  const rebuilt = await call(second.app, "GET", `/v1/games/${gameId}/players/me/turns`, {
    token: hostToken,
  });
  expect(rebuilt.body.notifications).toHaveLength(wakeCount);
  for (const note of rebuilt.body.notifications) expect(note.playerId).toBe(hostId);

  second.close();
});
