/**
 * Deterministic long-poll wake: a player blocked on `/me/turns?wait=…` is woken
 * by the durable turn stream exactly when control passes to them — no polling
 * loop, no missed wake.
 */

import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";

import { buildApp, type App } from "../../server/http/app.ts";
import { createInMemoryStores } from "../../server/persistence/stores.ts";
import { createBot } from "../../server/demo/bot.ts";
import { createSeededRng } from "../../src/domain/rng.ts";

const BASE = "http://risk.test";

function harness(seed: number): App {
  const protocol = createStreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
    // Prove that the endpoint's requested `wait` overrides this shorter default.
    longPollTimeoutMs: 20,
  });
  let clock = 0;
  return buildApp({
    protocol,
    stores: createInMemoryStores(),
    rng: createSeededRng(seed),
    now: () => (clock += 1),
  });
}

function httpFor(app: App) {
  return async (method: string, path: string, opts: { token?: string; body?: unknown } = {}) => {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (opts.token) headers.authorization = `Bearer ${opts.token}`;
    const init: RequestInit = { method, headers };
    if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
    const res = await app.fetch(new Request(`${BASE}${path}`, init));
    return { status: res.status, body: await res.json() };
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("turn-stream long-poll wake", () => {
  it("honors the requested wait and wakes when control passes", async () => {
    const app = harness(1234);
    const call = httpFor(app);
    const created = await call("POST", "/v1/games", {
      body: { ruleset: "risk-demo-v1", name: "Ada", color: "red" },
    });
    const gameId: string = created.body.game.id;
    const hostId: string = created.body.player.id;
    const tokenByPlayer: Record<string, string> = { [hostId]: created.body.capability };
    const joined = await call("POST", `/v1/games/${gameId}/players`, {
      body: { name: "Bob", color: "blue" },
    });
    tokenByPlayer[joined.body.player.id] = joined.body.capability;
    await call("POST", `/v1/games/${gameId}/start`, { token: tokenByPlayer[hostId], body: {} });

    const meta = await call("GET", `/v1/games/${gameId}`);
    const active: string = meta.body.activePlayerId;
    const inactive = active === hostId ? joined.body.player.id : hostId;

    // The inactive player has no wake yet; capture its cursor and block on it.
    const initial = await call("GET", `/v1/games/${gameId}/players/me/turns`, {
      token: tokenByPlayer[inactive]!,
    });
    expect(initial.body.notifications).toHaveLength(0);
    const cursor: string = initial.body.cursor;

    const pollStartedAt = performance.now();
    const pollPromise = call(
      "GET",
      `/v1/games/${gameId}/players/me/turns?offset=${cursor}&wait=3000`,
      { token: tokenByPlayer[inactive]! },
    );

    // This exceeds the protocol's 20ms default, discriminating whether the
    // HTTP `wait` value reaches the reusable live-read layer.
    await sleep(75);

    // The active player finishes its turn → control passes → the inactive
    // player's durable wake is produced, resolving the long-poll.
    const bot = createBot({
      call,
      gameId,
      playerId: active,
      token: tokenByPlayer[active]!,
      state: {},
    });
    await bot.awaitTurn();
    await bot.playTurn();

    const polled = await pollPromise;
    expect(performance.now() - pollStartedAt).toBeGreaterThanOrEqual(60);
    expect(polled.body.notifications.length).toBeGreaterThan(0);
    expect(polled.body.notifications[0].playerId).toBe(inactive);
    expect(polled.body.notifications[0].type).toBe("TurnAvailable");
  });
});
