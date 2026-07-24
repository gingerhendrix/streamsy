import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";

import { buildApp, type App } from "../../server/http/app.ts";
import { createInMemoryStores } from "../../server/persistence/stores.ts";
import { createSeededRng } from "../../src/domain/rng.ts";

const BASE = "http://risk.test";

function harness(seed = 7): App {
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  let clock = 0;
  return buildApp({
    protocol,
    stores: createInMemoryStores(),
    rng: createSeededRng(seed),
    now: () => (clock += 1),
  });
}

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

interface Game {
  gameId: string;
  tokenByPlayer: Record<string, string>;
  players: string[];
}

async function createJoinStart(app: App): Promise<Game> {
  const created = await call(app, "POST", "/v1/games", {
    body: { ruleset: "risk-demo-v1", name: "Alice", color: "red" },
  });
  const gameId: string = created.body.game.id;
  const hostId: string = created.body.player.id;
  const tokenByPlayer: Record<string, string> = { [hostId]: created.body.capability };
  const joined = await call(app, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  tokenByPlayer[joined.body.player.id] = joined.body.capability;
  await call(app, "POST", `/v1/games/${gameId}/start`, { token: tokenByPlayer[hostId], body: {} });
  return { gameId, tokenByPlayer, players: [hostId, joined.body.player.id] };
}

async function activeId(app: App, gameId: string): Promise<string> {
  const meta = await call(app, "GET", `/v1/games/${gameId}`);
  return meta.body.activePlayerId;
}

/** Place all reinforcements then end the active player's turn. */
async function endActiveTurn(app: App, game: Game): Promise<void> {
  for (let guard = 0; guard < 20; guard += 1) {
    const active = await activeId(app, game.gameId);
    const token = game.tokenByPlayer[active]!;
    const d = (await call(app, "GET", `/v1/games/${game.gameId}/decision`, { token })).body;
    if (d.turn.phase === "reinforce") {
      const reinforce = d.legalActions.find((a: any) => a.type === "reinforce");
      const owned = d.board.territories.filter((t: any) => t.ownerId === active);
      await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
        token,
        body: {
          commandId: `end-r-${guard}`,
          turnId: d.turn.id,
          action: { type: "reinforce", territoryId: owned[0].id, armies: reinforce.maxArmies },
        },
      });
      continue;
    }
    await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: { commandId: `end-t-${guard}`, turnId: d.turn.id, action: { type: "end-turn" } },
    });
    return;
  }
  throw new Error("could not end turn");
}

describe("turn notification streams", () => {
  it("wakes exactly the active player when the game starts", async () => {
    const app = harness();
    const game = await createJoinStart(app);
    const active = await activeId(app, game.gameId);
    const inactive = game.players.find((p) => p !== active)!;

    const wake = await call(app, "GET", `/v1/games/${game.gameId}/players/me/turns`, {
      token: game.tokenByPlayer[active]!,
    });
    expect(wake.status).toBe(200);
    expect(wake.body.notifications).toHaveLength(1);
    expect(wake.body.notifications[0].type).toBe("TurnAvailable");
    expect(wake.body.notifications[0].playerId).toBe(active);
    expect(wake.body.notifications[0].turnId).toBe(`round-1:${active}`);
    expect(typeof wake.body.notifications[0].causedBySourceOffset).toBe("string");

    // The inactive player has no wake yet.
    const idle = await call(app, "GET", `/v1/games/${game.gameId}/players/me/turns`, {
      token: game.tokenByPlayer[inactive]!,
    });
    expect(idle.body.notifications).toHaveLength(0);
  });

  it("resumes from a saved cursor and does not re-deliver, and is replay-idempotent", async () => {
    const app = harness();
    const game = await createJoinStart(app);
    const active = await activeId(app, game.gameId);
    const token = game.tokenByPlayer[active]!;

    const first = await call(app, "GET", `/v1/games/${game.gameId}/players/me/turns`, { token });
    expect(first.body.notifications).toHaveLength(1);
    const cursor = first.body.cursor;

    // Reading again from the saved cursor yields nothing new.
    const resumed = await call(
      app,
      "GET",
      `/v1/games/${game.gameId}/players/me/turns?offset=${cursor}`,
      { token },
    );
    expect(resumed.body.notifications).toHaveLength(0);

    // Re-reading from the start (a rebuild/replay) still yields exactly one wake,
    // proving the notifier did not append a duplicate.
    const replay = await call(app, "GET", `/v1/games/${game.gameId}/players/me/turns`, { token });
    expect(replay.body.notifications).toHaveLength(1);
    expect(replay.body.notifications[0].notificationId).toBe(
      first.body.notifications[0].notificationId,
    );
  });

  it("wakes the next player when control passes", async () => {
    const app = harness();
    const game = await createJoinStart(app);
    const first = await activeId(app, game.gameId);
    await endActiveTurn(app, game);
    const next = await activeId(app, game.gameId);
    expect(next).not.toBe(first);

    const wake = await call(app, "GET", `/v1/games/${game.gameId}/players/me/turns`, {
      token: game.tokenByPlayer[next]!,
    });
    expect(wake.body.notifications).toHaveLength(1);
    expect(wake.body.notifications[0].playerId).toBe(next);
  });

  it("makes a stale/delayed wake safe: acting on an old turnId is rejected", async () => {
    const app = harness();
    const game = await createJoinStart(app);
    const first = await activeId(app, game.gameId);
    const wake = await call(app, "GET", `/v1/games/${game.gameId}/players/me/turns`, {
      token: game.tokenByPlayer[first]!,
    });
    const staleTurnId = wake.body.notifications[0].turnId;

    await endActiveTurn(app, game); // control passes to the other player

    // The original player replays the stale wake and tries to act on it.
    const res = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token: game.tokenByPlayer[first]!,
      body: {
        commandId: "stale-wake",
        turnId: staleTurnId,
        action: { type: "reinforce", territoryId: "alpha", armies: 1 },
      },
    });
    expect(res.status).toBe(409);
    expect(["NOT_YOUR_TURN", "STALE_TURN"]).toContain(res.body.error.code);
  });
});
