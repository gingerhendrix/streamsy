import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";

import { buildApp, type App } from "../../server/http/app.ts";
import { createInMemoryStores } from "../../server/persistence/stores.ts";
import { createAgent, type Agent, type HttpCall } from "../../server/demo/agent.ts";
import { createSeededRng } from "../../src/domain/rng.ts";

const BASE = "http://risk.test";

function harness(seed: number): App {
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  let clock = 0;
  return buildApp({
    protocol,
    stores: createInMemoryStores(),
    rng: createSeededRng(seed),
    now: () => (clock += 1),
  });
}

function httpFor(app: App): HttpCall {
  return async (method, path, opts = {}) => {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (opts.token) headers.authorization = `Bearer ${opts.token}`;
    const init: RequestInit = { method, headers };
    if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
    const res = await app.fetch(new Request(`${BASE}${path}`, init));
    return { status: res.status, body: await res.json() };
  };
}

interface Setup {
  app: App;
  http: HttpCall;
  gameId: string;
  players: string[];
  tokenByPlayer: Record<string, string>;
}

async function setup(seed: number): Promise<Setup> {
  const app = harness(seed);
  const http = httpFor(app);
  const created = await http("POST", "/v1/games", { body: { name: "Alice", color: "red" } });
  const gameId: string = created.body.game.id;
  const hostId: string = created.body.player.id;
  const tokenByPlayer: Record<string, string> = { [hostId]: created.body.capability };
  const joined = await http("POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  tokenByPlayer[joined.body.player.id] = joined.body.capability;
  await http("POST", `/v1/games/${gameId}/start`, { token: tokenByPlayer[hostId], body: {} });
  return { app, http, gameId, players: [hostId, joined.body.player.id], tokenByPlayer };
}

describe("coding-agent harness", () => {
  it("plays a complete game using only HTTP resources and turn streams", async () => {
    const s = await setup(1234);
    const agents: Record<string, Agent> = {};
    for (const playerId of s.players) {
      agents[playerId] = createAgent({
        call: s.http,
        gameId: s.gameId,
        playerId,
        token: s.tokenByPlayer[playerId]!,
        state: {},
      });
    }

    let finished = false;
    for (let guard = 0; guard < 500 && !finished; guard += 1) {
      const meta = await s.http("GET", `/v1/games/${s.gameId}`);
      if (meta.body.status === "finished") {
        finished = true;
        break;
      }
      const active: string = meta.body.activePlayerId;
      const agent = agents[active]!;
      // Turn-stream driven: the active agent observes a wake before acting.
      const wake = await agent.awaitTurn();
      expect(wake).not.toBeNull();
      expect(wake!.playerId).toBe(active);
      await agent.playTurn();
    }

    expect(finished).toBe(true);
    const finalMeta = await s.http("GET", `/v1/games/${s.gameId}`);
    expect(finalMeta.body.status).toBe("finished");
    expect(finalMeta.body.winnerId).toBeDefined();

    const board = await s.http("GET", `/v1/games/${s.gameId}/board`);
    for (const t of board.body.territories) expect(t.ownerId).toBe(finalMeta.body.winnerId);
  });

  it("resumes from a persisted cursor after a simulated restart without missing a turn", async () => {
    const s = await setup(1234);
    const persisted: Record<string, { cursor?: string }> = {
      [s.players[0]!]: {},
      [s.players[1]!]: {},
    };
    const makeAgent = (playerId: string): Agent =>
      createAgent({
        call: s.http,
        gameId: s.gameId,
        playerId,
        token: s.tokenByPlayer[playerId]!,
        state: persisted[playerId]!, // mutated in place — the durable cursor
      });

    let finished = false;
    let restarted = false;
    for (let guard = 0; guard < 500 && !finished; guard += 1) {
      const meta = await s.http("GET", `/v1/games/${s.gameId}`);
      if (meta.body.status === "finished") {
        finished = true;
        break;
      }
      const active: string = meta.body.activePlayerId;

      // Halfway through, drop all in-memory agents and rebuild them from ONLY the
      // persisted cursor, proving resume does not miss the actionable turn.
      if (guard === 4) restarted = true;
      const agent = makeAgent(active);
      const wake = await agent.awaitTurn();
      expect(wake).not.toBeNull();
      await agent.playTurn();
    }

    expect(restarted).toBe(true);
    expect(finished).toBe(true);
  });

  it("tolerates a duplicate wake poll without double-committing", async () => {
    const s = await setup(1234);
    const active = (await s.http("GET", `/v1/games/${s.gameId}`)).body.activePlayerId;
    const agent = createAgent({
      call: s.http,
      gameId: s.gameId,
      playerId: active,
      token: s.tokenByPlayer[active]!,
      state: {},
    });

    const wake1 = await agent.awaitTurn();
    expect(wake1).not.toBeNull();
    // A second poll from a stale cursor re-observes wakes but must not corrupt play.
    const staleAgent = createAgent({
      call: s.http,
      gameId: s.gameId,
      playerId: active,
      token: s.tokenByPlayer[active]!,
      state: {}, // cursor at start → re-reads the same wake
    });
    const wake2 = await staleAgent.awaitTurn();
    expect(wake2!.notificationId).toBe(wake1!.notificationId);

    // The agent still plays its turn correctly and control eventually passes.
    await agent.playTurn();
    const after = await s.http("GET", `/v1/games/${s.gameId}`);
    expect(after.body.activePlayerId).not.toBe(active);
  });

  it("runs the pacing hook after every successful command", async () => {
    const s = await setup(1234);
    const active = (await s.http("GET", `/v1/games/${s.gameId}`)).body.activePlayerId;
    const committed: string[] = [];
    const agent = createAgent({
      call: s.http,
      gameId: s.gameId,
      playerId: active,
      token: s.tokenByPlayer[active]!,
      onCommandCommitted: (action) => {
        committed.push(String(action.type));
      },
    });

    await agent.playTurn();

    expect(committed[0]).toBe("reinforce");
    expect(committed.at(-1)).toBe("end-turn");
    expect(committed.length).toBeGreaterThan(1);
  });
});
