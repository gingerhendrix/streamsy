import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";

import { buildApp, type App } from "../../server/http/app.ts";
import { createInMemoryStores, type Stores } from "../../server/persistence/stores.ts";
import { createBoardProjectionAdapter } from "../../src/board/board-projection.ts";
import { rebuildBoardGeneration } from "../../server/game/rebuild.ts";
import { createSeededRng } from "../../src/domain/rng.ts";

const BASE = "http://risk.test";

interface Harness {
  app: App;
  protocol: StreamProtocolFactory;
  stores: Stores;
  gameId: string;
  http: (
    method: string,
    path: string,
    opts?: { token?: string; body?: unknown },
  ) => Promise<{ status: number; body: any }>;
}

/** Create + join + start and play one reinforce so the board is non-trivial. */
async function playedGame(seed: number): Promise<Harness> {
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  const stores = createInMemoryStores();
  let clock = 0;
  const app = buildApp({ protocol, stores, rng: createSeededRng(seed), now: () => (clock += 1) });
  const http = async (
    method: string,
    path: string,
    opts: { token?: string; body?: unknown } = {},
  ) => {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (opts.token) headers.authorization = `Bearer ${opts.token}`;
    const init: RequestInit = { method, headers };
    if (opts.body !== undefined) init.body = JSON.stringify(opts.body);
    const res = await app.fetch(new Request(`${BASE}${path}`, init));
    return { status: res.status, body: await res.json() };
  };

  const created = await http("POST", "/v1/games", {
    body: { ruleset: "risk-demo-v1", name: "Alice", color: "red" },
  });
  const gameId: string = created.body.game.id;
  const hostToken: string = created.body.capability;
  const hostId: string = created.body.player.id;
  const joined = await http("POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  const tokenByPlayer: Record<string, string> = {
    [hostId]: hostToken,
    [joined.body.player.id]: joined.body.capability,
  };
  await http("POST", `/v1/games/${gameId}/start`, { token: hostToken, body: {} });

  const meta = await http("GET", `/v1/games/${gameId}`);
  const active: string = meta.body.activePlayerId;
  const decision = await http("GET", `/v1/games/${gameId}/decision`, {
    token: tokenByPlayer[active]!,
  });
  const reinforce = decision.body.legalMoves.find((a: any) => a.type === "reinforce");
  await http("POST", `/v1/games/${gameId}/commands`, {
    token: tokenByPlayer[active]!,
    body: {
      commandId: "rein-1",
      turnId: decision.body.turn.id,
      action: {
        type: "reinforce",
        territoryId: reinforce.territoryIds[0],
        armies: reinforce.pool,
      },
    },
  });

  return { app, protocol, stores, gameId, http };
}

describe("board generation rebuild + durable cutover", () => {
  it("rebuilds an equivalent generation, cuts over, and retains the old one", async () => {
    const h = await playedGame(4321);
    const before = await h.http("GET", `/v1/games/${h.gameId}/board`);
    expect(before.body.generation).toBe("v1");

    const result = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores },
      h.gameId,
      { now: () => 100 },
    );

    expect(result.status).toBe("cutover");
    expect(result.fromGeneration).toBe("v1");
    expect(result.toGeneration).toBe("v2");
    expect(result.equivalence).toEqual({ boardEqual: true, watermarkEqual: true });
    expect(result.sourceThroughOffset).toBe(before.body.sourceThroughOffset);
    expect(result.activeGeneration).toBe("v2");
    // Old generation retained, not deleted.
    expect(result.retainedGenerations).toEqual(["v1", "v2"]);

    // Reads now use the new active generation but the same logical board + watermark.
    const after = await h.http("GET", `/v1/games/${h.gameId}/board`);
    expect(after.body.generation).toBe("v2");
    expect(after.body.sourceThroughOffset).toBe(before.body.sourceThroughOffset);
    expect(after.body.territories).toEqual(before.body.territories);

    // The store reflects the atomic pointer move.
    const gens = h.stores.generations.list(h.gameId);
    expect(gens.find((g) => g.generation === "v1")!.status).toBe("retired");
    expect(gens.find((g) => g.generation === "v2")!.status).toBe("active");
    expect(h.stores.games.get(h.gameId)!.generation).toBe("v2");
  });

  it("leaves the active generation unchanged when verification fails (rollback)", async () => {
    const h = await playedGame(4321);
    const before = await h.http("GET", `/v1/games/${h.gameId}/board`);

    // A corrupt reducer that ignores events → the rebuilt board diverges.
    const result = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores },
      h.gameId,
      {
        now: () => 100,
        makeAdapter: (o) => ({ ...createBoardProjectionAdapter(o), reduce: (state) => state }),
      },
    );

    expect(result.status).toBe("verification-failed");
    expect(result.equivalence.boardEqual).toBe(false);
    expect(result.activeGeneration).toBe("v1");

    // Old generation still active and usable; the failed one is retained as failed.
    const after = await h.http("GET", `/v1/games/${h.gameId}/board`);
    expect(after.body.generation).toBe("v1");
    expect(after.body.territories).toEqual(before.body.territories);
    expect(h.stores.games.get(h.gameId)!.generation).toBe("v1");
    const failed = h.stores.generations.get(h.gameId, "v2");
    expect(failed!.status).toBe("failed");
  });

  it("can rebuild again after a cutover, chaining generations", async () => {
    const h = await playedGame(4321);
    await rebuildBoardGeneration({ protocol: h.protocol, stores: h.stores }, h.gameId, {
      now: () => 100,
    });
    const second = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores },
      h.gameId,
      { now: () => 200 },
    );
    expect(second.status).toBe("cutover");
    expect(second.fromGeneration).toBe("v2");
    expect(second.toGeneration).toBe("v3");
    expect(second.retainedGenerations).toEqual(["v1", "v2", "v3"]);
    expect((await h.http("GET", `/v1/games/${h.gameId}/board`)).body.generation).toBe("v3");
  });
});
