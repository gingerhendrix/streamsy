import { describe, expect, it } from "vitest";

import worker, { type RiskWorkerEnv } from "./worker.ts";

function harness() {
  const routed: Array<{ id: unknown; gameId: string | null; path: string }> = [];
  const env: RiskWorkerEnv = {
    GAME: {
      idFromName: (name) => `do:${name}`,
      get: (id) => ({
        async fetch(request) {
          const gameId = request.headers.get("x-risk-game-id");
          routed.push({ id, gameId, path: new URL(request.url).pathname });
          return Response.json({ gameId });
        },
      }),
    },
    ASSETS: {
      fetch: async () => new Response("asset"),
    },
  };
  return { env, routed };
}

describe("Cloudflare game routing", () => {
  it("mints distinct Durable Object identities for distinct games", async () => {
    const h = harness();
    const first = await worker.fetch(
      new Request("https://risk.test/v1/games", { method: "POST", body: "{}" }),
      h.env,
    );
    const second = await worker.fetch(
      new Request("https://risk.test/v1/games", { method: "POST", body: "{}" }),
      h.env,
    );
    const firstId = ((await first.json()) as { gameId: string }).gameId;
    const secondId = ((await second.json()) as { gameId: string }).gameId;

    expect(firstId).toMatch(/^game_[0-9a-f]{24}$/);
    expect(secondId).toMatch(/^game_[0-9a-f]{24}$/);
    expect(secondId).not.toBe(firstId);
    expect(h.routed.map((route) => route.id)).toEqual([`do:${firstId}`, `do:${secondId}`]);
  });

  it("routes every game-scoped surface to the same named object", async () => {
    const h = harness();
    const gameId = "game_00112233445566778899aabb";
    const paths = [
      `/v1/games/${gameId}/board`,
      `/v1/games/${gameId}/agent/token/state`,
      `/streams/games/${gameId}/projections/board/hex1`,
      `/agent-seat/${gameId}/player`,
    ];
    for (const path of paths) {
      await worker.fetch(new Request(`https://risk.test${path}`), h.env);
    }

    expect(h.routed).toHaveLength(paths.length);
    expect(new Set(h.routed.map((route) => route.id))).toEqual(new Set([`do:${gameId}`]));
    expect(new Set(h.routed.map((route) => route.gameId))).toEqual(new Set([gameId]));
  });

  it("does not route a token-only agent URL through a global registry", async () => {
    const h = harness();
    const response = await worker.fetch(new Request("https://risk.test/agent/secret/state"), h.env);

    expect(response.status).toBe(404);
    expect(await response.json()).toMatchObject({
      error: { code: "GAME_SCOPED_ROUTE_REQUIRED" },
    });
    expect(h.routed).toHaveLength(0);
  });
});
