/**
 * Who may mint an agent seat, what a seat-scoped response may leak, and which
 * games have an actions stream at all.
 *
 * These are the boundaries the four-endpoint contract rests on: an agent
 * capability that only plays, a host that can hand over its own seat and no
 * one else's, a private stream that stays behind the bearer, and a v1
 * ruleset that says so instead of answering with silence.
 */

import { describe, expect, it } from "vitest";

import { BASE, call, createV2Game, v2Harness } from "../v2-harness.ts";
import { RULESET_V2 } from "../../src/domain/map-v2.ts";
import { actionStreamId } from "../../server/game/names.ts";

async function createHostedGame(h: ReturnType<typeof v2Harness>) {
  const created = await call(h.app, "POST", "/v1/games", {
    body: { ruleset: RULESET_V2, name: "Host", mapSeed: "authority-seed" },
  });
  expect(created.status).toBe(201);
  return {
    gameId: created.body.game.id as string,
    hostId: created.body.player.id as string,
    hostToken: created.body.capability as string,
  };
}

describe("agent seat authority", () => {
  it("lets the host delegate only its own seat", async () => {
    const h = v2Harness();
    const { gameId, hostId, hostToken } = await createHostedGame(h);

    // A second, ordinary human seat: exactly the seat a host must not be able
    // to convert into an agent seat it holds the capability for.
    const guest = await call(h.app, "POST", `/v1/games/${gameId}/players`, {
      body: { name: "Guest", color: "blue" },
    });
    expect(guest.status).toBe(201);
    const guestId = guest.body.player.id as string;

    const hijack = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { playerId: guestId },
    });
    expect(hijack.status).toBe(403);
    expect(hijack.body.error.code).toBe("FORBIDDEN");

    // The guest's seat is untouched: no capability was minted for it.
    const guestDecision = await call(h.app, "GET", `/v1/games/${gameId}/decision`, {
      token: guest.body.capability,
    });
    expect(guestDecision.status).toBe(200);

    // The host's own seat still converts, and joining a brand-new agent seat
    // still works — only *someone else's* seat is refused.
    const delegated = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { playerId: hostId },
    });
    expect(delegated.status).toBe(201);
    expect(delegated.body.seat.playerId).toBe(hostId);

    const fresh = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { name: "Agent 3", color: "green" },
    });
    expect(fresh.status).toBe(201);
    expect(fresh.body.seat.playerId).not.toBe(hostId);
  });

  it("refuses an agent seat on unauthenticated create and join, in either spelling", async () => {
    const h = v2Harness();
    // `agent` is the public vocabulary; `external-agent` is the canonical event
    // vocabulary. Neither may open a seat without host authority — and the
    // internal spelling must not quietly fall through to a human seat.
    for (const controller of ["agent", "external-agent"]) {
      const created = await call(h.app, "POST", "/v1/games", {
        body: { ruleset: RULESET_V2, name: "Sneaky", controller },
      });
      expect(created.status).toBe(403);
      expect(created.body.error.code).toBe("AGENT_SEAT_REQUIRES_HOST");
    }

    const { gameId } = await createHostedGame(h);
    for (const controller of ["agent", "external-agent"]) {
      const joined = await call(h.app, "POST", `/v1/games/${gameId}/players`, {
        body: { name: "Sneaky", controller },
      });
      expect(joined.status).toBe(403);
      expect(joined.body.error.code).toBe("AGENT_SEAT_REQUIRES_HOST");
    }

    const meta = await call(h.app, "GET", `/v1/games/${gameId}`);
    expect(meta.body.players).toHaveLength(1);
  });
});

describe("seat-scoped exposure", () => {
  it("marks /decision no-store for both rulesets", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const token = game.tokenByPlayer[game.players[0]!]!;
    const response = await h.app.fetch(
      new Request(`${BASE}/v1/games/${game.gameId}/decision`, {
        headers: { authorization: `Bearer ${token}` },
      }),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("referrer-policy")).toBe("no-referrer");

    const v1 = await call(h.app, "POST", "/v1/games", {
      body: { ruleset: "risk-demo-v1", name: "Legacy" },
    });
    const v1Response = await h.app.fetch(
      new Request(`${BASE}/v1/games/${v1.body.game.id}/decision`, {
        headers: { authorization: `Bearer ${v1.body.capability}` },
      }),
    );
    expect(v1Response.status).toBe(200);
    expect(v1Response.headers.get("cache-control")).toBe("no-store");
  });

  it("keeps a player's actions stream out of the public /streams facade", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    const playerId = meta.activePlayerId as string;

    // The stream genuinely exists and carries this player's messages...
    const authorized = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token: game.tokenByPlayer[playerId]!,
    });
    expect(authorized.status).toBe(200);
    expect(authorized.body.messages.length).toBeGreaterThan(0);

    // ...but the spectator facade exposes only the active board projection, so
    // naming the derived stream directly — with or without the seat bearer —
    // gets nothing. A capability is not a key to the raw stream layer.
    const streamPath = `${BASE}/streams/${actionStreamId(game.gameId, playerId)}`;
    const attempts: Array<Record<string, string>> = [
      {},
      { authorization: `Bearer ${game.tokenByPlayer[playerId]!}` },
    ];
    for (const headers of attempts) {
      const leaked = await h.app.fetch(new Request(streamPath, { headers }));
      expect(leaked.status).toBe(404);
    }

    // The canonical event stream stays private for the same reason.
    const canonical = await h.app.fetch(new Request(`${BASE}/streams/games/${game.gameId}/events`));
    expect(canonical.status).toBe(404);
  });
});

describe("risk-demo-v1 has no actions stream", () => {
  it("answers the actions route with a clear unsupported response", async () => {
    const h = v2Harness();
    const created = await call(h.app, "POST", "/v1/games", {
      body: { ruleset: "risk-demo-v1", name: "Legacy" },
    });
    const gameId = created.body.game.id as string;
    await call(h.app, "POST", `/v1/games/${gameId}/players`, { body: { name: "Guest" } });
    await call(h.app, "POST", `/v1/games/${gameId}/start`, {
      token: created.body.capability,
      body: {},
    });

    const actions = await call(h.app, "GET", `/v1/games/${gameId}/players/me/actions`, {
      token: created.body.capability,
    });
    expect(actions.status).toBe(400);
    expect(actions.body.error.code).toBe("BAD_REQUEST");
    // It names the resource that *does* serve a v1 seat, rather than leaving a
    // client to guess that silence means "wait".
    expect(actions.body.error.message).toContain("/decision");

    // The same refusal shape the immutable map uses for a v1 game.
    const map = await call(h.app, "GET", `/v1/games/${gameId}/map`);
    expect(map.status).toBe(400);

    // Agent seats are a v2 concept too.
    const seat = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: created.body.capability,
      body: {},
    });
    expect(seat.status).toBe(400);
  });
});
