/**
 * The lobby roster over HTTP: who may rename which seat, who may give one up, and
 * what the projected board says afterwards.
 *
 * The authorization questions are the point. A rename endpoint that took any
 * player id would let a host relabel another person's seat, and a leave endpoint
 * that took one would let it remove them outright — the same class of hole the
 * agent-seat delegation rule closes, so it is closed the same way: the leave is
 * scoped to `me`, and the rename admits exactly two callers.
 */

import { describe, expect, it } from "vitest";

import { call, riskHarness } from "../harness.ts";
import { RULES } from "../../src/domain/map.ts";

async function lobby(h: ReturnType<typeof riskHarness>) {
  // Created exactly as the landing page creates one: with no name at all.
  const created = await call(h.app, "POST", "/v1/games", { body: {} });
  expect(created.status).toBe(201);
  const gameId = created.body.game.id as string;
  const guest = await call(h.app, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "Mina" },
  });
  expect(guest.status).toBe(201);
  return {
    gameId,
    hostId: created.body.player.id as string,
    hostName: created.body.player.name as string,
    hostToken: created.body.capability as string,
    guestId: guest.body.player.id as string,
    guestToken: guest.body.capability as string,
  };
}

const boardPlayers = async (h: ReturnType<typeof riskHarness>, gameId: string) =>
  (await call(h.app, "GET", `/v1/games/${gameId}/board`)).body.players as Array<{
    id: string;
    name: string;
  }>;

describe("creating a game without a name", () => {
  it("issues a provisional host name the creator can then set", async () => {
    const h = riskHarness();
    const { gameId, hostId, hostName, hostToken } = await lobby(h);
    expect(hostName).toBeTruthy();

    const renamed = await call(h.app, "PATCH", `/v1/games/${gameId}/players/${hostId}`, {
      token: hostToken,
      body: { name: "  Wellington  " },
    });
    expect(renamed.status).toBe(200);
    // The recorded name, not the requested one.
    expect(renamed.body.player.name).toBe("Wellington");
    expect((await boardPlayers(h, gameId)).find((p) => p.id === hostId)?.name).toBe("Wellington");
  });

  it("bounds a name at the canonical limit and refuses a blank one", async () => {
    const h = riskHarness();
    const { gameId, guestId, guestToken } = await lobby(h);

    const long = await call(h.app, "PATCH", `/v1/games/${gameId}/players/${guestId}`, {
      token: guestToken,
      body: { name: "W".repeat(40) },
    });
    expect(long.status).toBe(200);
    expect(long.body.player.name).toHaveLength(RULES.maxPlayerNameLength);

    const blank = await call(h.app, "PATCH", `/v1/games/${gameId}/players/${guestId}`, {
      token: guestToken,
      body: { name: "   " },
    });
    expect(blank.status).toBe(400);
    expect(blank.body.error.code).toBe("INVALID_NAME");
  });
});

describe("rename authority", () => {
  it("lets the host name an agent seat but never another person's seat", async () => {
    const h = riskHarness();
    const { gameId, guestId, hostToken } = await lobby(h);
    const seat = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { name: "Agent 3" },
    });
    expect(seat.status).toBe(201);
    const agentId = seat.body.seat.playerId as string;

    const namedAgent = await call(h.app, "PATCH", `/v1/games/${gameId}/players/${agentId}`, {
      token: hostToken,
      body: { name: "Blücher" },
    });
    expect(namedAgent.status).toBe(200);
    expect((await boardPlayers(h, gameId)).find((p) => p.id === agentId)?.name).toBe("Blücher");

    const hijack = await call(h.app, "PATCH", `/v1/games/${gameId}/players/${guestId}`, {
      token: hostToken,
      body: { name: "Renamed by the host" },
    });
    expect(hijack.status).toBe(403);
    expect(hijack.body.error.code).toBe("FORBIDDEN");
    expect((await boardPlayers(h, gameId)).find((p) => p.id === guestId)?.name).toBe("Mina");
  });

  it("refuses an unauthenticated rename and an agent capability", async () => {
    const h = riskHarness();
    const { gameId, guestId, hostId, hostToken } = await lobby(h);
    const seat = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { name: "Agent 3" },
    });
    const agentToken = seat.body.seat.token as string;

    const anonymous = await call(h.app, "PATCH", `/v1/games/${gameId}/players/${guestId}`, {
      body: { name: "Nobody" },
    });
    expect(anonymous.status).toBe(401);

    // The agent surface stays exactly four endpoints; a seat's own name is not
    // among the things its capability may change.
    const byAgent = await call(h.app, "PATCH", `/v1/games/${gameId}/players/${hostId}`, {
      token: agentToken,
      body: { name: "Renamed by an agent" },
    });
    expect(byAgent.status).toBe(403);
    expect(byAgent.body.error.code).toBe("FORBIDDEN");
  });
});

describe("leaving a lobby", () => {
  it("removes the seat from the projected roster", async () => {
    const h = riskHarness();
    const { gameId, guestId, guestToken } = await lobby(h);

    const left = await call(h.app, "DELETE", `/v1/games/${gameId}/players/me`, {
      token: guestToken,
    });
    expect(left.status).toBe(200);
    expect(left.body.playerId).toBe(guestId);
    expect((await boardPlayers(h, gameId)).map((p) => p.id)).not.toContain(guestId);
    // The seat is gone canonically, so its own capability has nothing to decide.
    const orphaned = await call(h.app, "GET", `/v1/games/${gameId}/decision`, {
      token: guestToken,
    });
    expect(orphaned.status).toBe(404);
  });

  it("keeps the creator's host authority after it gives up its seat", async () => {
    const h = riskHarness();
    const { gameId, hostId, hostToken } = await lobby(h);
    const seat = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { name: "Agent 3" },
    });
    expect(seat.status).toBe(201);

    const left = await call(h.app, "DELETE", `/v1/games/${gameId}/players/me`, {
      token: hostToken,
    });
    expect(left.status).toBe(200);
    expect((await boardPlayers(h, gameId)).map((p) => p.id)).not.toContain(hostId);

    // Still the host: it can open another seat and start the game it created,
    // which is exactly the agent-versus-agent spectator flow.
    const another = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { name: "Agent 4" },
    });
    expect(another.status).toBe(201);
    const started = await call(h.app, "POST", `/v1/games/${gameId}/start`, {
      token: hostToken,
      body: {},
    });
    expect(started.status).toBe(200);
    expect((await call(h.app, "GET", `/v1/games/${gameId}`)).body.status).toBe("playing");
  });

  it("refuses a seat an agent plays, and any leave once the game is under way", async () => {
    const h = riskHarness();
    const { gameId, hostToken, guestToken } = await lobby(h);
    const seat = await call(h.app, "POST", `/v1/games/${gameId}/agent-seats`, {
      token: hostToken,
      body: { name: "Agent 3" },
    });
    const agentToken = seat.body.seat.token as string;

    const byAgent = await call(h.app, "DELETE", `/v1/games/${gameId}/players/me`, {
      token: agentToken,
    });
    expect(byAgent.status).toBe(403);

    const started = await call(h.app, "POST", `/v1/games/${gameId}/start`, {
      token: hostToken,
      body: {},
    });
    expect(started.status).toBe(200);
    const late = await call(h.app, "DELETE", `/v1/games/${gameId}/players/me`, {
      token: guestToken,
    });
    expect(late.status).toBe(409);
    expect(late.body.error.code).toBe("GAME_ALREADY_STARTED");
  });

  it("refuses an unauthenticated leave and one scoped to another game", async () => {
    const h = riskHarness();
    const first = await lobby(h);
    const second = await lobby(h);

    const anonymous = await call(h.app, "DELETE", `/v1/games/${first.gameId}/players/me`);
    expect(anonymous.status).toBe(401);

    const crossed = await call(h.app, "DELETE", `/v1/games/${first.gameId}/players/me`, {
      token: second.guestToken,
    });
    expect(crossed.status).toBe(403);
    expect(crossed.body.error.code).toBe("WRONG_GAME");
    expect((await boardPlayers(h, first.gameId)).map((p) => p.id)).toContain(first.guestId);
  });
});
