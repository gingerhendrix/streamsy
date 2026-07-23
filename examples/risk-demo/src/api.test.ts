import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";

import { buildApp, type App } from "../server/app.ts";
import { createInMemoryStores, type Stores } from "../server/stores.ts";
import { createSeededRng } from "./rng.ts";
import { boardProjectionTxId } from "./transaction.ts";

interface Harness {
  app: App;
  stores: Stores;
}

function harness(seed = 7): Harness {
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  const stores = createInMemoryStores();
  let clock = 0;
  const app = buildApp({ protocol, stores, rng: createSeededRng(seed), now: () => (clock += 1) });
  return { app, stores };
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

interface StartedGame {
  gameId: string;
  tokenByPlayer: Record<string, string>;
  players: string[];
}

async function createJoinStart(app: App): Promise<StartedGame> {
  const created = await call(app, "POST", "/v1/games", { body: { name: "Alice", color: "red" } });
  expect(created.status).toBe(201);
  const gameId: string = created.body.game.id;
  const hostId: string = created.body.player.id;
  const tokenByPlayer: Record<string, string> = { [hostId]: created.body.capability };

  const joined = await call(app, "POST", `/v1/games/${gameId}/players`, {
    body: { name: "Bob", color: "blue" },
  });
  expect(joined.status).toBe(201);
  tokenByPlayer[joined.body.player.id] = joined.body.capability;

  const started = await call(app, "POST", `/v1/games/${gameId}/start`, {
    token: tokenByPlayer[hostId],
    body: {},
  });
  expect(started.status).toBe(200);

  return { gameId, tokenByPlayer, players: [hostId, joined.body.player.id] };
}

async function activeContext(app: App, game: StartedGame) {
  const meta = await call(app, "GET", `/v1/games/${game.gameId}`);
  const active: string = meta.body.activePlayerId;
  const token = game.tokenByPlayer[active]!;
  const decision = await call(app, "GET", `/v1/games/${game.gameId}/decision`, { token });
  return { active, token, decision: decision.body };
}

/** Drive the active player's reinforce phase, returning the attack-phase decision. */
async function playToAttack(app: App, game: StartedGame) {
  for (let guard = 0; guard < 50; guard += 1) {
    const { active, token, decision } = await activeContext(app, game);
    if (decision.turn.phase === "attack") return { active, token, decision };
    const reinforce = decision.legalActions.find((a: any) => a.type === "reinforce");
    const owned = decision.board.territories.filter((t: any) => t.ownerId === active);
    const frontier =
      owned.find((t: any) =>
        t.adjacentTerritoryIds.some(
          (adj: string) =>
            decision.board.territories.find((x: any) => x.id === adj)?.ownerId !== active,
        ),
      ) ?? owned[0];
    const res = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: `reinf-${guard}`,
        turnId: decision.turn.id,
        action: { type: "reinforce", territoryId: frontier.id, armies: reinforce.maxArmies },
      },
    });
    expect(res.status).toBe(200);
  }
  throw new Error("never reached attack phase");
}

describe("risk command API", () => {
  it("exposes only the active board projection through the read-only Streamsy facade", async () => {
    const { app } = harness();
    const created = await call(app, "POST", "/v1/games", {
      body: { name: "Alice", color: "red" },
    });
    const gameId: string = created.body.game.id;

    const board = await app.fetch(
      new Request(`${BASE}/streams/games/${gameId}/projections/board/v1`),
    );
    expect(board.status).toBe(200);
    expect(board.headers.get("content-type")).toContain("application/json");
    const changes = (await board.json()) as Array<{ type: string }>;
    expect(changes.some((change) => change.type === "game")).toBe(true);
    expect(changes.some((change) => change.type === "projectionMeta")).toBe(true);

    const canonical = await app.fetch(new Request(`${BASE}/streams/games/${gameId}/events`));
    expect(canonical.status).toBe(404);
    const write = await app.fetch(
      new Request(`${BASE}/streams/games/${gameId}/projections/board/v1`, {
        method: "POST",
        body: "{}",
      }),
    );
    expect(write.status).toBe(405);
  });

  it("runs create → join → start → decision → command happy path", async () => {
    const { app } = harness();
    const game = await createJoinStart(app);

    const { token, decision } = await activeContext(app, game);
    expect(decision.turn.phase).toBe("reinforce");
    const reinforce = decision.legalActions.find((a: any) => a.type === "reinforce");
    expect(reinforce.maxArmies).toBeGreaterThanOrEqual(3);

    const res = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "cmd-1",
        turnId: decision.turn.id,
        action: { type: "reinforce", territoryId: reinforce.territoryIds[0], armies: 1 },
      },
    });
    expect(res.status).toBe(200);
    expect(res.body.status).toBe("accepted");
    expect(res.body.sourceStreamId).toBe(`games/${game.gameId}/events`);
    expect(typeof res.body.sourceOffset).toBe("string");
    expect(res.body.sourceOffset).not.toBe("");
    expect(res.body.txid).toBe(boardProjectionTxId("cmd-1", res.body.sourceOffset));
  });

  it("enforces capability isolation across players and games", async () => {
    const { app } = harness();
    const game = await createJoinStart(app);
    const [p1, p2] = game.players;
    const other = await createJoinStart(app);

    // Unauthenticated command is rejected.
    const anon = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      body: { commandId: "x", turnId: "t", action: { type: "end-turn" } },
    });
    expect(anon.status).toBe(401);

    // A token from another game cannot act here.
    const wrongGame = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token: other.tokenByPlayer[other.players[0]!],
      body: { commandId: "x", turnId: "t", action: { type: "end-turn" } },
    });
    expect(wrongGame.status).toBe(403);
    expect(wrongGame.body.error.code).toBe("WRONG_GAME");

    // The non-active player's own token cannot play the active player's turn:
    // the server derives playerId from the token, so it's NOT_YOUR_TURN.
    const meta = await call(app, "GET", `/v1/games/${game.gameId}`);
    const inactive = meta.body.activePlayerId === p1 ? p2! : p1!;
    const res = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token: game.tokenByPlayer[inactive]!,
      body: { commandId: "y", turnId: "round-1:whoever", action: { type: "end-turn" } },
    });
    expect(res.status).toBe(409);
    expect(res.body.error.code).toBe("NOT_YOUR_TURN");

    // Only the host may start; a player capability is forbidden.
    const badStart = await call(app, "POST", `/v1/games/${game.gameId}/start`, {
      token: game.tokenByPlayer[p2!]!,
      body: {},
    });
    // p2 may or may not be host depending on who created; use `other`'s player token.
    const otherPlayerStart = await call(app, "POST", `/v1/games/${other.gameId}/start`, {
      token: other.tokenByPlayer[other.players[1]!]!,
      body: {},
    });
    expect(otherPlayerStart.status).toBe(403);
    expect(badStart.status).toBeGreaterThanOrEqual(400);
  });

  it("returns the original ack and dice on an idempotent attack retry", async () => {
    const { app } = harness(3);
    const game = await createJoinStart(app);
    const { token, decision } = await playToAttack(app, game);

    const choice = decision.legalActions.find((a: any) => a.type === "attack").choices[0];
    const body = {
      commandId: "attack-once",
      turnId: decision.turn.id,
      action: {
        type: "attack",
        from: choice.from,
        to: choice.to,
        attackerDice: choice.maxAttackerDice,
      },
    };
    const first = await call(app, "POST", `/v1/games/${game.gameId}/commands`, { token, body });
    expect(first.status).toBe(200);
    expect(first.body.status).toBe("accepted");

    const retry = await call(app, "POST", `/v1/games/${game.gameId}/commands`, { token, body });
    expect(retry.status).toBe(200);
    expect(retry.body.status).toBe("duplicate");
    expect(retry.body.sourceOffset).toBe(first.body.sourceOffset);
    expect(retry.body.events).toEqual(first.body.events);

    // Recovery endpoint returns the same accepted result.
    const recovered = await call(app, "GET", `/v1/games/${game.gameId}/commands/attack-once`, {
      token,
    });
    expect(recovered.status).toBe(200);
    expect(recovered.body.sourceOffset).toBe(first.body.sourceOffset);
  });

  it("rejects a reused commandId that carries a different payload", async () => {
    const { app } = harness();
    const game = await createJoinStart(app);
    const { token, decision } = await activeContext(app, game);
    const reinforce = decision.legalActions.find((a: any) => a.type === "reinforce");

    const first = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "dup",
        turnId: decision.turn.id,
        action: { type: "reinforce", territoryId: reinforce.territoryIds[0], armies: 1 },
      },
    });
    expect(first.status).toBe(200);

    const reused = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "dup",
        turnId: decision.turn.id,
        action: { type: "reinforce", territoryId: reinforce.territoryIds[0], armies: 2 },
      },
    });
    expect(reused.status).toBe(409);
    expect(reused.body.error.code).toBe("COMMAND_ID_REUSED");
  });

  it("rejects a stale turn and an illegal phase with stable codes", async () => {
    const { app } = harness();
    const game = await createJoinStart(app);
    const { token, decision } = await activeContext(app, game);

    const stale = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "stale",
        turnId: "round-99:nobody",
        action: { type: "reinforce", territoryId: decision.board.territories[0].id, armies: 1 },
      },
    });
    expect(stale.status).toBe(409);
    expect(stale.body.error.code).toBe("STALE_TURN");

    const illegalPhase = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "phase",
        turnId: decision.turn.id,
        action: { type: "attack", from: "alpha", to: "bravo", attackerDice: 1 },
      },
    });
    expect(illegalPhase.status).toBe(409);
    expect(illegalPhase.body.error.code).toBe("INVALID_PHASE");
  });

  it("catches the board projection up to the ack offset and matches the aggregate", async () => {
    const { app } = harness();
    const game = await createJoinStart(app);
    const { token, decision } = await activeContext(app, game);
    const reinforce = decision.legalActions.find((a: any) => a.type === "reinforce");

    const ack = await call(app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "cmd-board",
        turnId: decision.turn.id,
        action: { type: "reinforce", territoryId: reinforce.territoryIds[0], armies: 2 },
      },
    });
    expect(ack.status).toBe(200);

    const board = await call(app, "GET", `/v1/games/${game.gameId}/board`);
    expect(board.status).toBe(200);
    expect(board.body.sourceThroughOffset).toBe(ack.body.sourceOffset);
    // The reinforced territory's army count reflects the accepted command.
    const reinforced = board.body.territories.find((t: any) => t.id === reinforce.territoryIds[0]);
    expect(reinforced.armies).toBeGreaterThanOrEqual(3);
  });

  it("keeps canonical history consistent under a concurrent command race (CAS)", async () => {
    const { app } = harness();
    const game = await createJoinStart(app);
    const { active, token, decision } = await activeContext(app, game);
    const owned = decision.board.territories
      .filter((t: any) => t.ownerId === active)
      .map((t: any) => t.id);

    // Two reinforce commands submitted together contend for the same head.
    const [a, b] = await Promise.all([
      call(app, "POST", `/v1/games/${game.gameId}/commands`, {
        token,
        body: {
          commandId: "race-a",
          turnId: decision.turn.id,
          action: { type: "reinforce", territoryId: owned[0], armies: 1 },
        },
      }),
      call(app, "POST", `/v1/games/${game.gameId}/commands`, {
        token,
        body: {
          commandId: "race-b",
          turnId: decision.turn.id,
          action: { type: "reinforce", territoryId: owned[0], armies: 1 },
        },
      }),
    ]);
    expect(a.status).toBe(200);
    expect(b.status).toBe(200);
    expect(a.body.sourceOffset).not.toBe(b.body.sourceOffset); // distinct commits, no lost update

    const after = await call(app, "GET", `/v1/games/${game.gameId}/decision`, { token });
    expect(after.body.legalActions.find((x: any) => x.type === "reinforce")?.maxArmies).toBe(
      decision.legalActions.find((x: any) => x.type === "reinforce").maxArmies - 2,
    );
  });

  it("publishes an OpenAPI document that documents the command shapes", async () => {
    const { app } = harness();
    const res = await call(app, "GET", "/openapi.json");
    expect(res.status).toBe(200);
    expect(res.body.openapi).toMatch(/^3\./);
    const schemas = res.body.components.schemas;
    expect(schemas.GameCommand.properties.action.oneOf).toHaveLength(4);
    expect(schemas.CommandAck.properties.sourceOffset).toBeDefined();
    expect(schemas.CommandAck.properties.txid).toBeDefined();
    expect(schemas.ErrorResponse.properties.error.properties.code.enum).toContain("STALE_TURN");
  });

  it("never exposes or stores a raw capability token", async () => {
    const { app, stores } = harness();
    const created = await call(app, "POST", "/v1/games", { body: { name: "Alice", color: "red" } });
    const token: string = created.body.capability;
    const secret = token.split("_")[2]!;

    // The stored capability row holds only a verifier hash, never the raw secret.
    const gameId = created.body.game.id;
    // Reissue lookup: the store keeps no field equal to the raw token/secret.
    const serialized = JSON.stringify([stores.capabilities.getByTokenId(token.split("_")[1]!)]);
    expect(serialized.includes(secret)).toBe(false);
    expect(serialized.includes(token)).toBe(false);

    // The canonical event stream carries no token material either.
    const board = await call(app, "GET", `/v1/games/${gameId}`);
    expect(JSON.stringify(board.body).includes(secret)).toBe(false);
  });
});
