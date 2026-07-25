import { describe, expect, it } from "vitest";

import {
  agentSeatBootstrapDocument,
  agentSeatPrompt,
  privateAgentSeatUrl,
} from "../../src/application/agent-seat-bootstrap.ts";
import { BASE, call, createV2Game, v2Harness } from "../v2-harness.ts";

describe("external-agent seat bootstrap", () => {
  it("keeps the capability in the fragment and out of the fragment-free document", () => {
    const capability = "rsk_tokenid_supersecret";
    const privateUrl = privateAgentSeatUrl({
      origin: BASE,
      gameId: "game/a",
      playerId: "player one",
      capability,
    });

    expect(privateUrl).toBe(`${BASE}/agent-seat/game%2Fa/player%20one#token=${capability}`);
    const parsed = new URL(privateUrl);
    expect(parsed.search).toBe("");
    expect(parsed.pathname).not.toContain(capability);

    const document = agentSeatBootstrapDocument({
      origin: BASE,
      gameId: "game/a",
      playerId: "player one",
    });
    expect(document).toContain("This document is NON-SECRET");
    expect(document).toContain("complete seat URL you were given is SECRET");
    expect(document).toContain(`OpenAPI: ${BASE}/openapi.json`);
    expect(document).toContain("Authorization: Bearer <capability>");
    expect(document).toContain(agentSeatPrompt());
    expect(document).not.toContain(capability);
  });

  it("serves a non-secret, non-cacheable document and rejects every query string", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const playerId = game.players[1]!;
    const capability = game.tokenByPlayer[playerId]!;
    const path = `/agent-seat/${game.gameId}/${playerId}`;

    const response = await h.app.fetch(new Request(`${BASE}${path}`));
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/plain; charset=utf-8");
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("referrer-policy")).toBe("no-referrer");
    const body = await response.text();
    expect(body).toContain(`Game ID: ${game.gameId}`);
    expect(body).toContain(`Player ID: ${playerId}`);
    expect(body).not.toContain(capability);

    const query = await h.app.fetch(
      new Request(`${BASE}${path}?token=${encodeURIComponent(capability)}`),
    );
    expect(query.status).toBe(400);
    const queryBody = await query.text();
    expect(queryBody).toContain("Query parameters are not accepted");
    expect(queryBody).not.toContain(capability);
  });

  it("does not reveal a seat capability through public game, board, stream, or OpenAPI", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const capability = game.tokenByPlayer[game.players[1]!]!;
    const secret = capability.split("_")[2]!;
    const publicBodies: string[] = [];

    for (const path of [
      `/v1/games/${game.gameId}`,
      `/v1/games/${game.gameId}/board`,
      "/openapi.json",
    ]) {
      const response = await h.app.fetch(new Request(`${BASE}${path}`));
      expect(response.status).toBe(200);
      publicBodies.push(await response.text());
    }

    const metadata = JSON.parse(publicBodies[0]!);
    const stream = await h.app.fetch(new Request(`${BASE}/streams/${metadata.boardStreamId}`));
    expect(stream.status).toBe(200);
    publicBodies.push(await stream.text());

    for (const body of publicBodies) {
      expect(body).not.toContain(capability);
      expect(body).not.toContain(secret);
    }

    const queryAuth = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/decision?token=${encodeURIComponent(capability)}`,
    );
    expect(queryAuth.status).toBe(401);
  });
});
