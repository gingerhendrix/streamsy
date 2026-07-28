import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import { buildApp } from "../../server/http/app.ts";
import { createInMemoryStores } from "../../server/persistence/stores.ts";
import { createBot } from "../../server/demo/bot.ts";
import { call, createV2Game } from "../v2-harness.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("actions stream long poll", () => {
  it("honors wait and wakes when control passes", async () => {
    const protocol = createStreamProtocol({
      storage: { adapter: createMemoryStorageAdapter() },
      longPollTimeoutMs: 20,
    });
    const app = buildApp({ protocol, stores: createInMemoryStores() });
    const game = await createV2Game(app, { controllers: ["agent", "agent"] });
    const meta = (await call(app, "GET", `/v1/games/${game.gameId}`)).body;
    const active = meta.activePlayerId;
    const inactive = game.players.find((player) => player !== active)!;
    const initial = await call(app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token: game.tokenByPlayer[inactive]!,
    });
    const started = performance.now();
    const pending = call(
      app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${initial.body.nextOffset}&wait=3000`,
      { token: game.tokenByPlayer[inactive]! },
    );
    await sleep(75);
    const bot = createBot({
      call: (method, path, options) => call(app, method, path, options),
      gameId: game.gameId,
      playerId: active,
      token: game.tokenByPlayer[active]!,
    });
    await bot.playTurn();
    const result = await pending;
    expect(performance.now() - started).toBeGreaterThanOrEqual(60);
    expect(result.body.messages.at(-1)).toMatchObject({
      type: "ActionRequired",
      playerId: inactive,
      reason: "turn-started",
    });
  });
});
