/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
/* oxlint-disable effecttsgo/global-timers, effecttsgo/new-promise -- These integration tests directly coordinate abortable Web SSE timing at the Promise-facing HTTP contract. */
/**
 * The agent-facing actions resource is a bounded `text/event-stream`.
 *
 * These tests hold real connections against the in-process app: framing and
 * headers, the immediate backlog, waking on a live action, the server's own
 * connection bound, exact resume across a reconnection, and a client that walks
 * away mid-hold. The 30-second bound is asserted as configuration — the route
 * defaults to it, and the behaviour is proven against an injected short one, so
 * nothing here waits 30 real seconds.
 */

import { describe, expect, it } from "vitest";

import {
  ACTIONS_STREAM_CLIENT_TIMEOUT_MS,
  ACTIONS_STREAM_TIMEOUT_MS,
  readActionsBatches,
  type ActionsBatch,
} from "../../src/application/actions-stream.ts";
import { AgentMessageSchema, type AgentMessage } from "../../server/game/action-notifier.ts";
import { createBot } from "../../server/demo/bot.ts";
import {
  BASE,
  call,
  checkedString,
  createGame,
  httpFor,
  riskHarness,
  streamFor,
  type Game,
  type Harness,
} from "../harness.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function open(
  h: Harness,
  gameId: string,
  token: string,
  options: { offset?: string; signal?: AbortSignal; accept?: string } = {},
): Promise<Response> {
  const query = options.offset ? `?offset=${encodeURIComponent(options.offset)}` : "";
  const headers: Record<string, string> = { accept: options.accept ?? "text/event-stream" };
  if (token) headers.authorization = `Bearer ${token}`;
  return h.app.fetch(
    new Request(`${BASE}/v1/games/${gameId}/players/me/actions${query}`, {
      headers,
      signal: options.signal,
    }),
  );
}

/** Take batches until `enough` says stop, then drop the connection. */
async function take(
  response: Response,
  enough: (batch: ActionsBatch<AgentMessage>) => boolean,
): Promise<ActionsBatch<AgentMessage>[]> {
  const batches: ActionsBatch<AgentMessage>[] = [];
  for await (const batch of readActionsBatches(response, AgentMessageSchema)) {
    batches.push(batch);
    if (enough(batch)) break;
  }
  return batches;
}

/** Drive both seats until the game is over, and answer with its final metadata. */
async function playToCompletion(h: Harness, game: Game) {
  const bots = Object.fromEntries(
    game.players.map((playerId) => [
      playerId,
      createBot({
        call: httpFor(h.app),
        gameId: game.gameId,
        playerId,
        token: game.tokenByPlayer[playerId]!,
      }),
    ]),
  );
  for (let step = 0; step < 4_000; step += 1) {
    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    if (meta.status === "finished") return meta;
    const bot = bots[checkedString(meta.activePlayerId, "active player id")]!;
    await bot.awaitTurn();
    await bot.playTurn();
  }
  throw new Error("the game never finished");
}

async function twoAgentGame(h: Harness) {
  const game = await createGame(h.app, { controllers: ["agent", "agent"] });
  const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
  const active = checkedString(meta.activePlayerId, "active player id");
  const idle = game.players.find((player) => player !== active)!;
  return { game, active, idle };
}

describe("actions stream (SSE)", () => {
  it("is capability-gated, uncacheable, and framed as data/control events", async () => {
    // A short server bound so the whole response can simply be read to its end.
    const h = riskHarness(11, { actionsStreamTimeoutMs: 50 });
    const { game, active } = await twoAgentGame(h);

    const anonymous = await open(h, game.gameId, "");
    expect(anonymous.status).toBe(401);

    const response = await open(h, game.gameId, game.tokenByPlayer[active]!);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("referrer-policy")).toBe("no-referrer");

    const body = await response.text();
    expect(body).toContain("event: data\ndata:[\n");
    expect(body).toContain("event: control\ndata:{");
    expect(body).toContain('"type":"ActionRequired"');
    // Every batch ends with a control frame, so a cursor is always re-stated.
    expect(body.trimEnd().endsWith("}")).toBe(true);
  });

  it("writes the backlog immediately, without waiting for anything to happen", async () => {
    const h = riskHarness(11, { actionsStreamTimeoutMs: 5_000 });
    const { game, active } = await twoAgentGame(h);

    const started = performance.now();
    const [first] = await take(await open(h, game.gameId, game.tokenByPlayer[active]!), () => true);
    // The opening ask already exists, so it must arrive at once — not after the
    // connection's bound, and not after the next thing to happen.
    expect(performance.now() - started).toBeLessThan(1_000);
    expect(first!.messages).toHaveLength(1);
    expect(first!.messages[0]).toMatchObject({
      type: "ActionRequired",
      seq: 1,
      playerId: active,
      reason: "turn-started",
    });
    expect(first!.nextOffset).toBeTruthy();
    expect(first!.upToDate).toBe(true);
  });

  it("holds the connection open and delivers an action the moment it lands", async () => {
    const h = riskHarness(11, { actionsStreamTimeoutMs: 10_000 });
    const { game, active, idle } = await twoAgentGame(h);

    const opening = await take(await open(h, game.gameId, game.tokenByPlayer[idle]!), () => true);
    const resumeFrom = opening.at(-1)!.nextOffset;

    const started = performance.now();
    const held = take(
      await open(h, game.gameId, game.tokenByPlayer[idle]!, { offset: resumeFrom }),
      (batch) => batch.messages.length > 0,
    );

    await sleep(75);
    const bot = createBot({
      call: httpFor(h.app),
      gameId: game.gameId,
      playerId: active,
      token: game.tokenByPlayer[active]!,
    });
    await bot.playTurn();

    const batches = await held;
    // It waited rather than returning an empty page, and it woke on the append.
    expect(performance.now() - started).toBeGreaterThanOrEqual(60);
    expect(batches.at(-1)!.messages.at(-1)).toMatchObject({
      type: "ActionRequired",
      playerId: idle,
      reason: "turn-started",
    });
  });

  it("closes on its own configured bound, which is 30 seconds", async () => {
    expect(ACTIONS_STREAM_TIMEOUT_MS).toBe(30_000);

    const h = riskHarness(11, { actionsStreamTimeoutMs: 80 });
    const { game, idle } = await twoAgentGame(h);
    const opening = await take(await open(h, game.gameId, game.tokenByPlayer[idle]!), () => true);

    const started = performance.now();
    const response = await open(h, game.gameId, game.tokenByPlayer[idle]!, {
      offset: opening.at(-1)!.nextOffset,
    });
    const batches: ActionsBatch<AgentMessage>[] = [];
    for await (const batch of readActionsBatches(response, AgentMessageSchema)) batches.push(batch);
    const elapsed = performance.now() - started;

    // The stream ended by itself with nothing to report, and it held for the
    // bound rather than returning empty at once.
    expect(elapsed).toBeGreaterThanOrEqual(60);
    expect(batches.every((batch) => batch.messages.length === 0)).toBe(true);
    // The cursor survives the close, so the reconnection resumes in place.
    expect(batches.at(-1)!.nextOffset).toBe(opening.at(-1)!.nextOffset);
    expect(batches.at(-1)!.closed).toBe(false);
  });

  it("resumes across a reconnection with no gap and no duplicate", async () => {
    const h = riskHarness(11, { actionsStreamTimeoutMs: 10_000 });
    const { game, active } = await twoAgentGame(h);
    const token = game.tokenByPlayer[active]!;

    const first = await take(await open(h, game.gameId, token), () => true);
    const seen = first.flatMap((batch) => batch.messages);
    expect(seen.at(-1)!.seq).toBe(1);

    // Act on the ask, then reconnect from exactly where the control frame said.
    const bot = createBot({ call: httpFor(h.app), gameId: game.gameId, playerId: active, token });
    await bot.step();

    const resumed = await take(
      await open(h, game.gameId, token, { offset: first.at(-1)!.nextOffset }),
      (batch) => batch.messages.length > 0,
    );
    const next = resumed.flatMap((batch) => batch.messages);
    expect(next.length).toBeGreaterThan(0);
    // Dense continuation: nothing replayed, nothing skipped.
    expect(next[0]!.seq).toBe(2);
    expect(next.map((message) => message.seq)).toEqual(next.map((_, index) => index + 2));
    expect(next.some((message) => message.messageId === seen.at(-1)!.messageId)).toBe(false);
  });

  it("stops cleanly when the client walks away mid-hold", async () => {
    const h = riskHarness(11, { actionsStreamTimeoutMs: 10_000 });
    const { game, idle } = await twoAgentGame(h);
    const token = game.tokenByPlayer[idle]!;
    const opening = await take(await open(h, game.gameId, token), () => true);

    const abort = new AbortController();
    const response = await open(h, game.gameId, token, {
      offset: opening.at(-1)!.nextOffset,
      signal: abort.signal,
    });
    const consumed = (async () => {
      const batches: ActionsBatch<AgentMessage>[] = [];
      try {
        for await (const batch of readActionsBatches(response, AgentMessageSchema))
          batches.push(batch);
      } catch {
        // An aborted body is the ordinary shape of a client disconnect.
      }
      return batches;
    })();
    await sleep(25);
    abort.abort();
    await consumed;

    // The seat is not wedged: a fresh read still serves the same position.
    const page = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${opening.at(-1)!.nextOffset}`,
      { token },
    );
    expect(page.status).toBe(200);
    expect(page.body.nextOffset).toBe(opening.at(-1)!.nextOffset);
  });

  it("marks the batch that delivers GameOver closed, and holds silently past it", async () => {
    const h = riskHarness(4242, { actionsStreamTimeoutMs: 80 });
    const game = await createGame(h.app, {
      controllers: ["agent", "agent"],
      mapSeed: "sse-game-over",
    });
    const meta = await playToCompletion(h, game);
    const token = game.tokenByPlayer[checkedString(meta.winnerId, "winner id")]!;

    // Read the seat's whole stream: the batch carrying `GameOver` says the
    // server is done, and the connection ends there rather than at its bound.
    const started = performance.now();
    const batches = await take(await open(h, game.gameId, token), (batch) => batch.closed);
    const terminal = batches.at(-1)!;
    expect(terminal.closed).toBe(true);
    expect(terminal.messages.at(-1)!.type).toBe("GameOver");
    expect(performance.now() - started).toBeLessThan(80);

    // Past that offset the server has nothing left to say and no way to say
    // so: an empty control, no `closed`, and the connection ends on the bound.
    // A client therefore records its own completion when it takes a
    // `GameOver`, which is exactly what the launcher persists.
    const past: ActionsBatch<AgentMessage>[] = [];
    for await (const batch of readActionsBatches(
      await open(h, game.gameId, token, { offset: terminal.nextOffset }),
      AgentMessageSchema,
    )) {
      past.push(batch);
    }
    expect(past.length).toBeGreaterThan(0);
    expect(past.every((batch) => batch.messages.length === 0 && !batch.closed)).toBe(true);
    expect(past.at(-1)!.nextOffset).toBe(terminal.nextOffset);
  }, 60_000);

  it("serves the representation the Accept header actually asked for", async () => {
    const h = riskHarness(11, { actionsStreamTimeoutMs: 50 });
    const { game, active } = await twoAgentGame(h);
    const token = game.tokenByPlayer[active]!;
    const typeOf = async (accept?: string) =>
      (await open(h, game.gameId, token, { accept })).headers.get("content-type");

    // The stream is the contract: silence, wildcards and ties all mean SSE.
    expect(await typeOf(undefined)).toBe("text/event-stream");
    expect(await typeOf("*/*")).toBe("text/event-stream");
    expect(await typeOf("application/json, text/event-stream")).toBe("text/event-stream");
    // Asking for the page, in whatever casing, or refusing the stream outright.
    expect(await typeOf("application/json")).toBe("application/json");
    expect(await typeOf("Application/JSON")).toBe("application/json");
    expect(await typeOf("text/event-stream;q=0, application/json")).toBe("application/json");
  });

  it("refuses the long poll it replaced, and any other unknown parameter", async () => {
    const h = riskHarness();
    const game = await createGame(h.app);
    const token = game.tokenByPlayer[game.players[0]!]!;

    const waited = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?wait=30000`,
      { token },
    );
    expect(waited.status).toBe(400);
    expect(waited.body.error.message).toContain("Server-Sent Events");

    const unknown = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?unexpected=value`,
      { token },
    );
    expect(unknown.status).toBe(400);
  });

  it("lets the server end an idle connection, not the client's guard timer", async () => {
    // The client guard must outlast the server bound: a client timer is already
    // running through connect, auth and catch-up, so one set to the server's
    // bound exactly fires first and discards the closing control frame.
    expect(ACTIONS_STREAM_CLIENT_TIMEOUT_MS).toBe(ACTIONS_STREAM_TIMEOUT_MS + 5_000);
    expect(ACTIONS_STREAM_CLIENT_TIMEOUT_MS).toBeGreaterThan(ACTIONS_STREAM_TIMEOUT_MS);

    const h = riskHarness(11, { actionsStreamTimeoutMs: 80 });
    const { game, idle } = await twoAgentGame(h);
    // A seat with no cursor yet and nothing to be told: this connection can only
    // end by the server closing it or by the client aborting it.
    const waiting = createBot({
      call: httpFor(h.app),
      openStream: streamFor(h.app),
      gameId: game.gameId,
      playerId: idle,
      token: game.tokenByPlayer[idle]!,
      state: {},
    });
    expect(waiting.state.cursor).toBeUndefined();

    const started = performance.now();
    const wake = await waiting.awaitTurn(5_000);
    const elapsed = performance.now() - started;

    expect(wake).toBeNull();
    // It ended at the server's bound, nowhere near the client's guard…
    expect(elapsed).toBeGreaterThanOrEqual(60);
    expect(elapsed).toBeLessThan(2_000);
    // …and it ended by *closing*, so the control frame's cursor was received and
    // persisted. A client-aborted connection carries no offset at all, and this
    // seat would have reconnected from the beginning every time instead.
    expect(waiting.state.cursor).toBeTruthy();
  });

  it("lets a scripted bot block on the stream for its turn", async () => {
    const h = riskHarness(11, { actionsStreamTimeoutMs: 10_000 });
    const { game, active, idle } = await twoAgentGame(h);

    const waiting = createBot({
      call: httpFor(h.app),
      openStream: streamFor(h.app),
      gameId: game.gameId,
      playerId: idle,
      token: game.tokenByPlayer[idle]!,
    });
    await waiting.awaitTurn();

    const blocked = waiting.awaitTurn(5_000);
    await sleep(50);
    const mover = createBot({
      call: httpFor(h.app),
      gameId: game.gameId,
      playerId: active,
      token: game.tokenByPlayer[active]!,
    });
    await mover.playTurn();

    expect(await blocked).toMatchObject({ type: "ActionRequired", playerId: idle });
  });
});
