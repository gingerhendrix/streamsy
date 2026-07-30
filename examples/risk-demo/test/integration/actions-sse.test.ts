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
  ACTIONS_STREAM_TIMEOUT_MS,
  readActionsBatches,
  type ActionsBatch,
} from "../../src/application/actions-stream.ts";
import type { AgentMessage } from "../../server/game/action-notifier.ts";
import { createBot } from "../../server/demo/bot.ts";
import {
  BASE,
  call,
  createGame,
  httpFor,
  riskHarness,
  streamFor,
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
  for await (const batch of readActionsBatches<AgentMessage>(response)) {
    batches.push(batch);
    if (enough(batch)) break;
  }
  return batches;
}

async function twoAgentGame(h: Harness) {
  const game = await createGame(h.app, { controllers: ["agent", "agent"] });
  const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
  const active = meta.activePlayerId as string;
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
    for await (const batch of readActionsBatches<AgentMessage>(response)) batches.push(batch);
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
        for await (const batch of readActionsBatches<AgentMessage>(response)) batches.push(batch);
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
