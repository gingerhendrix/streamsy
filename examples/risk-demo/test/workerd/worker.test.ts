/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/global-date, effecttsgo/node-builtin-import, effecttsgo/process-env, effecttsgo/global-console -- Black-box workerd tests own native HTTP, wall-clock deadlines, disposable storage, and outcome reporting. */
/**
 * Hex Domination on real local workerd (Miniflare), with disposable SQLite
 * Durable Object state. No Cloudflare account or network service is used.
 */
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { startWorker } from "../../scripts/worker-harness.ts";
import {
  activePlayer,
  api,
  board,
  command,
  decision,
  declareAttack,
  openActions,
  pendingAttack,
  reinforceBorder,
  resolutionsOf,
  sleep,
  startTwoPlayerGame,
  type PendingAttack,
  type TwoPlayerGame,
} from "./client.ts";

type Host = Awaited<ReturnType<typeof startWorker>>;

const scratchRoot = process.env.RISK_WORKERD_SCRATCH ?? tmpdir();

async function stateDirectory(): Promise<string> {
  await mkdir(scratchRoot, { recursive: true });
  return mkdtemp(`${scratchRoot}/risk-workerd-`);
}

const LATE_RESOLUTION_CODES = ["ATTACK_ALREADY_RESOLVED", "DEFENSE_DEADLINE_EXPIRED"];

/** From `offset` ms around the deadline, send the same defence four times, 15 ms apart. */
async function race(base: string, game: TwoPlayerGame, attack: PendingAttack, offset: number) {
  await sleep(Math.max(0, attack.deadlineAt + offset - Date.now()));
  const body = {
    commandId: `race-defence:${attack.attackId}`,
    turnId: attack.turnId,
    action: { type: "roll-defense", attackId: attack.attackId },
  };
  const attempts = [];
  for (let attempt = 0; attempt < 4; attempt += 1) {
    attempts.push(command(base, game, attack.defender, body));
    await sleep(15);
  }
  return Promise.all(attempts);
}

describe("Hex Domination edge and game object", () => {
  const DEFENSE_MS = 2_500;
  let directory: string;
  let host: Host;

  beforeAll(async () => {
    directory = await stateDirectory();
    host = await startWorker(directory, { defenseTimeoutMs: DEFENSE_MS });
  });
  afterAll(async () => {
    await host?.stop();
    await rm(directory, { recursive: true, force: true });
  });

  it("plays create, join, start, and commands with CAS receipts and an active public board", async () => {
    const game = await startTwoPlayerGame(host.url, "workerd-play");
    expect(game.gameId).toMatch(/^game_[0-9a-f]{24}$/);

    const placed = await reinforceBorder(host.url, game);
    expect(placed.ack.status).toBe("accepted");
    const retry = await command(host.url, game, placed.attacker, placed.body);
    expect(retry.body.status).toBe("duplicate");
    expect(retry.body.eventOffset).toBe(placed.ack.eventOffset);

    const territoryId = placed.body.action.placements[0]!.territoryId;
    const reused = await command(host.url, game, placed.attacker, {
      ...placed.body,
      action: { type: "reinforce", placements: [{ territoryId, armies: 1 }] },
    });
    expect(reused.body.status).toBe("rejected");
    expect(reused.body.error.code).toBe("COMMAND_ID_REUSED");

    const projected = await board(host.url, game);
    expect(projected.sourceThroughOffset).toBe(placed.ack.eventOffset);
    const stream = await fetch(
      `${host.url}/streams/games/${game.gameId}/projections/board/${projected.generation}?offset=-1`,
    );
    expect(stream.status).toBe(200);
    expect(await stream.text()).toContain(placed.ack.eventOffset);
  });

  it("isolates two games and exposes no private or retired stream", async () => {
    const first = await startTwoPlayerGame(host.url, "workerd-isolation-a");
    const second = await startTwoPlayerGame(host.url, "workerd-isolation-b");
    expect(second.gameId).not.toBe(first.gameId);

    // Each game is its own object with its own capability table, so another
    // game's token does not exist there at all.
    const crossed = await api(host.url, "GET", `/v1/games/${second.gameId}/decision`, {
      token: first.tokens[first.host],
    });
    expect(crossed.status).toBe(401);
    expect(crossed.body.error.code).toBe("UNAUTHORIZED");

    // The edge replaces a client-supplied routing header.
    const spoofed = await api(host.url, "GET", `/v1/games/${first.gameId}`, {
      headers: { "x-risk-game-id": second.gameId },
    });
    expect(spoofed.status).toBe(200);
    expect(spoofed.body.gameId).toBe(first.gameId);

    const hidden = [
      `/streams/games/${first.gameId}/events`,
      `/streams/games/${first.gameId}/players/${first.host}/actions`,
      `/streams/games/${first.gameId}/projections/board/board2`,
      `/v1/stream/games/${first.gameId}/events`,
      `/v1/games/game_000000000000000000000000`,
      `/v1/games/not-a-game/board`,
      `/agent/${first.gameId}`,
    ];
    for (const path of hidden) {
      const reply = await api(host.url, "GET", path, { token: first.tokens[first.host] });
      expect({ path, status: reply.status }).toEqual({ path, status: 404 });
    }

    const lobby = await api(host.url, "POST", "/v1/games", { body: { name: "Lobby" } });
    const guest = await api(host.url, "POST", `/v1/games/${lobby.body.game.id}/players`, {
      body: { name: "Guest" },
    });
    const forbidden = await api(host.url, "POST", `/v1/games/${lobby.body.game.id}/start`, {
      token: guest.body.capability,
      body: {},
    });
    expect(forbidden.status).toBe(403);
  });

  it("serves each seat's actions over bearer-scoped SSE and resumes from a cursor", async () => {
    const game = await startTwoPlayerGame(host.url, "workerd-sse");
    const other = await startTwoPlayerGame(host.url, "workerd-sse-other");

    const seat = await activePlayer(host.url, game);
    const opened = await openActions(host.url, game.gameId, game.tokens[seat]);
    expect(opened.response.status).toBe(200);
    expect(opened.response.headers.get("content-type")).toContain("text/event-stream");
    expect(opened.response.headers.get("cache-control")).toBe("no-store");
    const opening = await opened.reader.next(10_000);
    await opened.reader.close();
    expect(opening?.messages.length).toBeGreaterThan(0);
    for (const message of opening?.messages ?? []) {
      expect(message).toMatchObject({ gameId: game.gameId, playerId: seat });
    }

    const resumed = await openActions(
      host.url,
      game.gameId,
      game.tokens[seat],
      opening!.nextOffset,
    );
    const caughtUp = await resumed.reader.next(10_000);
    expect(caughtUp?.messages).toEqual([]);
    expect(caughtUp?.nextOffset).toBe(opening!.nextOffset);
    expect(await resumed.reader.next(1_000)).toBeNull();
    await resumed.reader.close();

    const anonymous = await api(host.url, "GET", `/v1/games/${game.gameId}/players/me/actions`);
    expect(anonymous.status).toBe(401);
    const foreign = await api(host.url, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token: other.tokens[other.host],
    });
    expect(foreign.status).toBe(401);
    const longPoll = await api(
      host.url,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?wait=1000`,
      { token: game.tokens[game.host] },
    );
    expect(longPoll.status).toBe(400);
  });

  it("serves the SPA shell and its bundle without exposing the Worker", async () => {
    const health = await api(host.url, "GET", "/healthz");
    expect(health.body).toEqual({ ok: true });
    expect((await api(host.url, "GET", "/openapi.json")).status).toBe(200);

    for (const path of ["/", "/game/game_00112233445566778899aabb"]) {
      const page = await fetch(`${host.url}${path}`);
      expect(page.status).toBe(200);
      const html = await page.text();
      expect(html).toContain('id="root"');
      const script = /<script[^>]+src="([^"]+)"/.exec(html)?.[1];
      expect(script).toBeDefined();
      const bundle = await fetch(new URL(script!, `${host.url}${path}`));
      expect(bundle.status).toBe(200);
      expect(bundle.headers.get("content-type")).toContain("javascript");
    }
    const worker = await fetch(`${host.url}/worker.js`);
    expect(await worker.text()).not.toContain("GameDurableObject");
  });

  it("resolves an expired defence from the alarm and wakes a waiting reader", async () => {
    const game = await startTwoPlayerGame(host.url, "workerd-alarm");
    const attack = await declareAttack(host.url, game);
    const opened = await openActions(host.url, game.gameId, game.tokens[attack.attacker]);
    const opening = await opened.reader.next(10_000);
    expect(opening).not.toBeNull();

    // No command follows. Only the object's alarm can resolve the defence.
    const woke = await opened.reader.next(DEFENSE_MS + 10_000);
    const wokeAt = Date.now();
    await opened.reader.close();
    expect(woke?.messages.length).toBeGreaterThan(0);
    expect(wokeAt).toBeGreaterThanOrEqual(attack.deadlineAt);

    const resolved = await resolutionsOf(host.url, game, attack.attackId);
    expect(resolved).toHaveLength(1);
    expect(resolved[0].resolutionSource).toBe("timeout");
    const late = await command(host.url, game, attack.defender, {
      commandId: "late-defence",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    });
    expect(LATE_RESOLUTION_CODES).toContain(late.body.error?.code);
    expect(await resolutionsOf(host.url, game, attack.attackId)).toHaveLength(1);
  });

  it("resolves human-versus-alarm races at the deadline exactly once", async () => {
    const offsets = [-60, -20, 0, 20, 60];
    const games = await Promise.all(
      offsets.map((_, index) => startTwoPlayerGame(host.url, `workerd-race-${index}`)),
    );
    const attacks = await Promise.all(games.map((game) => declareAttack(host.url, game)));

    const replies = await Promise.all(
      games.map((game, index) => race(host.url, game, attacks[index]!, offsets[index]!)),
    );
    await sleep(DEFENSE_MS);

    const outcomes: string[] = [];
    for (const [index, game] of games.entries()) {
      const attack = attacks[index]!;
      const resolved = await resolutionsOf(host.url, game, attack.attackId);
      expect(resolved).toHaveLength(1);
      const statuses = replies[index]!.map((reply) => reply.body.status);
      const source = resolved[0].resolutionSource;
      outcomes.push(source);
      if (source === "human") {
        expect(statuses.filter((status) => status === "accepted")).toHaveLength(1);
        expect(statuses.every((status) => status === "accepted" || status === "duplicate")).toBe(
          true,
        );
      } else {
        expect(source).toBe("timeout");
        for (const reply of replies[index]!) {
          expect(reply.body.status).toBe("rejected");
          expect(LATE_RESOLUTION_CODES).toContain(reply.body.error.code);
        }
      }
    }
    console.info(`race outcomes by offset ${offsets.join(",")}: ${outcomes.join(",")}`);
  });
});

describe("Hex Domination after workerd restarts on the same storage", () => {
  let directory: string;
  let host: Host | undefined;

  beforeAll(async () => {
    directory = await stateDirectory();
  });
  afterAll(async () => {
    await host?.stop();
    await rm(directory, { recursive: true, force: true });
  });

  const restart = async (defenseTimeoutMs: number) => {
    await host?.stop();
    host = await startWorker(directory, { defenseTimeoutMs });
    return host;
  };

  it("resolves a deadline that passed while workerd was stopped and keeps receipts and tokens", async () => {
    const DEFENSE_MS = 1_500;
    let live = await restart(DEFENSE_MS);
    const game = await startTwoPlayerGame(live.url, "restart-expired");
    const placed = await reinforceBorder(live.url, game);
    const page = await api(live.url, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token: game.tokens[placed.attacker],
    });
    const current = await decision(live.url, game, placed.attacker);
    const choice = current.legalMoves.find((m: any) => m.type === "declare-attack").choices[0];
    const declared = await command(live.url, game, placed.attacker, {
      commandId: "restart-declare",
      turnId: current.turn.id,
      action: { type: "declare-attack", from: choice.from, to: choice.to, attackerDice: 1 },
    });
    expect(declared.status).toBe(200);
    const attack = await pendingAttack(live.url, game);
    await host!.stop();
    host = undefined;
    await sleep(Math.max(0, attack.deadlineAt + 500 - Date.now()));

    live = await restart(DEFENSE_MS);
    const resolved = await resolutionsOf(live.url, game, attack.attackId);
    expect(resolved).toHaveLength(1);
    expect(resolved[0].resolutionSource).toBe("timeout");

    // Receipts, capability verifiers, and private actions survived the restart.
    const retry = await command(live.url, game, placed.attacker, placed.body);
    expect(retry.body.status).toBe("duplicate");
    expect(retry.body.eventOffset).toBe(placed.ack.eventOffset);
    expect(await activePlayer(live.url, game)).toBe(placed.attacker);
    const after = await api(
      live.url,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${encodeURIComponent(page.body.nextOffset)}`,
      { token: game.tokens[placed.attacker] },
    );
    expect(after.status).toBe(200);
    expect(after.body.messages.length).toBeGreaterThan(0);
    const projected = await board(live.url, game);
    const stream = await fetch(
      `${live.url}/streams/games/${game.gameId}/projections/board/${projected.generation}?offset=-1`,
    );
    expect(stream.status).toBe(200);
  });

  it("re-arms a pending deadline after restart and fires it without another write", async () => {
    const DEFENSE_MS = 6_000;
    let live = await restart(DEFENSE_MS);
    const game = await startTwoPlayerGame(live.url, "restart-pending");
    const attack = await declareAttack(live.url, game);
    live = await restart(DEFENSE_MS);
    expect(Date.now()).toBeLessThan(attack.deadlineAt);

    // A read recreates the object. It must re-derive the deadline itself.
    const opened = await openActions(live.url, game.gameId, game.tokens[attack.attacker]);
    expect(await opened.reader.next(10_000)).not.toBeNull();
    const woke = await opened.reader.next(DEFENSE_MS + 10_000);
    const wokeAt = Date.now();
    await opened.reader.close();
    expect(woke?.messages.length).toBeGreaterThan(0);
    expect(wokeAt).toBeGreaterThanOrEqual(attack.deadlineAt);

    const resolved = await resolutionsOf(live.url, game, attack.attackId);
    expect(resolved).toHaveLength(1);
    expect(resolved[0].resolutionSource).toBe("timeout");
  });

  it("commits one of several competing commands exactly once when workerd stops mid-write", async () => {
    const DEFENSE_MS = 15_000;
    let live = await restart(DEFENSE_MS);
    const game = await startTwoPlayerGame(live.url, "restart-crash");
    const attacker = await activePlayer(live.url, game);
    const current = await decision(live.url, game, attacker);
    const reinforce = current.legalMoves.find((m: any) => m.type === "reinforce");
    // Each command places the whole pool, so the decider can accept only one.
    const bodies = [0, 1, 2].map((index) => ({
      commandId: `crash-reinforce-${index}`,
      turnId: current.turn.id,
      action: {
        type: "reinforce",
        placements: [{ territoryId: reinforce.territoryIds[index], armies: reinforce.pool }],
      },
    }));

    const inFlight = bodies.map((body) =>
      command(live.url, game, attacker, body).catch(() => null),
    );
    await sleep(5);
    await host!.stop();
    host = undefined;
    await Promise.all(inFlight);

    live = await restart(DEFENSE_MS);
    const settle = async () => {
      const replies = [];
      for (const body of bodies) replies.push((await command(live.url, game, attacker, body)).body);
      return replies;
    };
    const first = await settle();
    const winners = first.filter((reply) => reply.status !== "rejected");
    expect(winners).toHaveLength(1);
    expect(["accepted", "duplicate"]).toContain(winners[0].status);
    // A second retry returns the same receipts.
    const second = await settle();
    expect(second.map((reply) => reply.status === "rejected")).toEqual(
      first.map((reply) => reply.status === "rejected"),
    );
    expect(second.find((reply) => reply.status !== "rejected")).toMatchObject({
      status: "duplicate",
      eventOffset: winners[0].eventOffset,
    });

    const projected = await board(live.url, game);
    const reinforced = projected.moves.filter(
      (move: any) =>
        move.kind === "ArmiesReinforced" && move.commandId.startsWith("crash-reinforce-"),
    );
    expect(reinforced).toHaveLength(1);
    expect(reinforced[0].commandId).toBe(winners[0].commandId);
    expect(projected.sourceThroughOffset).toBe(winners[0].eventOffset);
    // "duplicate" means the winner was committed before workerd stopped.
    console.info(`crash winner after restart: ${winners[0].status}`);
  });
});
