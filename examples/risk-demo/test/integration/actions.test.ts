import { describe, expect, it } from "vitest";
import { createJsonProtocol } from "@streamsy/json";
import {
  call,
  createV2Game,
  decisionFor,
  httpFor,
  restartV2,
  v2Harness,
  type V2Game,
  type V2Harness,
} from "../v2-harness.ts";
import { createBot, type BotState, type HttpCall } from "../../server/demo/bot.ts";
import { deriveActions, type AgentMessage } from "../../server/game/action-notifier.ts";
import { actionStreamId, eventStreamId } from "../../server/game/names.ts";

const passthrough = { encode: (value: any) => value, decode: (value: any) => value };

/** Read a player's whole action stream directly, without disturbing any cursor. */
async function allMessages(
  h: V2Harness,
  gameId: string,
  playerId: string,
): Promise<AgentMessage[]> {
  const stream = await createJsonProtocol(h.protocol, passthrough).getOrCreate(
    actionStreamId(gameId, playerId),
  );
  return (await stream.readAll()).messages.map((message) => message.value as AgentMessage);
}

/**
 * Play a whole game with every seat driven **only** by its action stream. The
 * returned counters prove the claim that matters: after one bootstrap read,
 * steady-state play makes no `/decision` round trips at all.
 */
async function playByMessagesOnly(
  h: V2Harness,
  game: V2Game,
): Promise<{ decisionCalls: number; commandCalls: number }> {
  let decisionCalls = 0;
  let commandCalls = 0;
  const counting: HttpCall = (method, path, options) => {
    if (path.includes("/decision")) decisionCalls += 1;
    if (path.includes("/commands")) commandCalls += 1;
    return call(h.app, method, path, options);
  };
  const bots = Object.fromEntries(
    game.players.map((playerId) => [
      playerId,
      createBot({
        call: counting,
        gameId: game.gameId,
        playerId,
        token: game.tokenByPlayer[playerId]!,
      }),
    ]),
  );

  for (let round = 0; round < 4000; round += 1) {
    let progressed = false;
    for (const playerId of game.players) {
      const message = await bots[playerId]!.awaitTurn();
      if (!message) continue;
      if (message.type === "GameOver") return { decisionCalls, commandCalls };
      if (await bots[playerId]!.step()) progressed = true;
    }
    if (!progressed) break;
  }
  throw new Error("the message-driven game never reached GameOver");
}

describe("player actions stream", () => {
  it("is self-sufficient, cursor-resumable, and emits after an accepted action", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    const active = meta.activePlayerId;
    const token = game.tokenByPlayer[active]!;

    const first = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token,
    });
    expect(first.status).toBe(200);
    expect(first.body.messages).toHaveLength(1);
    expect(first.body.messages[0]).toMatchObject({
      type: "ActionRequired",
      seq: 1,
      playerId: active,
      reason: "turn-started",
      mode: "active-turn",
    });
    expect(first.body.messages[0].legalMoves.length).toBeGreaterThan(0);
    expect(first.body.messages[0].board.territories.length).toBeGreaterThan(0);
    expect(first.body.messages[0].since.events.at(-1).type).toBe("GameStarted");

    const resumed = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${first.body.nextOffset}`,
      { token },
    );
    expect(resumed.body.messages).toEqual([]);

    const decision = await decisionFor(h.app, game, active);
    const reinforce = decision.legalMoves.find((move: any) => move.type === "reinforce");
    const command = await call(h.app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "actions-reinforce",
        turnId: decision.turn.id,
        action: {
          type: "reinforce",
          placements: [{ territoryId: reinforce.territoryIds[0], armies: reinforce.pool }],
        },
      },
    });
    expect(command.status).toBe(200);

    const next = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${first.body.nextOffset}`,
      { token },
    );
    // The placement spent the whole pool, so the turn moved into `attack`. That
    // is a phase change, not "there is still reinforcement to place".
    expect(next.body.messages.at(-1)).toMatchObject({
      type: "ActionRequired",
      seq: 2,
      reason: "phase-changed",
      turn: { phase: "attack" },
    });
    expect(
      next.body.messages
        .at(-1)
        .since.events.some((event: any) => event.type === "ArmiesReinforced"),
    ).toBe(true);
  });

  it("separates a partly-spent reinforcement pool from the placement that empties it", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    const active = meta.activePlayerId as string;
    const token = game.tokenByPlayer[active]!;

    const opening = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token,
    });
    expect(opening.body.messages.at(-1).reason).toBe("turn-started");

    // One command, two placements: the deriver folds the events one at a time,
    // so the pool is briefly non-empty between them.
    const decision = await decisionFor(h.app, game, active);
    const reinforce = decision.legalMoves.find((move: any) => move.type === "reinforce");
    expect(reinforce.pool).toBeGreaterThanOrEqual(2);
    expect(reinforce.territoryIds.length).toBeGreaterThanOrEqual(2);
    const submitted = await call(h.app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "reason-mapping-reinforce",
        turnId: decision.turn.id,
        action: {
          type: "reinforce",
          placements: [
            { territoryId: reinforce.territoryIds[0], armies: reinforce.pool - 1 },
            { territoryId: reinforce.territoryIds[1], armies: 1 },
          ],
        },
      },
    });
    expect(submitted.status).toBe(200);

    const page = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${opening.body.nextOffset}`,
      { token },
    );
    const asks = page.body.messages.filter((m: any) => m.type === "ActionRequired");
    // Pool still holding armies → asked again in the same phase.
    expect(asks.at(-2)).toMatchObject({
      reason: "reinforcement-remaining",
      turn: { phase: "reinforce" },
    });
    expect(asks.at(-2).turn.reinforcement.remaining).toBeGreaterThan(0);
    // Pool emptied → the turn is in a different phase, and says so.
    expect(asks.at(-1)).toMatchObject({ reason: "phase-changed", turn: { phase: "attack" } });
    expect(asks.at(-1).turn.reinforcement.remaining).toBe(0);
  });

  it("rejects unknown query parameters", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const response = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?unexpected=value`,
      { token: game.tokenByPlayer[game.players[0]!]! },
    );
    expect(response.status).toBe(400);
  });

  it("returns an empty, up-to-date page when a bounded wait expires", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    const idle = game.players.find((player) => player !== meta.activePlayerId)!;
    const token = game.tokenByPlayer[idle]!;

    const drained = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token,
    });
    const timed = await call(
      h.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${drained.body.nextOffset}&wait=25`,
      { token },
    );
    expect(timed.status).toBe(200);
    expect(timed.body.messages).toEqual([]);
    expect(timed.body.upToDate).toBe(true);
    // The cursor survives a timeout, so the next poll resumes from the same place.
    expect(timed.body.nextOffset).toBe(drained.body.nextOffset);
  });

  it("drives a complete two-seat game from messages alone, with no steady-state /decision reads", async () => {
    const h = v2Harness(4242);
    const game = await createV2Game(h.app, {
      controllers: ["agent", "agent"],
      mapSeed: "actions-cadence",
    });

    const counts = await playByMessagesOnly(h, game);
    // Exactly one bootstrap read per seat, and nothing after that.
    expect(counts.decisionCalls).toBe(game.players.length);
    expect(counts.commandCalls).toBeGreaterThan(20);

    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    expect(meta.status).toBe("finished");

    for (const playerId of game.players) {
      const messages = await allMessages(h, game.gameId, playerId);
      // `seq` is dense and 1-based, and `messageId` derives from it.
      messages.forEach((message, index) => {
        expect(message.seq).toBe(index + 1);
        expect(message.messageId).toBe(`act:${game.gameId}:${playerId}:${index + 1}`);
        expect(message.playerId).toBe(playerId);
      });

      // Every non-terminal message is actionable and self-sufficient.
      const actionable = messages.filter((message) => message.type === "ActionRequired");
      for (const message of actionable) {
        expect(message.legalMoves.length).toBeGreaterThan(0);
        expect(message.board.territories.length).toBeGreaterThan(0);
        expect(message.board.players).toHaveLength(game.players.length);
        expect(message.turn.id).toContain(message.turn.activePlayerId);
      }

      // The terminal message is a `GameOver` naming the same winner for everyone.
      const last = messages.at(-1)!;
      expect(last.type).toBe("GameOver");
      expect(last.type === "GameOver" && last.winner.id).toBe(meta.winnerId);
      expect(messages.filter((message) => message.type === "GameOver")).toHaveLength(1);

      // `since` chains without gaps: each message resumes at its predecessor's offset.
      messages.forEach((message, index) => {
        expect(message.since.fromEventOffset).toBe(
          index === 0 ? null : messages[index - 1]!.eventOffset,
        );
        expect(message.since.events.length).toBeGreaterThan(0);
      });
    }

    // The §5.3 cadence: the first ask of the game is a turn start, an occupation
    // is always demanded after a capture, and every reason emitted is a declared one.
    const winnerMessages = await allMessages(h, game.gameId, meta.winnerId);
    const reasons = winnerMessages
      .filter((message) => message.type === "ActionRequired")
      .map((message) => (message.type === "ActionRequired" ? message.reason : ""));
    expect(new Set(reasons)).toEqual(
      new Set(
        [...new Set(reasons)].filter((reason) =>
          [
            "turn-started",
            "phase-changed",
            "reinforcement-remaining",
            "attack-resolved",
            "occupation-required",
            "defense-required",
          ].includes(reason),
        ),
      ),
    );
    expect(reasons).toContain("turn-started");
    expect(reasons).toContain("occupation-required");
    for (const message of winnerMessages) {
      if (message.type !== "ActionRequired") continue;
      // Every occupation ask carries the occupation as the only thing to do, and
      // an agent seat is never asked to roll its own defence.
      if (message.reason === "occupation-required") {
        expect(message.legalMoves.map((move) => move.type)).toEqual(["occupy-territory"]);
      }
      expect(message.legalMoves.some((move) => move.type === "roll-defense")).toBe(false);
    }
  }, 60_000);

  it("never strands a consumer that crashes between reading an ask and answering it", async () => {
    const h = v2Harness(4242);
    const game = await createV2Game(h.app, {
      controllers: ["agent", "agent"],
      mapSeed: "consumer-crash",
    });
    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    const active = meta.activePlayerId as string;
    const options = {
      call: httpFor(h.app),
      gameId: game.gameId,
      playerId: active,
      token: game.tokenByPlayer[active]!,
    };

    // Crash A: the message has been read, no command has been sent. The durable
    // cursor must NOT have moved — nothing new will ever be emitted for an ask
    // that is still outstanding, so a cursor past it would wait forever.
    const reader = createBot({ ...options, state: {} });
    const ask = await reader.awaitTurn();
    expect(ask?.type).toBe("ActionRequired");
    const afterRead: BotState = structuredClone(reader.state);
    expect(afterRead.cursor).toBeUndefined();

    const resumedReader = createBot({ ...options, state: structuredClone(afterRead) });
    const replayed = await resumedReader.awaitTurn();
    expect(replayed?.messageId).toBe(ask!.messageId);
    expect(await resumedReader.step()).not.toBeNull();

    // Crash B: the command reached the server, but the response never reached
    // the client. The retained in-flight body is replayed verbatim and the
    // server dedupes it — one recorded command, and the loop keeps moving.
    const nextMeta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    const stillActive = nextMeta.activePlayerId as string;
    let lostResponses = 0;
    const losing: HttpCall = async (method, path, opts) => {
      const response = await call(h.app, method, path, opts);
      if (method === "POST" && path.includes("/commands")) {
        lostResponses += 1;
        return { status: 503, body: {} };
      }
      return response;
    };
    const crashing = createBot({
      call: losing,
      gameId: game.gameId,
      playerId: stillActive,
      token: game.tokenByPlayer[stillActive]!,
      state: {},
    });
    expect(await crashing.step()).toBeNull();
    expect(lostResponses).toBe(1);
    const midFlight: BotState = structuredClone(crashing.state);
    expect(midFlight.inflight).toBeDefined();
    const inflightCommandId = JSON.parse(midFlight.inflight!.body).commandId as string;
    expect(h.stores.commands.get(game.gameId, inflightCommandId)?.status).toBe("accepted");

    const recovered = createBot({
      call: httpFor(h.app),
      gameId: game.gameId,
      playerId: stillActive,
      token: game.tokenByPlayer[stillActive]!,
      state: structuredClone(midFlight),
    });
    expect(await recovered.step()).not.toBeNull();
    // The replay was a duplicate, not a second command: canonical history holds
    // one record under that id, and the cursor has finally moved past the ask.
    const record = h.stores.commands.get(game.gameId, inflightCommandId)!;
    expect(record.status).toBe("accepted");
    expect(recovered.state.inflight).toBeUndefined();
    expect(recovered.state.cursor).toBeDefined();

    // And the game as a whole still finishes — the crashes cost nothing but time.
    const counts = await playByMessagesOnly(h, game);
    expect(counts.commandCalls).toBeGreaterThan(0);
    expect((await call(h.app, "GET", `/v1/games/${game.gameId}`)).body.status).toBe("finished");
  }, 60_000);

  it("resumes exactly across a process restart and re-derives identical messages", async () => {
    const h = v2Harness(4242);
    const game = await createV2Game(h.app, {
      controllers: ["agent", "agent"],
      mapSeed: "actions-crash",
    });
    const meta = (await call(h.app, "GET", `/v1/games/${game.gameId}`)).body;
    const active = meta.activePlayerId as string;
    const token = game.tokenByPlayer[active]!;

    // Consume the opening ask, then lose the process holding everything but the cursor.
    const before = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token,
    });
    const cursor: string = before.body.nextOffset;
    const seenSeq = before.body.messages.at(-1).seq as number;

    const restarted = restartV2(h, 4242);
    const decision = await decisionFor(restarted.app, game, active);
    const reinforce = decision.legalMoves.find((move: any) => move.type === "reinforce");
    const submitted = await call(restarted.app, "POST", `/v1/games/${game.gameId}/commands`, {
      token,
      body: {
        commandId: "crash-resume-reinforce",
        turnId: decision.turn.id,
        action: {
          type: "reinforce",
          placements: [{ territoryId: reinforce.territoryIds[0], armies: reinforce.pool }],
        },
      },
    });
    expect(submitted.status).toBe(200);

    const after = await call(
      restarted.app,
      "GET",
      `/v1/games/${game.gameId}/players/me/actions?offset=${cursor}`,
      { token },
    );
    // No gap and no replay: the resumed page starts at the next unseen `seq`.
    expect(after.body.messages.length).toBeGreaterThan(0);
    expect(after.body.messages[0].seq).toBe(seenSeq + 1);
    expect(after.body.messages[0].since.fromEventOffset).toBe(
      before.body.messages.at(-1).eventOffset,
    );

    // Re-deriving from canonical history reproduces the stream byte for byte,
    // which is what makes the stream rebuildable rather than merely durable.
    const events = await createJsonProtocol(restarted.protocol, passthrough).getOrCreate(
      eventStreamId(game.gameId),
    );
    const history = (await events.readAll()).messages.map((message) => ({
      event: message.value as any,
      offset: message.offset,
    }));
    const rederived = deriveActions(game.gameId, history);
    for (const playerId of game.players) {
      const stored = await allMessages(restarted, game.gameId, playerId);
      expect(rederived.get(playerId) ?? []).toEqual(stored);
    }
  });
});
