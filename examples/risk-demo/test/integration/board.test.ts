/**
 * The `Hex Domination` board projection over HTTP.
 *
 * The kernel tests already prove the reducer agrees with the aggregate. What is
 * under test here is the *surface*: `GET /board` serves a current game on its own
 * generation, the `combat` row walks awaiting-defense → awaiting-occupation →
 * gone as the interrupt resolves, and `/decision` reports the projection's real
 * watermark rather than a canonical-head placeholder.
 */

import { describe, expect, it } from "vitest";

import { rebuildBoardGeneration } from "../../server/game/rebuild.ts";
import type { JsonValue } from "@streamsy/core";
import { createBoardMesh } from "../../src/board/mesh.ts";
import {
  BOARD_META_KEY,
  BOARD_META_TYPE,
  BOARD_REDUCER_VERSION,
} from "../../src/board/board-projection.ts";
import {
  boardFor,
  call,
  createGame,
  decisionFor,
  declareAttack,
  post,
  throwUntilCapture,
  riskHarness,
} from "../harness.ts";

describe("Hex Domination board projection surface", () => {
  it("assigns lobby colours conflict-safely instead of rejecting duplicates", async () => {
    const h = riskHarness();
    const created = await call(h.app, "POST", "/v1/games", {
      body: { name: "Alice", color: "#E05A47", mapSeed: "unique-colours" },
    });
    expect(created.status).toBe(201);
    expect(created.body.player.color).toBe("#E05A47");
    const gameId = created.body.game.id as string;

    // Both contenders ask for the same colour; the decider seats both and issues
    // the loser the first free palette colour rather than rejecting the join.
    const contenders = await Promise.all([
      call(h.app, "POST", `/v1/games/${gameId}/players`, {
        body: { name: "Bob", color: "#3B82F6" },
      }),
      call(h.app, "POST", `/v1/games/${gameId}/players`, {
        body: { name: "Cara", color: " #3b82f6 " },
      }),
    ]);
    for (const contender of contenders) expect(contender.status).toBe(201);
    expect(
      new Set(contenders.map((contender) => contender.body.player.color.trim().toLowerCase())).size,
    ).toBe(2);

    // A join that requests no colour at all is issued a free palette colour.
    const colourless = await call(h.app, "POST", `/v1/games/${gameId}/players`, {
      body: { name: "Latecomer" },
    });
    expect(colourless.status).toBe(201);

    // The canonical roster holds four players with four distinct colours, and
    // every response reported the colour its seat was actually issued.
    const lobby = await call(h.app, "GET", `/v1/games/${gameId}`);
    expect(lobby.body.players).toHaveLength(4);
    const rosterColors = new Map(
      lobby.body.players.map((player: any) => [player.id, player.color.trim().toLowerCase()]),
    );
    expect(new Set(rosterColors.values()).size).toBe(4);
    for (const seated of [...contenders, colourless]) {
      expect(rosterColors.get(seated.body.player.id)).toBe(
        seated.body.player.color.trim().toLowerCase(),
      );
    }
  });

  it("maps public agent seats to external control and keeps bots explicit", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, {
      controllers: ["agent", "bot"],
      mapSeed: "controller-boundary",
    });
    const board = await boardFor(h.app, game);

    expect(board.players.map((player: any) => player.controller)).toEqual([
      "external-agent",
      "bot",
    ]);
  });

  it("projects the map, roster totals, and current turn once the game starts", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { players: 3, mapSeed: "board-surface" });
    const board = await boardFor(h.app, game);

    expect(board.game.status).toBe("playing");
    expect(board.game.mapVersion).toBe("procedural-hex-v1");
    expect(board.hexes).toHaveLength(84);
    expect(board.territories).toHaveLength(18);
    expect(board.continents).toHaveLength(4);

    // Every hex belongs to a projected territory, and every territory is owned.
    const territoryIds = new Set(board.territories.map((t: any) => t.id));
    for (const hex of board.hexes) expect(territoryIds.has(hex.territoryId)).toBe(true);
    for (const territory of board.territories) expect(territory.ownerId).toBeTruthy();

    // Roster totals are derived, and add up to the whole board.
    const totalTerritories = board.players.reduce(
      (sum: number, p: any) => sum + p.territoryCount,
      0,
    );
    expect(totalTerritories).toBe(board.territories.length);

    expect(board.turn.turnId).toBe(`round-1:${board.game.activePlayerId}`);
    expect(board.turn.phase).toBe("reinforce");
    expect(board.turn.reinforcement.total).toBe(
      board.turn.reinforcement.base +
        board.turn.reinforcement.continents.reduce((sum: number, c: any) => sum + c.bonus, 0),
    );
    expect(board.turn.reinforcementsPlaced).toBe(0);
    expect(board.combat).toBeNull();
  });

  it("commits a distributed reinforcement turn as one atomic command", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "atomic-reinforcement" });
    const before = await boardFor(h.app, game);
    const active = before.game.activePlayerId as string;
    const decision = await decisionFor(h.app, game, active);
    const reinforce = decision.legalMoves.find((action: any) => action.type === "reinforce");
    const [first, second] = reinforce.territoryIds as [string, string];
    const firstBefore = before.territories.find((territory: any) => territory.id === first).armies;
    const secondBefore = before.territories.find(
      (territory: any) => territory.id === second,
    ).armies;

    const ack = await post(h.app, game, active, {
      commandId: "reinforce-all-at-once",
      turnId: decision.turn.id,
      action: {
        type: "reinforce",
        placements: [
          { territoryId: first, armies: reinforce.pool - 1 },
          { territoryId: second, armies: 1 },
        ],
      },
    });

    expect(ack.status).toBe(200);
    // One command, one canonical batch: the ack names only the batch's final
    // offset, so atomicity is read off the recorded events rather than the ack.
    const record = h.stores.commands.get(game.gameId, "reinforce-all-at-once")!;
    expect(record.events).toHaveLength(2);
    expect(new Set((record.events as any[]).map((event) => event.commandId))).toEqual(
      new Set(["reinforce-all-at-once"]),
    );
    const after = await boardFor(h.app, game);
    expect(after.turn.phase).toBe("attack");
    expect(after.turn.reinforcement.remaining).toBe(0);
    expect(after.territories.find((territory: any) => territory.id === first).armies).toBe(
      firstBefore + reinforce.pool - 1,
    );
    expect(after.territories.find((territory: any) => territory.id === second).armies).toBe(
      secondBefore + 1,
    );
  });

  it("walks the combat row from awaiting-defense to awaiting-occupation to cleared", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "combat-lifecycle" });
    const attack = await declareAttack(h, game, [6, 6, 6]);

    const declared = await boardFor(h.app, game);
    expect(declared.combat).toMatchObject({
      attackId: attack.attackId,
      turnId: attack.turnId,
      status: "awaiting-defense",
      attackerId: attack.attacker,
      defenderId: attack.defender,
      from: attack.from,
      to: attack.to,
    });
    expect(declared.combat.attackerRolls.every((face: number) => face === 6)).toBe(true);
    expect(declared.combat.defenseDeadlineAt).toBeGreaterThan(declared.combat.declaredAt);
    expect(declared.combat.defenderRolls).toBeUndefined();
    expect(declared.turn.attacksDeclared).toBe(1);

    // Throw (always winning) until the target falls and an occupation is owed.
    const capturing = await throwUntilCapture(h, game, attack);

    const resolved = await boardFor(h.app, game);
    expect(resolved.combat).toMatchObject({
      attackId: capturing.attackId,
      status: "awaiting-occupation",
      territoryCaptured: true,
      resolutionSource: "human",
    });
    expect(resolved.combat.minArmies).toBeGreaterThanOrEqual(1);
    expect(resolved.combat.maxArmies).toBeGreaterThanOrEqual(resolved.combat.minArmies);
    expect(resolved.turn.throwsResolved).toBe(resolved.turn.attacksDeclared);
    expect(resolved.turn.latestDice.attackId).toBe(capturing.attackId);
    // Ownership has NOT moved yet — that waits for the occupation command.
    expect(resolved.territories.find((t: any) => t.id === attack.to).ownerId).toBe(attack.defender);

    const occupied = await post(h.app, game, attack.attacker, {
      commandId: "occupy-1",
      turnId: attack.turnId,
      action: {
        type: "occupy-territory",
        attackId: capturing.attackId,
        armies: resolved.combat.minArmies,
      },
    });
    expect(occupied.status).toBe(200);

    const after = await boardFor(h.app, game);
    expect(after.combat).toBeNull();
    expect(after.turn.captures).toBe(1);
    const captured = after.territories.find((t: any) => t.id === attack.to);
    expect(captured.ownerId).toBe(attack.attacker);
    expect(captured.armies).toBe(resolved.combat.minArmies);
  });

  it("labels a timeout-resolved throw as such in the projected turn ledger", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "combat-timeout" });
    const attack = await declareAttack(h, game);

    h.clock.now += 15_001;
    await h.app.defenseTimers.fire(game.gameId, attack.attackId);

    const board = await boardFor(h.app, game);
    expect(board.turn.latestDice.resolutionSource).toBe("timeout");
    expect(board.moves.some((m: any) => m.kind === "AttackResolved")).toBe(true);
  });

  it("reports a real projection watermark that the decision is never ahead of", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "watermark" });
    const player = game.players[0]!;

    const decision = await decisionFor(h.app, game, player);
    const board = await boardFor(h.app, game);
    expect(decision.board.sourceStreamId).toBe(`games/${game.gameId}/events`);
    expect(decision.board.sourceThroughOffset).toBe(board.sourceThroughOffset);
    expect(decision.board.generation).toBe(board.generation);
    expect(decision.board.map.boardStreamId).toBe(board.boardStreamId);

    // After a command, the ack's canonical offset is already incorporated.
    const active = decision.turn.activePlayerId as string;
    const activeDecision = await decisionFor(h.app, game, active);
    const reinforce = activeDecision.legalMoves.find((a: any) => a.type === "reinforce");
    const ack = await post(h.app, game, active, {
      commandId: "wm-1",
      turnId: activeDecision.turn.id,
      action: {
        type: "reinforce",
        placements: [{ territoryId: reinforce.territoryIds[0], armies: reinforce.pool }],
      },
    });
    expect(ack.status).toBe(200);
    const synced = await decisionFor(h.app, game, active);
    expect(synced.board.sourceThroughOffset).toBe(ack.body.eventOffset);
  });

  it("rebuilds a fresh generation and cuts over after verification", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "current-rebuild" });
    await declareAttack(h, game);
    const before = await boardFor(h.app, game);
    expect(before.generation).toBe("board1");

    const result = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores, boardRuntime: h.app.boardRuntime },
      game.gameId,
      { now: () => h.clock.now + 1_000 },
    );
    expect(result.status).toBe("cutover");
    expect(result.toGeneration).toBe("board2");
    expect(result.equivalence).toEqual({ boardEqual: true, watermarkEqual: true });
    // Both generations here were built by the current reducer, so the rebuild is
    // a fresh replay rather than a migration. The fields are still reported, and
    // are what tells the two cases apart.
    expect(result.fromReducerVersion).toBe(BOARD_REDUCER_VERSION);
    expect(result.toReducerVersion).toBe(BOARD_REDUCER_VERSION);

    const after = await boardFor(h.app, game);
    expect(after.generation).toBe("board2");
    expect(after.reducerVersion).toBe(BOARD_REDUCER_VERSION);
    expect(after.sourceThroughOffset).toBe(before.sourceThroughOffset);
    expect(after.territories).toEqual(before.territories);
    expect(after.combat).toEqual(before.combat);
    expect(h.stores.generations.list(game.gameId).map((g) => g.generation)).toEqual([
      "board1",
      "board2",
    ]);
  });

  it("reports a rebuild that moves a generation onto a newer reducer", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "reducer-migration" });
    await declareAttack(h, game);

    // Stand in for a stream this deployment inherited: the rows are the current
    // reducer's, but the generation is *recorded* as an older reducer's, which is
    // the only durable trace a bump leaves behind.
    const active = h.stores.generations.get(game.gameId, "board1")!;
    h.stores.generations.put({ ...active, reducerVersion: "hex-domination:board-1" });

    const result = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores, boardRuntime: h.app.boardRuntime },
      game.gameId,
      { now: () => h.clock.now + 1_000 },
    );
    expect(result.status).toBe("cutover");
    expect(result.fromReducerVersion).toBe("hex-domination:board-1");
    expect(result.toReducerVersion).toBe(BOARD_REDUCER_VERSION);
    // The rebuilt generation is recorded under the reducer that actually built it.
    expect(h.stores.generations.get(game.gameId, "board2")?.reducerVersion).toBe(
      BOARD_REDUCER_VERSION,
    );
  });

  it("keeps the active generation and board unchanged when rebuild verification fails", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "failed-rebuild" });
    await declareAttack(h, game);
    const before = await boardFor(h.app, game);
    const activeBefore = h.stores.games.get(game.gameId)!;

    const result = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores, boardRuntime: h.app.boardRuntime },
      game.gameId,
      {
        now: () => h.clock.now + 1_000,
        // A reducer that advances lineage but never advances the board: the
        // watermark reaches the head while the rows stay wrong, which is exactly
        // the case verification exists to catch.
        makeMesh: async (options) => {
          const mesh = await createBoardMesh(options);
          return {
            ...mesh,
            reduce: (events, boundary, prior) => [
              {
                type: BOARD_META_TYPE,
                key: BOARD_META_KEY,
                value: {
                  sourceStreamId: options.sourceStreamId,
                  sourceThroughOffset: boundary.source.position,
                  // Progress is recorded honestly; only the board is wrong.
                  sourceSeq: prior.sourceSeq + events.length,
                  generation: options.generation,
                  reducerVersion: BOARD_REDUCER_VERSION,
                  // Internally consistent metadata, stale board: the watermark
                  // advances honestly while no event is ever applied.
                  snapshot: { ...prior.state, sourceThroughOffset: boundary.source.position },
                },
                headers: { operation: "upsert", offset: boundary.source.position },
              } as unknown as JsonValue,
            ],
          };
        },
      },
    );

    expect(result.status).toBe("verification-failed");
    expect(result.equivalence).toEqual({ boardEqual: false, watermarkEqual: true });
    expect(result.activeGeneration).toBe("board1");

    const activeAfter = h.stores.games.get(game.gameId)!;
    expect(activeAfter.generation).toBe(activeBefore.generation);
    expect(activeAfter.projectionStreamId).toBe(activeBefore.projectionStreamId);
    expect(h.stores.generations.get(game.gameId, "board1")?.status).toBe("active");
    expect(h.stores.generations.get(game.gameId, "board2")?.status).toBe("failed");

    const after = await boardFor(h.app, game);
    expect(after).toEqual(before);
  });
});

/**
 * Upgrading a game that was projected by an older reducer.
 *
 * `board-3` changed the *output format*: every transaction now ends at a
 * reserved mesh lineage row, and move rows are keyed by event ordinal. A
 * `board-2` generation has neither. The requirement is not that such a stream
 * keeps working — it cannot — but that it can never be silently misread as
 * current, and that the documented repair actually repairs it.
 */
describe("legacy generation upgrade", () => {
  /** Write a pre-mesh board generation: application rows, no lineage row. */
  async function writeLegacyGeneration(h: ReturnType<typeof riskHarness>, gameId: string) {
    const streamId = `games/${gameId}/projections/board/legacy1`;
    const created = await h.protocol.create(streamId, { contentType: "application/json" });
    if (created.status !== "created" && created.status !== "exists") {
      throw new Error(`cannot create legacy stream: ${created.status}`);
    }
    const stream = created.stream;
    const legacyRows: JsonValue[] = [
      {
        type: "game",
        key: gameId,
        value: { id: gameId, status: "lobby", round: 0 },
        headers: { operation: "insert", offset: "0000000000000000_0000000000000001" },
      },
      {
        type: BOARD_META_TYPE,
        key: BOARD_META_KEY,
        value: {
          sourceStreamId: `games/${gameId}/events`,
          sourceThroughOffset: "0000000000000000_0000000000000001",
          sourceSeq: 0,
          generation: "legacy1",
          // The tell: written by the previous reducer.
          reducerVersion: "hex-domination:board-2",
          snapshot: { game: { id: gameId, status: "lobby", round: 0 } },
        },
        headers: { operation: "update", offset: "0000000000000000_0000000000000001" },
      },
    ];
    // A JSON array body is framed into one message per item, which is exactly
    // how the pre-mesh runtime wrote a transition.
    const appended = await stream.append({
      data: new TextEncoder().encode(JSON.stringify(legacyRows)),
      contentType: "application/json",
    });
    if (appended.status !== "appended") throw new Error(`legacy append: ${appended.status}`);
    return streamId;
  }

  it("refuses to read a pre-mesh generation instead of serving it as current", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "legacy-refusal" });
    await writeLegacyGeneration(h, game.gameId);

    // Point the game at the legacy generation, as an in-place upgrade would.
    const row = h.stores.games.get(game.gameId)!;
    h.stores.games.put({
      ...row,
      generation: "legacy1",
      projectionStreamId: `games/${game.gameId}/projections/board/legacy1`,
    });

    const board = await call(h.app, "GET", `/v1/games/${game.gameId}/board`);
    // The read fails. What matters is that it does not return 200 with a board
    // built from rows this reducer never wrote, and does not report a causal
    // watermark for a generation it cannot actually interpret.
    expect(board.status).toBeGreaterThanOrEqual(500);
    expect(board.body?.sourceThroughOffset).toBeUndefined();
  });

  it("rebuilds from canonical history off an activated legacy generation and cuts over", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "legacy-rebuild" });
    await declareAttack(h, game);

    // Activate a genuinely pre-mesh generation: real rows, no reserved lineage,
    // recorded under the previous reducer — the state an un-upgraded game is in.
    const legacyStreamId = await writeLegacyGeneration(h, game.gameId);
    const row = h.stores.games.get(game.gameId)!;
    h.stores.games.put({ ...row, generation: "legacy1", projectionStreamId: legacyStreamId });
    h.stores.generations.put({
      gameId: game.gameId,
      generation: "legacy1",
      streamId: legacyStreamId,
      reducerVersion: "hex-domination:board-2",
      status: "active",
      sourceThroughOffset: null,
      createdAt: h.clock.now,
    });

    // While it is active the board cannot be served at all.
    const broken = await call(h.app, "GET", `/v1/games/${game.gameId}/board`);
    expect(broken.status).toBeGreaterThanOrEqual(500);

    // Rebuild replays canonical history — which the legacy stream never touched
    // — into a fresh generation, verifies it, and cuts over.
    const result = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores, boardRuntime: h.app.boardRuntime },
      game.gameId,
      { now: () => h.clock.now + 1_000 },
    );
    expect(result.status).toBe("cutover");
    expect(result.fromGeneration).toBe("legacy1");
    expect(result.fromReducerVersion).toBe("hex-domination:board-2");
    expect(result.toReducerVersion).toBe(BOARD_REDUCER_VERSION);
    expect(result.equivalence).toEqual({ boardEqual: true, watermarkEqual: true });

    // The game now serves a current, readable board on the new generation.
    const board = await boardFor(h.app, game);
    expect(board.generation).toBe(result.toGeneration);
    expect(board.reducerVersion).toBe(BOARD_REDUCER_VERSION);
    expect(board.sourceThroughOffset).toBe(result.sourceThroughOffset);
    expect(board.territories.length).toBeGreaterThan(0);
    // The decision resource agrees, so the cutover is causally coherent too.
    const decision = await decisionFor(h.app, game, board.game.activePlayerId as string);
    expect(decision.board.sourceThroughOffset).toBe(board.sourceThroughOffset);

    // The old generation is retained, still recorded as legacy, and still
    // unreadable — a cutover is reversible only in the sense that the evidence
    // survives, not that the old stream became interpretable.
    expect(result.retainedGenerations).toContain("legacy1");
    expect(h.stores.generations.get(game.gameId, "legacy1")?.reducerVersion).toBe(
      "hex-domination:board-2",
    );
    // The legacy stream itself is untouched and still has no reserved lineage
    // row, which is precisely why it stayed unreadable rather than being
    // migrated in place.
    const legacyStream = await h.protocol.get(legacyStreamId);
    if (legacyStream.status !== "ok") throw new Error("legacy stream vanished");
    const legacyRead = await legacyStream.stream.read({});
    if (legacyRead.status !== "ok") throw new Error("cannot read legacy stream");
    const legacyTypes = legacyRead.messages.map(
      (message) => (JSON.parse(new TextDecoder().decode(message.data)) as { type: string }).type,
    );
    expect(legacyTypes).not.toContain("__streamsy.mesh.lineage.v1");
    expect(legacyTypes).toContain("projectionMeta");
  });
});
