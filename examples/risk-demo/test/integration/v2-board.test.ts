/**
 * The `risk-demo-v2` board projection over HTTP.
 *
 * The kernel tests already prove the reducer agrees with the aggregate. What is
 * under test here is the *surface*: `GET /board` serves a v2 game on its own
 * generation, the `combat` row walks awaiting-defense → awaiting-occupation →
 * gone as the interrupt resolves, and `/decision` reports the projection's real
 * watermark rather than a canonical-head placeholder.
 */

import { describe, expect, it } from "vitest";

import { rebuildBoardGeneration } from "../../server/game/rebuild.ts";
import { RULESET_V2 } from "../../src/domain/map-v2.ts";
import { rendererForGame } from "../../src/ui/shared.tsx";
import {
  boardFor,
  call,
  createV2Game,
  decisionFor,
  declareAttack,
  post,
  throwUntilCapture,
  v2Harness,
} from "../v2-harness.ts";

describe("risk-demo-v2 board projection surface", () => {
  it("maps public agent seats to external control and keeps bots explicit", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, {
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
    const h = v2Harness();
    const game = await createV2Game(h.app, { players: 3, mapSeed: "board-surface" });
    const board = await boardFor(h.app, game);

    expect(board.game.status).toBe("playing");
    expect(board.game.ruleset).toBe(RULESET_V2);
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

  it("walks the combat row from awaiting-defense to awaiting-occupation to cleared", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { mapSeed: "combat-lifecycle" });
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
    const h = v2Harness();
    const game = await createV2Game(h.app, { mapSeed: "combat-timeout" });
    const attack = await declareAttack(h, game);

    h.clock.now += 15_001;
    await h.app.defenseTimers.fire(game.gameId, attack.attackId);

    const board = await boardFor(h.app, game);
    expect(board.turn.latestDice.resolutionSource).toBe("timeout");
    expect(board.moves.some((m: any) => m.kind === "AttackResolved")).toBe(true);
  });

  it("reports a real projection watermark that the decision is never ahead of", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { mapSeed: "watermark" });
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
    const reinforce = activeDecision.legalActions.find((a: any) => a.type === "reinforce");
    const ack = await post(h.app, game, active, {
      commandId: "wm-1",
      turnId: activeDecision.turn.id,
      action: {
        type: "reinforce",
        territoryId: reinforce.territoryIds[0],
        armies: reinforce.maxArmies,
      },
    });
    expect(ack.status).toBe(200);
    const synced = await decisionFor(h.app, game, active);
    expect(synced.board.sourceThroughOffset).toBe(ack.body.sourceOffset);
  });

  it("rebuilds the v2 generation and cuts over without touching v1 lineage", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { mapSeed: "v2-rebuild" });
    await declareAttack(h, game);
    const before = await boardFor(h.app, game);
    expect(before.generation).toBe("hex1");

    const result = await rebuildBoardGeneration(
      { protocol: h.protocol, stores: h.stores },
      game.gameId,
      { now: () => h.clock.now + 1_000 },
    );
    expect(result.status).toBe("cutover");
    expect(result.toGeneration).toBe("hex2");
    expect(result.equivalence).toEqual({ boardEqual: true, watermarkEqual: true });

    const after = await boardFor(h.app, game);
    expect(after.generation).toBe("hex2");
    expect(after.reducerVersion).toBe("risk-demo-v2:board-1");
    expect(after.sourceThroughOffset).toBe(before.sourceThroughOffset);
    expect(after.territories).toEqual(before.territories);
    expect(after.combat).toEqual(before.combat);
    expect(h.stores.generations.list(game.gameId).map((g) => g.generation)).toEqual([
      "hex1",
      "hex2",
    ]);
  });

  it("keeps a v1 game on the v1 generation and reducer", async () => {
    const h = v2Harness();
    const created = await call(h.app, "POST", "/v1/games", {
      body: { ruleset: "risk-demo-v1", name: "Alice" },
    });
    const board = await call(h.app, "GET", `/v1/games/${created.body.game.id}/board`);
    expect(board.status).toBe(200);
    expect(board.body.ruleset).toBe("risk-demo-v1");
    expect(board.body.generation).toBe("v1");
    expect(board.body).not.toHaveProperty("hexes");
  });

  it("routes each game to its own renderer from the canonical ruleset alone", async () => {
    // Slice 7 acceptance: a v1 game stays viewable and playable on the v1 board
    // after v2 became the default. The choice comes from `ruleset`, never from
    // which rows a projection happens to be missing (design spec §11).
    const h = v2Harness();
    const v1 = await call(h.app, "POST", "/v1/games", {
      body: { ruleset: "risk-demo-v1", name: "Alice" },
    });
    const v2 = await call(h.app, "POST", "/v1/games", { body: { name: "Alice" } });

    expect(rendererForGame(v1.body.game)).toBe("risk-demo-v1");
    expect(rendererForGame(v2.body.game)).toBe("risk-demo-v2");
    // Nothing is chosen before the game resource arrives, so neither board stream
    // is opened speculatively.
    expect(rendererForGame(null)).toBeNull();

    // And the v1 game still accepts v1 play through the v1 kernel.
    const joined = await call(h.app, "POST", `/v1/games/${v1.body.game.id}/players`, {
      body: { name: "Bob", color: "blue" },
    });
    expect(joined.status).toBe(201);
    const started = await call(h.app, "POST", `/v1/games/${v1.body.game.id}/start`, {
      token: v1.body.capability,
      body: {},
    });
    expect(started.status).toBe(200);
    const playing = await call(h.app, "GET", `/v1/games/${v1.body.game.id}`);
    expect(playing.body.status).toBe("playing");
    expect(playing.body.ruleset).toBe("risk-demo-v1");
  });
});
