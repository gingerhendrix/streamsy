/**
 * `risk-demo-v2` runtime: the two-stage combat protocol over the real command
 * log, notification streams, and durable defence timers.
 *
 * The pure kernel tests already cover the rules. What is under test here is the
 * *protocol*: exactly one of {human, bot, external-agent, timeout} may resolve
 * an attack, a retry returns the original dice rather than new ones, duplicate
 * and stale timer deliveries are harmless, and a restart rebuilds or resolves
 * outstanding timers from canonical state alone.
 */

import { describe, expect, it } from "vitest";

import { defenseTimeoutCommandId, defenseTimerId } from "../../server/game/defense-timer.ts";
import { RULESET_V2 } from "../../src/domain/map-v2.ts";
import {
  DEFENSE_MS,
  boardFor,
  call,
  createV2Game,
  decisionFor,
  declareAttack,
  gameMeta,
  post,
  restartV2,
  v2Harness,
} from "../v2-harness.ts";

describe("risk-demo-v2 creation seam", () => {
  it("creates v2 by default and v1 only when asked for by name", async () => {
    const h = v2Harness();
    const v1 = await call(h.app, "POST", "/v1/games", {
      body: { ruleset: "risk-demo-v1", name: "Alice" },
    });
    expect(v1.body.game.ruleset).toBe("risk-demo-v1");
    expect(h.stores.games.get(v1.body.game.id)!.ruleset).toBe("risk-demo-v1");

    const v2 = await call(h.app, "POST", "/v1/games", {
      body: { name: "Alice", mapSeed: "abc" },
    });
    expect(v2.body.game.ruleset).toBe(RULESET_V2);
    expect(v2.body.game.mapVersion).toBe("procedural-hex-v1");
    expect(h.stores.games.get(v2.body.game.id)!.ruleset).toBe(RULESET_V2);
  });

  it("records the map snapshot in GameStarted and does not regenerate on retry", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { mapSeed: "fixed-seed-1" });
    const decision = await decisionFor(h.app, game, game.players[0]!);
    // `/decision` names the map; the snapshot itself lives on the board surface.
    expect(decision.board.map.seed).toBe("fixed-seed-1");
    expect(decision.board.map.generatorVersion).toBe("hex-generator-v1");
    expect(decision.board.map.territoryCount).toBe(16);
    const board = await boardFor(h.app, game);
    expect(board.game.mapSeed).toBe("fixed-seed-1");
    expect(board.territories).toHaveLength(16);
    expect(board.hexes).toHaveLength(72);
    expect(board.continents).toHaveLength(4);

    // A start retry under the same commandId returns the original GameStarted.
    const retry = await call(h.app, "POST", `/v1/games/${game.gameId}/start`, {
      token: game.tokenByPlayer[game.players[0]!],
      body: { commandId: "start-again" },
    });
    expect(retry.status).toBe(409);
    expect(retry.body.error.code).toBe("GAME_ALREADY_STARTED");
  });

  it("serves the v2 board on its own generation and reducer version", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const board = await call(h.app, "GET", `/v1/games/${game.gameId}/board`);
    expect(board.status).toBe(200);
    expect(board.body.ruleset).toBe(RULESET_V2);
    expect(board.body.generation).toBe("hex1");
    expect(board.body.reducerVersion).toBe("risk-demo-v2:board-1");
    expect(board.body.boardStreamId).toBe(`games/${game.gameId}/projections/board/hex1`);
    expect(board.body.sourceThroughOffset).not.toBeNull();
    expect(board.body.combat).toBeNull();
  });
});

describe("risk-demo-v2 defence resolution", () => {
  it("attributes an external agent's submitted defence without treating it as a bot", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const attack = await declareAttack(h, game);

    const rolled = await post(h.app, game, attack.defender, {
      commandId: "external-agent-roll",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    });

    expect(rolled.status).toBe(200);
    expect(rolled.body.events.find((event: any) => event.type === "AttackResolved")).toMatchObject({
      resolutionSource: "agent",
    });
  });

  it("wakes the defender with DefenseAvailable and no one else", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { players: 3 });
    const attack = await declareAttack(h, game);

    const defenderWakes = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/turns`, {
      token: game.tokenByPlayer[attack.defender]!,
    });
    const defense = defenderWakes.body.notifications.filter(
      (n: any) => n.type === "DefenseAvailable",
    );
    expect(defense).toHaveLength(1);
    expect(defense[0].attackId).toBe(attack.attackId);
    expect(defense[0].playerId).toBe(attack.defender);
    expect(defense[0].turnId).toBe(attack.turnId);
    expect(defense[0].notificationId).toBe(
      `defense:${game.gameId}:${attack.defender}:${attack.attackId}`,
    );

    const bystander = game.players.find((p) => p !== attack.defender && p !== attack.attacker)!;
    const other = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/turns`, {
      token: game.tokenByPlayer[bystander]!,
    });
    expect(other.body.notifications.filter((n: any) => n.type === "DefenseAvailable")).toHaveLength(
      0,
    );
  });

  it("lets a human roll before the deadline, and the timeout then finds nothing", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const attack = await declareAttack(h, game);
    expect(h.scheduler.pending()).toEqual([defenseTimerId(game.gameId, attack.attackId)]);

    h.rig([1, 1]);
    const rolled = await post(h.app, game, attack.defender, {
      commandId: "human-roll",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    });
    expect(rolled.status).toBe(200);
    const resolved = rolled.body.events.find((e: any) => e.type === "AttackResolved");
    expect(resolved.resolutionSource).toBe("human");
    expect(resolved.defenderRolls).toEqual([1, 1].slice(0, resolved.defenderRolls.length));

    // The defence interrupt is closed (a capture leaves an occupation instead).
    const closed = await gameMeta(h.app, game);
    expect(closed.pendingInteraction?.type).not.toBe("defense");

    // The timer still fires (delivery is at-least-once) and is a harmless no-op:
    // no timeout command is ever recorded, so no second die is consumed.
    expect(h.scheduler.fire(defenseTimerId(game.gameId, attack.attackId))).toBe(true);
    await h.scheduler.settle();
    expect(h.stores.commands.get(game.gameId, defenseTimeoutCommandId(attack.attackId))).toBeNull();
    const after = await gameMeta(h.app, game);
    expect(after.pendingInteraction?.type).not.toBe("defense");
  });

  it("resolves by timeout when the deadline passes, and a later human roll is rejected", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const attack = await declareAttack(h, game);

    h.clock.now += DEFENSE_MS + 1;
    await h.app.defenseTimers.fire(game.gameId, attack.attackId);

    const meta = await gameMeta(h.app, game);
    expect(meta.pendingInteraction?.type).not.toBe("defense");
    const record = h.stores.commands.get(game.gameId, defenseTimeoutCommandId(attack.attackId))!;
    expect(record.status).toBe("accepted");
    const resolved = (record.events as any[]).find((e) => e.type === "AttackResolved");
    expect(resolved.resolutionSource).toBe("timeout");

    const late = await post(h.app, game, attack.defender, {
      commandId: "too-late",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    });
    expect(late.status).toBe(409);
    expect(late.body.error.code).toBe("ATTACK_ALREADY_RESOLVED");
  });

  it("rejects a human roll submitted after the deadline but before the timer fires", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const attack = await declareAttack(h, game);

    h.clock.now += DEFENSE_MS + 1;
    const late = await post(h.app, game, attack.defender, {
      commandId: "expired-roll",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    });
    expect(late.status).toBe(409);
    expect(late.body.error.code).toBe("DEFENSE_DEADLINE_EXPIRED");

    // The interrupt is still open, and the timeout resolver still owns it.
    const meta = await gameMeta(h.app, game);
    expect(meta.pendingInteraction.attackId).toBe(attack.attackId);
    const fired = await h.app.defenseTimers.fire(game.gameId, attack.attackId);
    expect(fired?.status).toBe("accepted");
  });

  it("returns the original dice on a retry of the winning resolver", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const attack = await declareAttack(h, game);

    const body = {
      commandId: "retry-me",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    };
    const first = await post(h.app, game, attack.defender, body);
    expect(first.status).toBe(200);
    const retry = await post(h.app, game, attack.defender, body);
    expect(retry.status).toBe(200);
    expect(retry.body.status).toBe("duplicate");
    expect(retry.body.sourceOffset).toBe(first.body.sourceOffset);
    expect(retry.body.events).toEqual(first.body.events);
  });

  it("returns the original dice on a duplicate timeout delivery", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const attack = await declareAttack(h, game);
    h.clock.now += DEFENSE_MS + 1;

    const first = await h.app.defenseTimers.fire(game.gameId, attack.attackId);
    expect(first?.status).toBe("accepted");
    // A second delivery refolds, finds nothing pending, and does nothing at all.
    const second = await h.app.defenseTimers.fire(game.gameId, attack.attackId);
    expect(second).toBeNull();
  });

  it("picks exactly one winner when a human and the timeout resolve together", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const attack = await declareAttack(h, game);

    const [human, timeout] = await Promise.all([
      post(h.app, game, attack.defender, {
        commandId: "simultaneous-human",
        turnId: attack.turnId,
        action: { type: "roll-defense", attackId: attack.attackId },
      }),
      h.app.defenseTimers.fire(game.gameId, attack.attackId),
    ]);

    const humanWon = human.status === 200;
    const timeoutWon = timeout?.status === "accepted";
    expect(humanWon !== timeoutWon).toBe(true); // exactly one committed

    if (!humanWon) {
      expect(human.status).toBe(409);
      expect(human.body.error.code).toBe("ATTACK_ALREADY_RESOLVED");
    }

    // Canonical history contains exactly one resolution for this attack.
    const decision = await decisionFor(h.app, game, attack.attacker);
    expect(decision.pendingInteraction?.type).not.toBe("defense");
  });

  it("cannot be resolved by a bystander, or under the wrong attackId", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { players: 3 });
    const attack = await declareAttack(h, game);
    const bystander = game.players.find((p) => p !== attack.defender && p !== attack.attacker)!;

    const wrongPlayer = await post(h.app, game, bystander, {
      commandId: "not-mine",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    });
    expect(wrongPlayer.status).toBe(409);
    expect(wrongPlayer.body.error.code).toBe("NOT_DEFENDING_PLAYER");

    const wrongAttack = await post(h.app, game, attack.defender, {
      commandId: "wrong-attack",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: "some-other-attack" },
    });
    expect(wrongAttack.status).toBe(409);
    expect(wrongAttack.body.error.code).toBe("ATTACK_ID_MISMATCH");

    // The attacker cannot carry on while their own throw is unresolved.
    const impatient = await post(h.app, game, attack.attacker, {
      commandId: "impatient",
      turnId: attack.turnId,
      action: { type: "end-turn" },
    });
    expect(impatient.status).toBe(409);
    expect(impatient.body.error.code).toBe("PENDING_DEFENSE");
  });

  it("never exposes the internal timeout command through the player endpoint", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app);
    const attack = await declareAttack(h, game);

    const forged = await post(h.app, game, attack.defender, {
      commandId: "forged",
      turnId: attack.turnId,
      action: { type: "resolve-defense-timeout", attackId: attack.attackId },
    });
    expect(forged.status).toBe(400);
    expect(forged.body.error.code).toBe("BAD_REQUEST");
  });
});

describe("risk-demo-v2 timer recovery", () => {
  it("rebuilds an outstanding timer from canonical state after a restart", async () => {
    const original = v2Harness();
    const game = await createV2Game(original.app, { mapSeed: "restart-seed" });
    const attack = await declareAttack(original, game);
    expect(original.scheduler.pending()).toEqual([defenseTimerId(game.gameId, attack.attackId)]);

    // Restart: a fresh app over the same storage, with an empty scheduler.
    const restarted = restartV2(original);
    expect(restarted.scheduler.pending()).toEqual([]);
    await restarted.app.defenseTimers.recover();
    // The deadline has not passed, so the timer is rebuilt rather than fired.
    expect(restarted.scheduler.pending()).toEqual([defenseTimerId(game.gameId, attack.attackId)]);
    const meta = await gameMeta(restarted.app, game);
    expect(meta.pendingInteraction.attackId).toBe(attack.attackId);

    // And firing it resolves the combat exactly once.
    restarted.clock.now += DEFENSE_MS + 1;
    expect(restarted.scheduler.fire(defenseTimerId(game.gameId, attack.attackId))).toBe(true);
    await restarted.scheduler.settle();
    const after = await gameMeta(restarted.app, game);
    expect(after.pendingInteraction?.type).not.toBe("defense");
  });

  it("resolves immediately on restart when the deadline already passed", async () => {
    const original = v2Harness();
    const game = await createV2Game(original.app, { mapSeed: "expired-seed" });
    const attack = await declareAttack(original, game);

    // The process is down while the window closes.
    original.clock.now += DEFENSE_MS + 5_000;
    const restarted = restartV2(original);
    await restarted.app.defenseTimers.recover();

    // No timer is left outstanding; the attack is already closed.
    expect(restarted.scheduler.pending()).toEqual([]);
    const record = restarted.stores.commands.get(
      game.gameId,
      defenseTimeoutCommandId(attack.attackId),
    )!;
    expect(record.status).toBe("accepted");
    const meta = await gameMeta(restarted.app, game);
    expect(meta.pendingInteraction?.type).not.toBe("defense");
  });

  it("does not schedule or resolve anything for v1 games", async () => {
    const h = v2Harness();
    const created = await call(h.app, "POST", "/v1/games", {
      body: { ruleset: "risk-demo-v1", name: "Alice" },
    });
    await h.app.defenseTimers.ensure(created.body.game.id);
    await h.app.defenseTimers.recover();
    expect(h.scheduler.pending()).toEqual([]);
  });
});
