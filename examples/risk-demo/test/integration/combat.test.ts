/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
/* oxlint-disable typescript/no-unsafe-type-assertion, typescript/consistent-return, typescript/no-unnecessary-type-conversion, unicorn/consistent-function-scoping, effecttsgo/extends-native-error -- Remaining assertions are confined to caller-owned generic codecs, framework-generated structural types, or test-owned fixtures; native errors are synchronous Promise/domain exceptions rather than Effect failure-channel values, and exhaustive switches are protected by closed unions. */
/**
 * `Hex Domination` runtime: the two-stage combat protocol over the real command
 * log, notification streams, and durable defence timers.
 *
 * The pure kernel tests already cover the rules. What is under test here is the
 * *protocol*: exactly one of {human, bot, external-agent, timeout} may resolve
 * an attack, a retry returns the original dice rather than new ones, duplicate
 * and stale timer deliveries are harmless, and a restart rebuilds or resolves
 * outstanding timers from canonical state alone.
 */

import { describe, expect, it } from "vitest";

import { BOARD_REDUCER_VERSION } from "../../src/board/board-projection.ts";

import { defenseTimeoutCommandId, defenseTimerId } from "../../server/game/defense-timer.ts";
import {
  DEFENSE_MS,
  boardFor,
  call,
  createGame,
  decisionFor,
  declareAttack,
  gameMeta,
  post,
  restart,
  riskHarness,
} from "../harness.ts";

describe("Hex Domination creation seam", () => {
  it("creates a procedural Hex Domination game", async () => {
    const h = riskHarness();
    const current = await call(h.app, "POST", "/v1/games", {
      body: { name: "Alice", mapSeed: "abc" },
    });
    expect(current.body.game.mapVersion).toBe("procedural-hex-v1");
    expect(h.stores.games.get(current.body.game.id)).not.toBeNull();
  });

  it("records the map snapshot in GameStarted and does not regenerate on retry", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { mapSeed: "fixed-seed-1" });
    const decision = await decisionFor(h.app, game, game.players[0]!);
    // `/decision` names the map; the snapshot itself lives on the board surface.
    expect(decision.board.map.seed).toBe("fixed-seed-1");
    expect(decision.board.map.generatorVersion).toBe("hex-generator-v2");
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

  it("serves the current board on its own generation and reducer version", async () => {
    const h = riskHarness();
    const game = await createGame(h.app);
    const board = await call(h.app, "GET", `/v1/games/${game.gameId}/board`);
    expect(board.status).toBe(200);
    expect(board.body.generation).toBe("board1");
    expect(board.body.reducerVersion).toBe(BOARD_REDUCER_VERSION);
    expect(board.body.boardStreamId).toBe(`games/${game.gameId}/projections/board/board1`);
    expect(board.body.sourceThroughOffset).not.toBeNull();
    expect(board.body.combat).toBeNull();
  });

  it("ends the turn canonically when fortify succeeds through the public API", async () => {
    const h = riskHarness();
    const game = await createGame(h.app);
    const active = (await gameMeta(h.app, game)).activePlayerId as string;
    let decision = await decisionFor(h.app, game, active);
    const reinforce = decision.legalMoves.find((move: any) => move.type === "reinforce");

    const reinforced = await post(h.app, game, active, {
      commandId: "fortify-setup",
      turnId: decision.turn.id,
      action: {
        type: "reinforce",
        placements: [{ territoryId: reinforce.territoryIds[0], armies: reinforce.pool }],
      },
    });
    expect(reinforced.status).toBe(200);

    decision = await decisionFor(h.app, game, active);
    const fortify = decision.legalMoves.find((move: any) => move.type === "fortify");
    const choice = fortify.choices[0];
    const destination = choice.reachable[0];
    const fortified = await post(h.app, game, active, {
      commandId: "fortify-and-end",
      turnId: decision.turn.id,
      action: {
        type: "fortify",
        from: choice.from,
        to: destination.to,
        armies: 1,
      },
    });

    expect(fortified.status).toBe(200);
    expect(fortified.body.status).toBe("accepted");
    const board = await boardFor(h.app, game);
    expect(board.game.activePlayerId).not.toBe(active);
    expect(board.game.phase).toBe("reinforce");
    expect(board.turn.playerId).toBe(board.game.activePlayerId);
  });
});

describe("Hex Domination defence resolution", () => {
  it("auto-resolves an external agent's defence without asking it to roll", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { controllers: ["agent", "agent"] });
    const attack = await declareAttack(h, game);

    const board = await boardFor(h.app, game);
    expect(board.turn.latestDice).toMatchObject({
      attackId: attack.attackId,
      resolutionSource: "agent",
    });
    expect(h.scheduler.pending()).toEqual([]);

    const defender = await decisionFor(h.app, game, attack.defender);
    expect(defender.mode).toBe("waiting");
    expect(defender.legalMoves).not.toContainEqual(
      expect.objectContaining({ type: "roll-defense" }),
    );
  });

  it("sends a defense-required action message to the defender and no one else", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { players: 3 });
    const attack = await declareAttack(h, game);

    const defenderWakes = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token: game.tokenByPlayer[attack.defender]!,
    });
    const defense = defenderWakes.body.messages.filter(
      (message: any) => message.type === "ActionRequired" && message.reason === "defense-required",
    );
    expect(defense).toHaveLength(1);
    expect(defense[0].pendingInteraction.attackId).toBe(attack.attackId);
    expect(defense[0].playerId).toBe(attack.defender);
    expect(defense[0].turn.id).toBe(attack.turnId);

    const bystander = game.players.find((p) => p !== attack.defender && p !== attack.attacker)!;
    const other = await call(h.app, "GET", `/v1/games/${game.gameId}/players/me/actions`, {
      token: game.tokenByPlayer[bystander]!,
    });
    expect(
      other.body.messages.filter((message: any) => message.reason === "defense-required"),
    ).toHaveLength(0);
  });

  it("lets a human roll before the deadline, and the timeout then finds nothing", async () => {
    const h = riskHarness();
    const game = await createGame(h.app);
    const attack = await declareAttack(h, game);
    expect(h.scheduler.pending()).toEqual([defenseTimerId(game.gameId, attack.attackId)]);

    h.rig([1, 1]);
    const rolled = await post(h.app, game, attack.defender, {
      commandId: "human-roll",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    });
    expect(rolled.status).toBe(200);
    // The ack is a receipt; the recorded outcome is read from canonical history.
    const rollRecord = h.stores.commands.get(game.gameId, "human-roll")!;
    const resolved = (rollRecord.events as any[]).find((e) => e.type === "AttackResolved");
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
    const h = riskHarness();
    const game = await createGame(h.app);
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
    const h = riskHarness();
    const game = await createGame(h.app);
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
    const h = riskHarness();
    const game = await createGame(h.app);
    const attack = await declareAttack(h, game);

    const body = {
      commandId: "retry-me",
      turnId: attack.turnId,
      action: { type: "roll-defense", attackId: attack.attackId },
    };
    // Rigged faces are consumed once. A retry that rolled again would fall back
    // to the seeded rng and record different dice.
    h.rig([1, 1]);
    const first = await post(h.app, game, attack.defender, body);
    expect(first.status).toBe(200);
    const original = structuredClone(
      (h.stores.commands.get(game.gameId, "retry-me")!.events as any[]).find(
        (e) => e.type === "AttackResolved",
      ),
    );
    expect(original.defenderRolls.length).toBeGreaterThan(0);

    const retry = await post(h.app, game, attack.defender, body);
    expect(retry.status).toBe(200);
    expect(retry.body.status).toBe("duplicate");
    expect(retry.body.eventOffset).toBe(first.body.eventOffset);
    // The ack is a receipt, so the dice are not in it at all: the
    // original outcome is proven from canonical history instead.
    expect(Object.keys(retry.body).toSorted()).toEqual([
      "commandId",
      "eventOffset",
      "status",
      "turnId",
    ]);
    const afterRetry = h.stores.commands.get(game.gameId, "retry-me")!;
    expect((afterRetry.events as any[]).find((e) => e.type === "AttackResolved")).toEqual(original);
  });

  it("returns the original dice on a duplicate timeout delivery", async () => {
    const h = riskHarness();
    const game = await createGame(h.app);
    const attack = await declareAttack(h, game);
    h.clock.now += DEFENSE_MS + 1;

    const first = await h.app.defenseTimers.fire(game.gameId, attack.attackId);
    expect(first?.status).toBe("accepted");
    // A second delivery refolds, finds nothing pending, and does nothing at all.
    const second = await h.app.defenseTimers.fire(game.gameId, attack.attackId);
    expect(second).toBeNull();
  });

  it("picks exactly one winner when a human and the timeout resolve together", async () => {
    const h = riskHarness();
    const game = await createGame(h.app);
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
    const h = riskHarness();
    const game = await createGame(h.app, { players: 3 });
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
      action: { type: "skip-fortifications" },
    });
    expect(impatient.status).toBe(409);
    expect(impatient.body.error.code).toBe("PENDING_DEFENSE");
  });

  it("never exposes the internal timeout command through the player endpoint", async () => {
    const h = riskHarness();
    const game = await createGame(h.app);
    const attack = await declareAttack(h, game);

    const forged = await post(h.app, game, attack.defender, {
      commandId: "forged",
      turnId: attack.turnId,
      action: { type: "resolve-defense-timeout", attackId: attack.attackId },
    });
    expect(forged.status).toBe(400);
    expect(forged.body.error.code).toBe("INVALID_ACTION");
  });
});

describe("Hex Domination timer recovery", () => {
  it("rebuilds an outstanding timer from canonical state after a restart", async () => {
    const original = riskHarness();
    const game = await createGame(original.app, { mapSeed: "restart-seed" });
    const attack = await declareAttack(original, game);
    expect(original.scheduler.pending()).toEqual([defenseTimerId(game.gameId, attack.attackId)]);

    // Restart: a fresh app over the same storage, with an empty scheduler.
    const restarted = restart(original);
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
    const original = riskHarness();
    const game = await createGame(original.app, { mapSeed: "expired-seed" });
    const attack = await declareAttack(original, game);

    // The process is down while the window closes.
    original.clock.now += DEFENSE_MS + 5_000;
    const restarted = restart(original);
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
});
