/**
 * `risk-demo-v2` agent harness: a full game driven end-to-end through the HTTP
 * API by machine players that see nothing but `/decision`, `/commands`, and
 * their action stream.
 *
 * The interesting property is the defence interrupt. In v1 a turn is a single
 * actor's uninterrupted sequence; in v2 the attacker's turn *stops* until the
 * defender — a different agent, out of turn — rolls. These tests prove that the
 * loop still closes: the defender wakes on `DefenseAvailable`, auto-rolls with a
 * stable id, and if it never does, the canonical timeout finishes the combat
 * without it.
 */

import { describe, expect, it } from "vitest";

import { createAgent, type Agent } from "../../server/demo/agent.ts";
import { defenseTimeoutCommandId } from "../../server/game/defense-timer.ts";
import {
  DEFENSE_MS,
  createV2Game,
  decisionFor,
  declareAttack,
  gameMeta,
  httpFor,
  post,
  v2Harness,
  type V2Game,
  type V2Harness,
} from "../v2-harness.ts";

function agentsFor(h: V2Harness, game: V2Game): Record<string, Agent> {
  const http = httpFor(h.app);
  const agents: Record<string, Agent> = {};
  for (const playerId of game.players) {
    agents[playerId] = createAgent({
      call: http,
      gameId: game.gameId,
      playerId,
      token: game.tokenByPlayer[playerId]!,
      state: {},
    });
  }
  return agents;
}

/**
 * Drive the game one action at a time, always re-reading canonical state first.
 * A pending defence hands control to the defender; everything else belongs to
 * the active player. This is exactly the shape a real orchestrator has, minus
 * the network waits.
 */
async function driveToCompletion(
  h: V2Harness,
  game: V2Game,
  agents: Record<string, Agent>,
  options: { maxSteps?: number; onStep?: (meta: any) => Promise<void> | void } = {},
): Promise<{ finished: boolean; steps: number; defences: number }> {
  let defences = 0;
  const maxSteps = options.maxSteps ?? 4000;

  for (let step = 0; step < maxSteps; step += 1) {
    const meta = await gameMeta(h.app, game);
    if (meta.status === "finished") return { finished: true, steps: step, defences };
    await options.onStep?.(meta);

    const pending = meta.pendingInteraction;
    if (pending?.type === "defense") {
      const defender = agents[pending.defenderId]!;
      // Consume the wake first, the way a real harness would.
      await defender.awaitTurn();
      const rolled = await defender.defend();
      if (!rolled) {
        // The agent declined; the canonical timeout owns it from here.
        h.clock.now = pending.defenseDeadlineAt + 1;
        await h.app.defenseTimers.fire(game.gameId, pending.attackId);
      }
      defences += 1;
      continue;
    }

    const active = agents[meta.activePlayerId as string]!;
    if (!(await active.step())) {
      // No legal action and no interrupt: nothing can make progress.
      return { finished: false, steps: step, defences };
    }
  }
  return { finished: false, steps: maxSteps, defences };
}

describe("risk-demo-v2 agent harness", () => {
  it("plays a complete game through HTTP, resolving every defence out of turn", async () => {
    const h = v2Harness(4242);
    const game = await createV2Game(h.app, {
      controllers: ["agent", "agent"],
      mapSeed: "agent-game-1",
    });
    const agents = agentsFor(h, game);

    const result = await driveToCompletion(h, game, agents);
    expect(result.finished).toBe(true);
    expect(result.defences).toBeGreaterThan(0);

    const meta = await gameMeta(h.app, game);
    expect(meta.status).toBe("finished");
    expect(meta.winnerId).toBeDefined();
    expect(meta.pendingInteraction).toBeUndefined();

    // The winner holds every country, and every other seat is eliminated.
    const decision = await decisionFor(h.app, game, meta.winnerId);
    for (const territory of decision.board.territories) {
      expect(territory.ownerId).toBe(meta.winnerId);
    }
    for (const player of meta.players) {
      expect(player.eliminated).toBe(player.id !== meta.winnerId);
    }
  });

  it("auto-rolls from a DefenseAvailable wake under a stable command id", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const attack = await declareAttack(h, game);
    const agents = agentsFor(h, game);
    const defender = agents[attack.defender]!;

    const wake = await defender.awaitTurn();
    expect(wake?.type).toBe("DefenseAvailable");
    if (wake?.type !== "DefenseAvailable") throw new Error("unreachable");
    expect(wake.attackId).toBe(attack.attackId);
    expect(wake.deadlineAt).toBe(h.clock.now + DEFENSE_MS);

    expect(await defender.defend()).toBe(true);
    const record = h.stores.commands.get(game.gameId, `agent-defense:${attack.attackId}`)!;
    expect(record.status).toBe("accepted");
    const resolved = (record.events as any[]).find((e) => e.type === "AttackResolved");
    expect(resolved.resolutionSource).toBe("agent-auto");

    // A duplicate wake produces no second roll and no second event.
    const replay = createAgent({
      call: httpFor(h.app),
      gameId: game.gameId,
      playerId: attack.defender,
      token: game.tokenByPlayer[attack.defender]!,
      state: {},
    });
    const replayed = await replay.awaitTurn();
    expect(replayed?.notificationId).toBe(wake.notificationId);
    await replay.defend();
    const after = h.stores.commands.get(game.gameId, `agent-defense:${attack.attackId}`)!;
    expect(after.sourceOffset).toBe(record.sourceOffset);
  });

  it("still resolves when the defending agent is offline", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const attack = await declareAttack(h, game);

    // The defending agent never wakes. The canonical timeout closes the combat.
    h.clock.now += DEFENSE_MS + 1;
    const fired = await h.app.defenseTimers.fire(game.gameId, attack.attackId);
    expect(fired?.status).toBe("accepted");

    const record = h.stores.commands.get(game.gameId, defenseTimeoutCommandId(attack.attackId))!;
    const resolved = (record.events as any[]).find((e) => e.type === "AttackResolved");
    expect(resolved.resolutionSource).toBe("timeout");

    // And the attacker can carry on with their turn.
    const decision = await decisionFor(h.app, game, attack.attacker);
    expect(decision.legalActions.length).toBeGreaterThan(0);
  });

  it("occupies a captured country with a legal garrison chosen by the agent", async () => {
    const h = v2Harness();
    const game = await createV2Game(h.app, { controllers: ["agent", "agent"] });
    const agents = agentsFor(h, game);

    // Throw until something falls, keeping the attacker sweeping every pair.
    let occupation: any;
    for (let guard = 0; guard < 12 && !occupation; guard += 1) {
      const meta = await gameMeta(h.app, game);
      const pending = meta.pendingInteraction;
      if (pending?.type === "occupation") {
        occupation = pending;
        break;
      }
      if (pending?.type === "defense") {
        h.rig([1, 1]);
        await post(h.app, game, pending.defenderId, {
          commandId: `roll-${guard}`,
          turnId: pending.turnId,
          action: { type: "roll-defense", attackId: pending.attackId },
        });
        continue;
      }
      h.rig([6, 6, 6]);
      await declareAttackOrContinue(h, game, guard);
    }
    expect(occupation).toBeDefined();

    const attacker = agents[occupation.playerId]!;
    const before = await decisionFor(h.app, game, occupation.playerId);
    const sourceArmies = before.board.territories.find(
      (t: any) => t.id === occupation.from,
    )!.armies;

    const action = await attacker.step();
    expect(action?.type).toBe("occupy-territory");
    const moved = action!.armies as number;
    expect(moved).toBeGreaterThanOrEqual(occupation.minArmies);
    expect(moved).toBeLessThanOrEqual(occupation.maxArmies);

    const after = await decisionFor(h.app, game, occupation.playerId);
    const captured = after.board.territories.find((t: any) => t.id === occupation.to)!;
    expect(captured.ownerId).toBe(occupation.playerId);
    expect(captured.armies).toBe(moved);
    expect(after.board.territories.find((t: any) => t.id === occupation.from)!.armies).toBe(
      sourceArmies - moved,
    );
  });
});

/** Declare a fresh attack if the board allows one; otherwise advance the turn. */
async function declareAttackOrContinue(h: V2Harness, game: V2Game, guard: number): Promise<void> {
  const meta = await gameMeta(h.app, game);
  const active = meta.activePlayerId as string;
  const decision = await decisionFor(h.app, game, active);
  const reinforce = decision.legalActions.find((a: any) => a.type === "reinforce");
  if (reinforce) {
    const ownerOf = (id: string) =>
      decision.board.territories.find((x: any) => x.id === id)?.ownerId;
    const border =
      decision.board.map.territories.find(
        (t: any) =>
          ownerOf(t.id) === active &&
          t.adjacentTerritoryIds.some((adj: string) => ownerOf(adj) !== active),
      ) ?? decision.board.map.territories[0];
    await post(h.app, game, active, {
      commandId: `pre-${guard}`,
      turnId: decision.turn.id,
      action: { type: "reinforce", territoryId: border.id, armies: reinforce.maxArmies },
    });
    return;
  }
  const attack = decision.legalActions.find((a: any) => a.type === "declare-attack");
  if (!attack) {
    await post(h.app, game, active, {
      commandId: `end-${guard}`,
      turnId: decision.turn.id,
      action: { type: "end-turn" },
    });
    return;
  }
  const choice = attack.choices[0];
  await post(h.app, game, active, {
    commandId: `atk-${guard}`,
    turnId: decision.turn.id,
    action: {
      type: "declare-attack",
      from: choice.from,
      to: choice.to,
      attackerDice: choice.maxAttackerDice,
    },
  });
}
