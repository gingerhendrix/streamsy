/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
/**
 * `Hex Domination` scripted bot: a full game driven end-to-end through the HTTP
 * API by machine players that see nothing but `/decision`, `/commands`, and
 * their action stream.
 *
 * The interesting property is the defence interrupt: the attacker's turn stops until the
 * defender — a different bot, out of turn — rolls. These tests prove that the
 * loop still closes: the defender wakes on `DefenseAvailable`, auto-rolls with a
 * stable id, and if it never does, the canonical timeout finishes the combat
 * without it.
 */

import { describe, expect, it } from "vitest";

import { createBot, type Bot } from "../../server/demo/bot.ts";
import { defenseTimeoutCommandId } from "../../server/game/defense-timer.ts";
import {
  DEFENSE_MS,
  boardFor,
  checkedArray,
  checkedNumber,
  checkedString,
  createGame,
  decisionFor,
  declareAttack,
  gameMeta,
  httpFor,
  post,
  riskHarness,
  type Game,
  type Harness,
} from "../harness.ts";

function botsFor(h: Harness, game: Game): Map<string, Bot> {
  const http = httpFor(h.app);
  const bots = new Map<string, Bot>();
  for (const playerId of game.players) {
    bots.set(
      playerId,
      createBot({
        call: http,
        gameId: game.gameId,
        playerId,
        token: game.tokenByPlayer[playerId]!,
        state: {},
      }),
    );
  }
  return bots;
}

/**
 * Drive the game one action at a time, always re-reading canonical state first.
 * A pending defence hands control to the defender; everything else belongs to
 * the active player. This is exactly the shape a real orchestrator has, minus
 * the network waits.
 */
async function driveToCompletion(
  h: Harness,
  game: Game,
  bots: Map<string, Bot>,
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
      const defender = bots.get(pending.defenderId)!;
      // Consume the wake first, the way a real harness would.
      await defender.awaitTurn();
      const rolled = await defender.defend();
      if (!rolled) {
        // The bot declined; the canonical timeout owns it from here.
        h.clock.now = pending.defenseDeadlineAt + 1;
        await h.app.defenseTimers.fire(game.gameId, pending.attackId);
      }
      defences += 1;
      continue;
    }

    const activePlayerId = checkedString(meta.activePlayerId, "active player id");
    const active = bots.get(activePlayerId)!;
    if (!(await active.step())) {
      // No legal action and no interrupt: nothing can make progress.
      return { finished: false, steps: step, defences };
    }
  }
  return { finished: false, steps: maxSteps, defences };
}

describe("Hex Domination scripted bot", () => {
  it("plays a complete game through HTTP, resolving every defence out of turn", async () => {
    const h = riskHarness(4242);
    const game = await createGame(h.app, {
      controllers: ["bot", "bot"],
      mapSeed: "bot-game-1",
    });
    const bots = botsFor(h, game);

    const result = await driveToCompletion(h, game, bots);
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
    // A whole game is a few hundred HTTP round trips; the default 5s budget is
    // tight enough to fail on a loaded machine rather than on a real regression.
  }, 60_000);

  it("auto-rolls from a defense-required action message under a stable command id", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { controllers: ["bot", "bot"] });
    const attack = await declareAttack(h, game);
    const bots = botsFor(h, game);
    const defender = bots.get(attack.defender)!;

    const wake = await defender.awaitTurn();
    expect(wake?.type).toBe("ActionRequired");
    if (wake?.type !== "ActionRequired") throw new Error("unreachable");
    expect(wake.reason).toBe("defense-required");
    expect(wake.pendingInteraction?.attackId).toBe(attack.attackId);
    expect(
      wake.pendingInteraction?.type === "defense"
        ? wake.pendingInteraction.defenseDeadlineAt
        : undefined,
    ).toBe(h.clock.now + DEFENSE_MS);

    expect(await defender.defend()).toBe(true);
    const record = h.stores.commands.get(game.gameId, `bot-defense:${attack.attackId}`)!;
    expect(record.status).toBe("accepted");
    const resolved = record.events?.find((event) => event.type === "AttackResolved");
    if (resolved?.type !== "AttackResolved") throw new Error("expected an attack resolution");
    expect(resolved.resolutionSource).toBe("bot");

    // A duplicate wake produces no second roll and no second event.
    const replay = createBot({
      call: httpFor(h.app),
      gameId: game.gameId,
      playerId: attack.defender,
      token: game.tokenByPlayer[attack.defender]!,
      state: {},
    });
    const replayed = await replay.awaitTurn();
    expect(replayed?.messageId).toBe(wake.messageId);
    await replay.defend();
    const after = h.stores.commands.get(game.gameId, `bot-defense:${attack.attackId}`)!;
    expect(after.sourceOffset).toBe(record.sourceOffset);
  });

  it("still resolves when the defending bot is offline", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { controllers: ["bot", "bot"] });
    const attack = await declareAttack(h, game);

    // The defending bot never wakes. The canonical timeout closes the combat.
    h.clock.now += DEFENSE_MS + 1;
    const fired = await h.app.defenseTimers.fire(game.gameId, attack.attackId);
    expect(fired?.status).toBe("accepted");

    const record = h.stores.commands.get(game.gameId, defenseTimeoutCommandId(attack.attackId))!;
    const resolved = record.events?.find((event) => event.type === "AttackResolved");
    if (resolved?.type !== "AttackResolved") throw new Error("expected an attack resolution");
    expect(resolved.resolutionSource).toBe("timeout");

    // And the attacker can carry on with their turn.
    const decision = await decisionFor(h.app, game, attack.attacker);
    expect(decision.legalMoves.length).toBeGreaterThan(0);
  });

  it("keeps making territorial progress against an opponent who only turtles", async () => {
    // A human who never attacks and pours every
    // reinforcement into one fortress used to freeze the game outright. The bot
    // stacked the border facing the fortress, could not attack out of it, would
    // not fortify away from it, and the position never changed again.
    const h = riskHarness(31);
    const game = await createGame(h.app, {
      controllers: ["human", "bot"],
      mapSeed: "turtle-seed",
    });
    const turtleId = game.players[0];
    const botId = game.players[1];
    if (!turtleId || !botId) throw new Error("expected two players");
    const bot = botsFor(h, game).get(botId)!;

    const countriesOf = async (playerId: string): Promise<number> => {
      const decision = await decisionFor(h.app, game, botId);
      return decision.board.territories.filter((t: any) => t.ownerId === playerId).length;
    };
    const openingBotCountries = await countriesOf(botId);
    const openingTurtleCountries = await countriesOf(turtleId);

    /** The fortress: one country, chosen once, fed every single reinforcement. */
    let fortress: string | null = null;
    const turtleStep = async (round: number): Promise<void> => {
      const decision = await decisionFor(h.app, game, turtleId);
      const reinforce = decision.legalMoves.find((a: any) => a.type === "reinforce");
      if (reinforce) {
        // Stick to the same country while it is still held; if it ever falls,
        // turtle onto the next one rather than spreading out.
        if (!fortress || !reinforce.territoryIds.includes(fortress)) {
          fortress = checkedArray(reinforce.territoryIds, "reinforcement territory ids")
            .map((id) => checkedString(id, "reinforcement territory id"))
            .toSorted()[0]!;
        }
        const target = fortress;
        await post(h.app, game, turtleId, {
          commandId: `turtle:${decision.turn.id}:${round}`,
          turnId: decision.turn.id,
          action: {
            type: "reinforce",
            placements: [{ territoryId: target, armies: reinforce.pool }],
          },
        });
        return;
      }
      // Never attacks, never fortifies — the whole point of a turtle.
      await post(h.app, game, turtleId, {
        commandId: `turtle-end:${decision.turn.id}`,
        turnId: decision.turn.id,
        action: { type: "skip-fortifications" },
      });
    };

    const sampled: Array<{ round: number; botCountries: number }> = [];
    const maxRound = 24;
    let lastRound = 0;
    for (let step = 0; step < 3_000; step += 1) {
      const meta = await gameMeta(h.app, game);
      if (meta.status === "finished") break;
      if (meta.round > lastRound) {
        lastRound = meta.round;
        sampled.push({ round: meta.round, botCountries: await countriesOf(botId) });
        if (meta.round > maxRound) break;
      }

      const pending = meta.pendingInteraction;
      if (pending?.type === "defense") {
        if (pending.defenderId === botId) {
          await bot.defend();
        } else {
          // The turtle never rolls; the canonical timeout closes every combat.
          h.clock.now = pending.defenseDeadlineAt + 1;
          await h.app.defenseTimers.fire(game.gameId, pending.attackId);
        }
        continue;
      }

      if (meta.activePlayerId === botId) {
        expect(await bot.step()).not.toBeNull();
      } else {
        await turtleStep(meta.round);
      }
    }

    const endMeta = await gameMeta(h.app, game);
    const finalBotCountries = await countriesOf(botId);

    // The turtle can only be beaten by taking countries, so a finished game *is*
    // the anti-stalemate property: no throw, no capture, no winner.
    expect(endMeta.status).toBe("finished");
    expect(endMeta.winnerId).toBe(botId);
    expect(endMeta.round).toBeLessThanOrEqual(maxRound);
    expect(finalBotCountries).toBeGreaterThan(openingBotCountries);
    expect(await countriesOf(turtleId)).toBeLessThan(openingTurtleCountries);

    // And progress is continuous rather than an opening flurry: the bot never
    // spends several rounds in a row placing armies it cannot use.
    let frozen = 0;
    let longestFreeze = 0;
    for (const [index, entry] of sampled.entries()) {
      const previous = sampled[index - 1];
      frozen = previous && entry.botCountries <= previous.botCountries ? frozen + 1 : 0;
      longestFreeze = Math.max(longestFreeze, frozen);
    }
    expect(longestFreeze).toBeLessThanOrEqual(3);
  }, 60_000);

  it("occupies a captured country with a legal garrison chosen by the bot", async () => {
    const h = riskHarness();
    const game = await createGame(h.app, { controllers: ["bot", "bot"] });
    const bots = botsFor(h, game);

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

    const attacker = bots.get(occupation.playerId)!;
    const before = await decisionFor(h.app, game, occupation.playerId);
    const sourceArmies = before.board.territories.find(
      (t: any) => t.id === occupation.from,
    )!.armies;

    const action = await attacker.step();
    expect(action?.type).toBe("occupy-territory");
    const moved = checkedNumber(action?.armies, "occupation armies");
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
async function declareAttackOrContinue(h: Harness, game: Game, guard: number): Promise<void> {
  const meta = await gameMeta(h.app, game);
  const active = checkedString(meta.activePlayerId, "active player id");
  const decision = await decisionFor(h.app, game, active);
  const reinforce = decision.legalMoves.find((a: any) => a.type === "reinforce");
  if (reinforce) {
    const board = await boardFor(h.app, game);
    const ownerOf = (id: string) =>
      decision.board.territories.find((x: any) => x.id === id)?.ownerId;
    const border =
      board.territories.find(
        (t: any) =>
          ownerOf(t.id) === active &&
          t.adjacentTerritoryIds.some((adj: string) => ownerOf(adj) !== active),
      ) ?? board.territories[0];
    await post(h.app, game, active, {
      commandId: `pre-${guard}`,
      turnId: decision.turn.id,
      action: {
        type: "reinforce",
        placements: [{ territoryId: border.id, armies: reinforce.pool }],
      },
    });
    return;
  }
  const attack = decision.legalMoves.find((a: any) => a.type === "declare-attack");
  if (!attack) {
    await post(h.app, game, active, {
      commandId: `end-${guard}`,
      turnId: decision.turn.id,
      action: { type: "skip-fortifications" },
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
