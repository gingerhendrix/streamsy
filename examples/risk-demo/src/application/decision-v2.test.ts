/**
 * The player-relative v2 decision resource (design spec §6.1).
 *
 * Two properties matter here beyond "the right actions come back": the resource
 * is *player-relative*, so an out-of-turn defender is the only one with a move
 * while combat is pending; and it is *slim*, so the static map is a reference to
 * the board surface rather than a snapshot repeated on every fetch.
 */

import { describe, expect, it } from "vitest";

import { buildDecisionContextV2, type BoardWatermarkV2 } from "./decision-v2.ts";
import {
  armForAttack,
  declareAttackV2,
  occupyPending,
  startGameV2,
  throwUntilCapture,
  winThrow,
  type ScriptedGameV2,
} from "../../test/testkit-v2.ts";

const WATERMARK: BoardWatermarkV2 = {
  sourceStreamId: "games/game-v2/events",
  sourceThroughOffset: "0000000000000042",
  generation: "hex1",
  boardStreamId: "games/game-v2/projections/board/hex1",
};

function decisionFor(game: ScriptedGameV2, playerId: string) {
  return buildDecisionContextV2(game.state(), playerId, WATERMARK);
}

/** A two-player game where the second seat holds a single country. */
function nearlyWonGame(mapSeed: string): ScriptedGameV2 {
  return startGameV2({
    players: 2,
    mapSeed,
    board: ({ map, turnOrder }) => {
      const [winner, loser] = turnOrder;
      const ids = map.territories.map((t) => t.id);
      return {
        initialTerritories: ids.map((territoryId, index) => ({
          territoryId,
          ownerId: index === ids.length - 1 ? loser! : winner!,
          armies: index === ids.length - 1 ? 1 : 3,
        })),
      };
    },
  });
}

describe("risk-demo-v2 decision context", () => {
  it("reports active-turn for the player whose turn it is", () => {
    const game = startGameV2({ players: 3, mapSeed: "decision-active" });
    const active = game.state().activePlayerId!;
    const decision = decisionFor(game, active);

    expect(decision.mode).toBe("active-turn");
    expect(decision.ruleset).toBe("risk-demo-v2");
    expect(decision.turn.id).toBe(game.turnId());
    expect(decision.turn.phase).toBe("reinforce");
    expect(decision.turn.reinforcement.total).toBeGreaterThanOrEqual(3);
    expect(decision.legalMoves.map((a) => a.type)).toContain("reinforce");
  });

  it("reports waiting, with no actions, for everyone else", () => {
    const game = startGameV2({ players: 3, mapSeed: "decision-waiting" });
    const active = game.state().activePlayerId!;
    const bystander = game.playerIds.find((id) => id !== active)!;
    const decision = decisionFor(game, bystander);

    expect(decision.mode).toBe("waiting");
    expect(decision.legalMoves).toEqual([]);
    // The board is still fully visible — v2 has no fog of war.
    expect(decision.board.territories.length).toBeGreaterThan(0);
  });

  it("reports defense for the defender only, and waiting for the attacker", () => {
    const game = startGameV2({ players: 3, mapSeed: "decision-defense" });
    const setup = armForAttack(game);
    game.rig([6, 6, 6]);
    const attackId = declareAttackV2(game, setup);

    const defender = decisionFor(game, setup.defenderId);
    expect(defender.mode).toBe("defense");
    expect(defender.pendingInteraction?.type).toBe("defense");
    expect(defender.legalMoves).toEqual([
      {
        type: "roll-defense",
        attackId,
        dice: expect.any(Number),
        deadlineAt: expect.any(Number),
        submit: { type: "roll-defense", attackId: "<attackId>" },
      },
    ]);

    const attacker = decisionFor(game, setup.attackerId);
    expect(attacker.mode).toBe("waiting");
    expect(attacker.legalMoves).toEqual([]);
    // The attacker still sees the interrupt they are blocked on.
    expect(attacker.pendingInteraction?.attackId).toBe(attackId);

    const bystander = game.playerIds.find(
      (id) => id !== setup.attackerId && id !== setup.defenderId,
    )!;
    expect(decisionFor(game, bystander).mode).toBe("waiting");
  });

  it("keeps the attacker on active-turn while an occupation is pending", () => {
    const game = startGameV2({ players: 2, mapSeed: "decision-occupation" });
    const setup = armForAttack(game);
    const pending = throwUntilCapture(game, setup);

    const attacker = decisionFor(game, setup.attackerId);
    expect(attacker.mode).toBe("active-turn");
    expect(attacker.legalMoves).toEqual([
      {
        type: "occupy-territory",
        attackId: pending.attackId,
        from: pending.from,
        to: pending.to,
        minArmies: pending.minArmies,
        maxArmies: pending.maxArmies,
        submit: {
          type: "occupy-territory",
          attackId: "<attackId>",
          armies: "<minArmies..maxArmies>",
        },
      },
    ]);
    expect(decisionFor(game, setup.defenderId).mode).toBe("waiting");
  });

  it("reports finished for everyone once the game is won", () => {
    const game = nearlyWonGame("decision-finished");
    const setup = armForAttack(game);
    winThrow(game, setup);
    occupyPending(game);

    const state = game.state();
    expect(state.status).toBe("finished");
    for (const playerId of game.playerIds) {
      const decision = decisionFor(game, playerId);
      expect(decision.mode).toBe("finished");
      expect(decision.legalMoves).toEqual([]);
    }
  });

  it("names the map instead of shipping it, and reports the projection watermark", () => {
    const game = startGameV2({ players: 2, mapSeed: "decision-map-ref" });
    const decision = decisionFor(game, game.state().activePlayerId!);

    expect(decision.board.map).toEqual({
      mapVersion: "procedural-hex-v1",
      generatorVersion: "hex-generator-v1",
      seed: "decision-map-ref",
      boardStreamId: WATERMARK.boardStreamId,
      territoryCount: 16,
      continentCount: 4,
    });
    // No tiles, adjacency, names, or label anchors ride along on every fetch.
    expect(JSON.stringify(decision.board)).not.toContain("labelAnchor");
    expect(JSON.stringify(decision.board)).not.toContain("terrain");

    expect(decision.board.sourceStreamId).toBe(WATERMARK.sourceStreamId);
    expect(decision.board.sourceThroughOffset).toBe(WATERMARK.sourceThroughOffset);
    expect(decision.board.generation).toBe(WATERMARK.generation);
  });

  it("rejects a player who is not in the game", () => {
    const game = startGameV2({ players: 2, mapSeed: "decision-unknown" });
    expect(() => decisionFor(game, "nobody")).toThrow("unknown player nobody");
  });
});
