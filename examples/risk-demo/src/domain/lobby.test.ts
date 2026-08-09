/**
 * Lobby roster commands: renaming a seat and giving one up.
 *
 * Both are lobby-only by design. Once `GameStarted` has dealt the board, the
 * roster the match is played under is the roster its recorded history names, so
 * the interesting assertions here are as much about what is refused after the
 * start as about what is accepted before it.
 */

import { describe, expect, it } from "vitest";

import { foldAggregate } from "./aggregate.ts";
import type { AggregateState } from "./aggregate.ts";
import type { Command } from "./commands.ts";
import { decide, normalizePlayerName } from "./decide.ts";
import type { DecideContext } from "./decide.ts";
import type { GameEvent } from "./events.ts";
import { RULES } from "./map.ts";
import { createSeededRng } from "./rng.ts";

const ctx: DecideContext = { rng: createSeededRng(3), now: () => 1_700_000_000_000 };

let sequence = 0;
const commandId = (): string => `lobby-${(sequence += 1)}`;

/** A two-seat lobby: a human host, a human guest, and an agent seat. */
function lobby(): { events: GameEvent[]; state(): AggregateState; run(command: Command): void } {
  const events: GameEvent[] = [];
  const run = (command: Command) => {
    const decision = decide(foldAggregate(events), command, ctx);
    if (decision.status !== "accepted") {
      throw new Error(`${command.type} rejected: ${decision.error.code}`);
    }
    events.push(...decision.events);
  };
  run({
    type: "create-game",
    commandId: commandId(),
    gameId: "game",
    hostPlayerId: "p1",
    hostName: "Host",
    hostController: "human",
    mapSeed: "lobby-fixture-seed",
  });
  run({
    type: "join-game",
    commandId: commandId(),
    playerId: "p2",
    name: "Mina",
    controller: "human",
  });
  run({
    type: "join-game",
    commandId: commandId(),
    playerId: "p3",
    name: "Agent 3",
    controller: "external-agent",
  });
  return { events, state: () => foldAggregate(events), run };
}

function attempt(game: ReturnType<typeof lobby>, command: Command) {
  return decide(game.state(), command, ctx);
}

describe("the canonical name invariant", () => {
  // Every path that records a seat name goes through the same gate, so
  // `RULES.maxPlayerNameLength` is a property of the game rather than of whichever
  // client happened to be calling.
  const overlong = "W".repeat(40);

  it("normalizes the host's name at creation", () => {
    const events: GameEvent[] = [];
    const created = decide(
      foldAggregate(events),
      {
        type: "create-game",
        commandId: commandId(),
        gameId: "game",
        hostPlayerId: "p1",
        hostName: `  ${overlong}  `,
        hostController: "human",
        mapSeed: "name-invariant-seed",
      },
      ctx,
    );
    expect(created.status).toBe("accepted");
    if (created.status !== "accepted") return;
    expect(created.events[0]).toMatchObject({
      type: "GameCreated",
      hostName: overlong.slice(0, RULES.maxPlayerNameLength),
    });
  });

  it("refuses a blank host name rather than inventing one", () => {
    // Choosing a provisional default for a caller that supplied nothing is an
    // HTTP-boundary product decision; the decider will not guess.
    const blank = decide(
      foldAggregate([]),
      {
        type: "create-game",
        commandId: commandId(),
        gameId: "game",
        hostPlayerId: "p1",
        hostName: "  ",
        hostController: "human",
        mapSeed: "name-invariant-seed",
      },
      ctx,
    );
    expect(blank.status).toBe("rejected");
    if (blank.status === "rejected") expect(blank.error.code).toBe("INVALID_NAME");
  });

  it("normalizes a joining seat and refuses a blank one", () => {
    const game = lobby();
    const blank = attempt(game, {
      type: "join-game",
      commandId: commandId(),
      playerId: "p4",
      name: "\t\n ",
      controller: "human",
    });
    expect(blank.status).toBe("rejected");
    if (blank.status === "rejected") expect(blank.error.code).toBe("INVALID_NAME");

    game.run({
      type: "join-game",
      commandId: commandId(),
      playerId: "p4",
      name: `  ${overlong}  `,
      controller: "human",
    });
    expect(game.state().players[3]!.name).toBe(overlong.slice(0, RULES.maxPlayerNameLength));
  });
});

describe("seat names", () => {
  it("trims, bounds, and refuses a name that is only whitespace", () => {
    expect(normalizePlayerName("   Napoleon   ")).toBe("Napoleon");
    expect(normalizePlayerName("W".repeat(40))).toHaveLength(RULES.maxPlayerNameLength);
    expect(normalizePlayerName("   ")).toBe("");

    const game = lobby();
    const blank = attempt(game, {
      type: "rename-player",
      commandId: commandId(),
      playerId: "p1",
      name: "   ",
    });
    expect(blank.status).toBe("rejected");
    if (blank.status === "rejected") expect(blank.error.code).toBe("INVALID_NAME");
  });

  it("records the trimmed name and folds it onto the seat", () => {
    const game = lobby();
    game.run({
      type: "rename-player",
      commandId: commandId(),
      playerId: "p1",
      name: "  Wellington  ",
    });
    expect(game.events.at(-1)).toMatchObject({ type: "PlayerRenamed", name: "Wellington" });
    expect(game.state().players[0]!.name).toBe("Wellington");
    // Renaming touches nothing else about the seat.
    expect(game.state().players).toHaveLength(3);
    expect(game.state().players[0]!.controller).toBe("human");
  });

  it("renames an agent seat, which is how a host names the agents it invited", () => {
    const game = lobby();
    game.run({
      type: "rename-player",
      commandId: commandId(),
      playerId: "p3",
      name: "Blücher",
    });
    expect(game.state().players[2]!.name).toBe("Blücher");
  });

  it("refuses an unknown seat and a game already under way", () => {
    const game = lobby();
    const unknown = attempt(game, {
      type: "rename-player",
      commandId: commandId(),
      playerId: "p9",
      name: "Ghost",
    });
    expect(unknown.status).toBe("rejected");
    if (unknown.status === "rejected") expect(unknown.error.code).toBe("UNKNOWN_PLAYER");

    game.run({ type: "start-game", commandId: commandId() });
    const late = attempt(game, {
      type: "rename-player",
      commandId: commandId(),
      playerId: "p1",
      name: "Too late",
    });
    expect(late.status).toBe("rejected");
    if (late.status === "rejected") expect(late.error.code).toBe("GAME_ALREADY_STARTED");
  });
});

describe("leaving a seat", () => {
  it("removes the seat from the roster the game would start with", () => {
    const game = lobby();
    game.run({ type: "leave-game", commandId: commandId(), playerId: "p2" });
    expect(game.events.at(-1)).toMatchObject({ type: "PlayerLeft", playerId: "p2" });
    expect(game.state().players.map((player) => player.id)).toEqual(["p1", "p3"]);
  });

  it("lets the creator give up its seat without dissolving the game it hosts", () => {
    const game = lobby();
    game.run({ type: "leave-game", commandId: commandId(), playerId: "p1" });
    const state = game.state();
    expect(state.players.map((player) => player.id)).toEqual(["p2", "p3"]);
    expect(state.status).toBe("lobby");
    // Hosting is a capability, not a seat: the game still starts, with the two
    // seats that remain, and its recorded creator is untouched.
    game.run({ type: "start-game", commandId: commandId() });
    expect(game.state().status).toBe("playing");
    expect(game.state().turnOrder).toHaveLength(2);
  });

  it("holds the start when leaving drops the lobby below the minimum", () => {
    const game = lobby();
    game.run({ type: "leave-game", commandId: commandId(), playerId: "p2" });
    game.run({ type: "leave-game", commandId: commandId(), playerId: "p1" });
    const short = attempt(game, { type: "start-game", commandId: commandId() });
    expect(short.status).toBe("rejected");
    if (short.status === "rejected") expect(short.error.code).toBe("NOT_ENOUGH_PLAYERS");
  });

  it("refuses to give up a seat something other than a person is playing", () => {
    const game = lobby();
    const agentSeat = attempt(game, {
      type: "leave-game",
      commandId: commandId(),
      playerId: "p3",
    });
    expect(agentSeat.status).toBe("rejected");
    if (agentSeat.status === "rejected") expect(agentSeat.error.code).toBe("ILLEGAL_ACTION");

    // Same rule via delegation: once the host's own seat is an agent's, the host
    // capability naming it can no longer take it back by "leaving".
    game.run({ type: "delegate-agent-seat", commandId: commandId(), playerId: "p1" });
    const delegated = attempt(game, {
      type: "leave-game",
      commandId: commandId(),
      playerId: "p1",
    });
    expect(delegated.status).toBe("rejected");
    if (delegated.status === "rejected") expect(delegated.error.code).toBe("ILLEGAL_ACTION");
  });

  it("refuses an unknown seat and a game already under way", () => {
    const game = lobby();
    const unknown = attempt(game, { type: "leave-game", commandId: commandId(), playerId: "p9" });
    expect(unknown.status).toBe("rejected");
    if (unknown.status === "rejected") expect(unknown.error.code).toBe("UNKNOWN_PLAYER");

    game.run({ type: "start-game", commandId: commandId() });
    const late = attempt(game, { type: "leave-game", commandId: commandId(), playerId: "p2" });
    expect(late.status).toBe("rejected");
    if (late.status === "rejected") expect(late.error.code).toBe("GAME_ALREADY_STARTED");
  });
});
