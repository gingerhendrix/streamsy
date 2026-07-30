/**
 * The board reducer's declared identity, pinned to its actual output.
 *
 * The same guarantee the known-seed map hashes give the generator, for the same
 * reason: a projection stream is a durable artefact, and a resumed runtime
 * appends to rows a previous version of this code wrote. A reducer whose output
 * changed while `BOARD_REDUCER_VERSION` stayed put is a stream nobody can tell
 * needs rebuilding.
 *
 * So the hash below is not a snapshot for its own sake — it is the tripwire.
 * Changing what the reducer emits breaks it, and the only correct repair is to
 * bump the version *and* repin, which is exactly the pair of edits that keeps the
 * declared identity true.
 */

import { describe, expect, it } from "vitest";

import { BOARD_REDUCER_VERSION } from "./board-projection.ts";
import { initialProjection, projectEvent, type ProjectionState } from "./projection.ts";
import { canonicalJson } from "../domain/hex-generator.ts";
import { fnv1a32 } from "../domain/generator-rng.ts";
import { GENERATOR_VERSION, MAP_VERSION } from "../domain/map.ts";
import type { GameEvent } from "../domain/events.ts";

/**
 * A lobby's whole roster vocabulary in one log: a seat created, one joined, one
 * delegated to an agent, one renamed, one gone. Deliberately lobby-only — the
 * playing events are pinned by their own equivalence tests, and a fixture that
 * dealt a map would pin the *generator* here by accident.
 */
const ROSTER_LOG: readonly GameEvent[] = [
  {
    type: "GameCreated",
    gameId: "game",
    hostPlayerId: "p1",
    hostName: "Host",
    hostColor: "#e05a47",
    hostController: "human",
    mapVersion: MAP_VERSION,
    generatorVersion: GENERATOR_VERSION,
    mapSeed: "reducer-version-fixture",
    commandId: "c1",
  },
  {
    type: "PlayerJoined",
    playerId: "p2",
    name: "Mina",
    color: "#3b82f6",
    controller: "human",
    commandId: "c2",
  },
  {
    type: "PlayerJoined",
    playerId: "p3",
    name: "Agent 3",
    color: "#4d7c2f",
    controller: "external-agent",
    commandId: "c3",
  },
  { type: "PlayerRenamed", playerId: "p3", name: "Blücher", commandId: "c4" },
  {
    type: "PlayerControllerChanged",
    playerId: "p1",
    controller: "external-agent",
    commandId: "c5",
  },
  { type: "PlayerLeft", playerId: "p2", commandId: "c6" },
];

function projectAll(events: readonly GameEvent[]): ProjectionState {
  let state = initialProjection("game");
  events.forEach((event, index) => {
    state = projectEvent(state, event, `0000000000000000${index}_0000000000`);
  });
  return state;
}

/** Stable hash of the fixture's projected state, in the generator's encoding. */
function rosterHash(): string {
  const encoded = canonicalJson(projectAll(ROSTER_LOG));
  return fnv1a32(encoded).toString(16).padStart(8, "0");
}

/**
 * The pinned pair. Both literals, deliberately: a hash derived at run time from
 * the code under test would assert nothing, and a version read from the constant
 * it is meant to police would police nothing. Repin only together.
 */
const PINNED_REDUCER_VERSION = "hex-domination:board-2";
const PINNED_ROSTER_HASH = "f44d24d5";

describe("board reducer version", () => {
  it("is the version this reducer's output is pinned to", () => {
    expect(BOARD_REDUCER_VERSION).toBe(PINNED_REDUCER_VERSION);
  });

  it("reduces the roster vocabulary to the pinned state", () => {
    expect(rosterHash()).toBe(PINNED_ROSTER_HASH);
  });

  it("emits rows a `board-1` reducer could not have written", () => {
    const state = projectAll(ROSTER_LOG);
    // The three behaviours the bump to `board-2` is *about*, asserted directly so
    // a future refactor cannot satisfy the hash by coincidence.
    expect(state.players.map((player) => player.id)).toEqual(["p1", "p3"]);
    expect(state.players.find((player) => player.id === "p3")?.name).toBe("Blücher");
    expect(state.moves.find((move) => move.kind === "PlayerLeft")?.name).toBe("Mina");
  });

  it("keeps the version distinct from the generation it writes into", () => {
    // `board1` names one game's output stream; this names the code that filled
    // it. Conflating them would make a rebuild look like a version change.
    expect(BOARD_REDUCER_VERSION).not.toMatch(/^board\d+$/);
    expect(BOARD_REDUCER_VERSION.startsWith("hex-domination:")).toBe(true);
  });
});
