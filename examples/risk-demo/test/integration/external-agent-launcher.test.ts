import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import {
  actionIsLegal,
  buildModelContract,
  resolveModelSelection,
} from "../../external-agent/risk-seat.mjs";

const execFileAsync = promisify(execFile);
const launcher = path.resolve("external-agent/risk-seat.mjs");
const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("repository-independent external-seat launcher", () => {
  it("validates the published reinforcement pool and submit shape", () => {
    const legalMoves = [
      {
        type: "reinforce",
        territoryIds: ["a", "b"],
        pool: 4,
        submit: { type: "reinforce", placements: [] },
      },
    ];
    expect(
      actionIsLegal(
        {
          type: "reinforce",
          placements: [
            { territoryId: "a", armies: 1 },
            { territoryId: "b", armies: 3 },
          ],
        },
        legalMoves,
      ),
    ).toBe(true);
    expect(
      actionIsLegal(
        { type: "reinforce", placements: [{ territoryId: "a", armies: 3 }] },
        legalMoves,
      ),
    ).toBe(false);
  });

  it("keeps canonical ids in the launcher and resolves an indexed model choice", () => {
    const decision = {
      player: { id: "p1" },
      mode: "active-turn",
      turn: { id: "turn", phase: "attack", activePlayerId: "p1" },
      legalMoves: [
        {
          type: "occupy-territory",
          attackId: "attack",
          from: "secret-a",
          to: "secret-b",
          minArmies: 2,
          maxArmies: 4,
        },
      ],
    };
    const contract = buildModelContract(decision, {
      players: [{ id: "p1", eliminated: false }],
      territories: [
        { id: "secret-a", ownerId: "p1", armies: 5 },
        { id: "secret-b", ownerId: null, armies: 0 },
      ],
      continents: [],
    });
    expect(JSON.stringify(contract.observation)).not.toContain("secret-a");
    expect(resolveModelSelection({ choiceIndex: 0, armies: 3 }, contract.resolution)).toEqual({
      ok: true,
      action: { type: "occupy-territory", attackId: "attack", armies: 3 },
    });
  });

  it("initializes directly from the machine-readable seat descriptor", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "risk-seat-v2-"));
    roots.push(root);
    const state = path.join(root, "state");
    const seat = {
      origin: "http://127.0.0.1:1339",
      gameId: "game",
      playerId: "player",
      token: "rsk_test",
      urls: {
        map: "/v1/games/game/map",
        actions: "/v1/games/game/players/me/actions",
        decision: "/v1/games/game/decision",
        commands: "/v1/games/game/commands",
      },
    };
    await execFileAsync("node", [
      launcher,
      "init",
      "--seat",
      JSON.stringify(seat),
      "--state",
      state,
    ]);
    const session = JSON.parse(await readFile(path.join(state, "session.json"), "utf8"));
    expect(session).toMatchObject({
      version: 2,
      origin: seat.origin,
      gameId: seat.gameId,
      playerId: seat.playerId,
      capability: seat.token,
      cursor: null,
    });
    expect(session.discovery.seatSha256).toMatch(/^[0-9a-f]{64}$/);
  });
});
