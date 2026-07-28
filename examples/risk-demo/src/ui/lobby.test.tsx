/**
 * The lobby rendered for real: the muster roll always sets all four ruleset
 * seats, annotations carry role/controller/self without colour, and the host's
 * commands change with the roster instead of guessing.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ProjectedPlayerV2 } from "../board/projection-v2.ts";
import { LobbyV2 } from "./lobby.tsx";
import type { Identity } from "./shared.tsx";

function player(overrides: Partial<ProjectedPlayerV2> & { id: string }): ProjectedPlayerV2 {
  return {
    name: "Ada",
    color: "#e05a47",
    controller: "human",
    eliminated: false,
    territoryCount: 0,
    armyCount: 0,
    ...overrides,
  };
}

const HOST_IDENTITY: Identity = { gameId: "g1", playerId: "p1", token: "t", role: "host" };

function renderLobby(options: {
  players: ProjectedPlayerV2[];
  identity?: Identity | null;
  mapSeed?: string;
}): string {
  return renderToStaticMarkup(
    <LobbyV2
      players={options.players}
      hostPlayerId="p1"
      identity={options.identity ?? null}
      name="Visitor"
      color="#3b82f6"
      busy={false}
      agentSeats={[]}
      unavailableColors={[]}
      mapSeed={options.mapSeed}
      onName={() => {}}
      onColor={() => {}}
      onJoin={() => {}}
      onStart={() => {}}
      onAddAgent={() => {}}
      onCopy={async () => {}}
    />,
  );
}

describe("LobbyV2 muster roll", () => {
  it("always sets four seats, with open seats explained", () => {
    const markup = renderLobby({ players: [player({ id: "p1", name: "Ada" })] });
    expect(markup.match(/muster-row/g)?.length).toBeGreaterThanOrEqual(4);
    expect(markup.match(/Open seat/g)?.length).toBe(3);
    expect(markup).toContain("Needed to start");
    expect(markup).toContain("Optional reinforcement");
    expect(markup).toContain("Ada");
  });

  it("annotates host, controller, and self without relying on colour", () => {
    const markup = renderLobby({
      players: [
        player({ id: "p1", name: "Ada" }),
        player({ id: "p2", name: "Bot Mina", color: "#3b82f6", controller: "external-agent" }),
      ],
      identity: HOST_IDENTITY,
    });
    expect(markup).toContain("Host · you");
    expect(markup).toContain("Player · agent");
    expect(markup).toContain(">Ready<");
  });
});

describe("LobbyV2 commands", () => {
  it("offers a join seat, not host commands, before an identity exists", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })] });
    expect(markup).toContain("Join this game");
    expect(markup).not.toContain("Start game");
    expect(markup).not.toContain("Open an agent seat");
    expect(markup).toContain("Copy invite link");
  });

  it("holds the host's start command until two seats are filled", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })], identity: HOST_IDENTITY });
    expect(markup).toContain("Waiting for 2 players");
    expect(markup).toContain("disabled");
    expect(markup).toContain("Open an agent seat");
  });

  it("arms the start command once a challenger is seated", () => {
    const markup = renderLobby({
      players: [player({ id: "p1" }), player({ id: "p2", name: "Mina", color: "#3b82f6" })],
      identity: HOST_IDENTITY,
    });
    expect(markup).toContain("Start game");
    expect(markup).not.toContain("Waiting for 2 players");
  });
});

describe("LobbyV2 terrain survey", () => {
  it("draws the survey from the projected map seed", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })], mapSeed: "lobby-test-seed" });
    expect(markup).toContain("Terrain survey");
    expect(markup).toContain("<svg");
    expect(markup).toContain("surveyed for 2 players");
  });

  it("keeps a pending sheet when the seed is not yet projected", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })] });
    expect(markup).toContain("Survey pending");
    expect(markup).not.toContain("<svg");
  });
});
