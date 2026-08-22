/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
/**
 * The lobby rendered for real: the muster roll always shows all four
 * seats, annotations carry role/controller/self without colour, and the host's
 * commands change with the roster instead of guessing.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ProjectedPlayer } from "../board/projection.ts";
import { Lobby } from "./lobby.tsx";
import type { Identity } from "./shared.tsx";

function player(overrides: Partial<ProjectedPlayer> & { id: string }): ProjectedPlayer {
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

const PLAYER_IDENTITY: Identity = { gameId: "g1", playerId: "p2", token: "t", role: "player" };

function renderLobby(options: {
  players: ProjectedPlayer[];
  identity?: Identity | null;
  spectating?: boolean;
  mapSeed?: string;
}): string {
  return renderToStaticMarkup(
    <Lobby
      players={options.players}
      hostPlayerId="p1"
      identity={options.identity ?? null}
      spectating={options.spectating ?? options.identity === undefined}
      name="Visitor"
      busy={false}
      agentSeats={[]}
      mapSeed={options.mapSeed}
      onName={() => {}}
      onJoin={() => {}}
      onStart={() => {}}
      onAddAgent={() => {}}
      onRename={() => {}}
      onLeave={() => {}}
      onCopy={async () => {}}
    />,
  );
}

describe("Lobby muster roll", () => {
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

  it("does not annotate a delegated host seat as the reader's own", () => {
    const markup = renderLobby({
      players: [
        player({ id: "p1", name: "Ada", controller: "external-agent" }),
        player({ id: "p2", name: "Mina", color: "#3b82f6", controller: "external-agent" }),
      ],
      identity: HOST_IDENTITY,
      spectating: true,
    });
    expect(markup).toContain("Host · agent");
    expect(markup).not.toContain("· you");
  });
});

describe("Lobby commands", () => {
  it("offers a join seat, not host commands, before an identity exists", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })] });
    expect(markup).toContain("Join this game");
    expect(markup).toContain("Your name");
    expect(markup).not.toContain("Colour");
    expect(markup).not.toContain("Start game");
    expect(markup).not.toContain("Invite an agent");
    expect(markup).toContain("Copy invite link");
    // Nothing to give up, so nothing to confirm giving up.
    expect(markup).not.toContain("Leave game");
  });

  it("holds the host's start command until two seats are filled", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })], identity: HOST_IDENTITY });
    expect(markup).toContain("Waiting for 2 players");
    expect(markup).toContain("disabled");
    expect(markup).toContain("Invite an agent");
  });

  it("offers the host a name for the next agent seat, defaulting to its roll number", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })], identity: HOST_IDENTITY });
    expect(markup).toContain("Agent name");
    expect(markup).toContain('placeholder="Agent 2"');
  });

  it("drops the agent-name field once every seat is filled", () => {
    const markup = renderLobby({
      players: [
        player({ id: "p1" }),
        player({ id: "p2" }),
        player({ id: "p3" }),
        player({ id: "p4" }),
      ],
      identity: HOST_IDENTITY,
    });
    expect(markup).not.toContain("Agent name");
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

describe("Lobby naming", () => {
  it("offers a rename only on seats this browser may actually rename", () => {
    const markup = renderLobby({
      players: [
        player({ id: "p1", name: "Ada" }),
        player({ id: "p2", name: "Mina", color: "#3b82f6" }),
        player({ id: "p3", name: "Turing", color: "#4d7c2f", controller: "external-agent" }),
      ],
      identity: HOST_IDENTITY,
    });
    // The host's own seat and the agent seat it opened, but never the other
    // person's seat — the server refuses that, so the UI must not offer it.
    expect(markup).toContain('aria-label="Rename Ada"');
    expect(markup).toContain('aria-label="Rename Turing"');
    expect(markup).not.toContain('aria-label="Rename Mina"');
  });

  it("offers a player only their own seat, and a visitor none", () => {
    const seats = [
      player({ id: "p1", name: "Ada" }),
      player({ id: "p2", name: "Mina", color: "#3b82f6" }),
      player({ id: "p3", name: "Turing", color: "#4d7c2f", controller: "external-agent" }),
    ];
    const asPlayer = renderLobby({ players: seats, identity: PLAYER_IDENTITY });
    expect(asPlayer).toContain('aria-label="Rename Mina"');
    expect(asPlayer).not.toContain('aria-label="Rename Ada"');
    // A non-host did not open the agent seat, so it is not theirs to name.
    expect(asPlayer).not.toContain('aria-label="Rename Turing"');

    expect(renderLobby({ players: seats })).not.toContain("Rename");
  });
});

describe("Lobby leaving", () => {
  it("offers leaving to a seated human, host included", () => {
    const seats = [
      player({ id: "p1", name: "Ada" }),
      player({ id: "p2", name: "Mina", color: "#3b82f6" }),
    ];
    expect(renderLobby({ players: seats, identity: HOST_IDENTITY })).toContain("Leave game");
    expect(renderLobby({ players: seats, identity: PLAYER_IDENTITY })).toContain("Leave game");
  });

  it("withholds leaving from a seat this browser no longer plays", () => {
    // A host that already delegated its seat holds a capability naming a seat an
    // agent plays. There is nothing left to give up, and the agent's seat is not
    // the host's to take back.
    const delegated = renderLobby({
      players: [
        player({ id: "p1", name: "Ada", controller: "external-agent" }),
        player({ id: "p2", name: "Mina", color: "#3b82f6", controller: "external-agent" }),
      ],
      identity: HOST_IDENTITY,
      spectating: true,
    });
    expect(delegated).not.toContain("Leave game");

    // Likewise a host that has already left: it still runs the lobby, but the
    // roster no longer carries its seat.
    const left = renderLobby({
      players: [
        player({ id: "p2", name: "Mina", color: "#3b82f6" }),
        player({ id: "p3", name: "Turing", color: "#4d7c2f", controller: "external-agent" }),
      ],
      identity: HOST_IDENTITY,
      spectating: true,
    });
    expect(left).not.toContain("Leave game");
    // ...and it keeps the commands that are the host's, not the seat's.
    expect(left).toContain("Start game");
    expect(left).toContain("Invite an agent");
  });
});

describe("Lobby terrain survey", () => {
  it("waits for a valid roster, then draws for the current player count", () => {
    const waiting = renderLobby({
      players: [player({ id: "p1" })],
      mapSeed: "lobby-test-seed",
    });
    expect(waiting).toContain("Terrain survey");
    expect(waiting).toContain("at least 2 players are seated");
    expect(waiting).not.toContain("<svg");

    const twoPlayers = renderLobby({
      players: [player({ id: "p1" }), player({ id: "p2" })],
      mapSeed: "lobby-test-seed",
    });
    const threePlayers = renderLobby({
      players: [player({ id: "p1" }), player({ id: "p2" }), player({ id: "p3" })],
      mapSeed: "lobby-test-seed",
    });
    expect(twoPlayers).toContain("<svg");
    expect(twoPlayers).toContain("surveyed for 2 players");
    expect(threePlayers).toContain("surveyed for 3 players");
    expect(threePlayers).not.toBe(twoPlayers);
  });

  it("keeps a pending sheet when the seed is not yet projected", () => {
    const markup = renderLobby({ players: [player({ id: "p1" })] });
    expect(markup).toContain("Survey pending");
    expect(markup).not.toContain("<svg");
  });
});
