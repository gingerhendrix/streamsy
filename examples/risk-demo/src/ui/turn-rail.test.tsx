/**
 * The rail rendered for real, not just its helpers.
 *
 * These are server-rendered snapshots of the two states a player most needs the
 * rail to get right and which no pure helper covers on its own: the live turn,
 * and the finished game. The finished one is here because a game that has ended
 * has no legal action for anybody, and the honest thing to show then is the
 * winner — not "waiting for another player" (D6).
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ProjectedTurnV2 } from "../board/projection-v2.ts";
import type { NameLookup } from "./presentation-v2.ts";
import { TurnRail, VictoryCard } from "./turn-rail.tsx";

const NAMES: NameLookup = {
  territory: (id) => ({ t1: "Ashfell", t2: "Northgate" })[id] ?? id,
  player: (id) => ({ p1: "Ada", p2: "Mina" })[id ?? ""] ?? "Nobody",
  continent: (id) => ({ c1: "Northreach" })[id] ?? id,
};

const TURN: ProjectedTurnV2 = {
  id: "turn",
  turnId: "round-4:p1",
  round: 4,
  playerId: "p1",
  phase: "attack",
  reinforcement: { base: 4, continents: [{ continentId: "c1", bonus: 2 }], total: 6, remaining: 2 },
  reinforcementsPlaced: 4,
  attacksDeclared: 1,
  throwsResolved: 1,
  captures: 0,
  eliminations: 0,
};

const ADA = { id: "p1", name: "Ada", color: "#e05a47" } as any;

describe("current-turn rail", () => {
  it("renders the live turn with its phase stepper and reinforcement equation", () => {
    const html = renderToStaticMarkup(
      <TurnRail
        round={4}
        status="playing"
        turn={TURN}
        phase="attack"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Your turn"
        selfId="p1"
      />,
    );
    expect(html).toContain("Round 4");
    expect(html).toContain("Your turn");
    expect(html).toContain("6 total = 4 territory + 2 Northreach");
    expect(html).toContain("4 placed · 2 remaining");
    expect(html).toContain("phase-stepper");
  });

  it("renders a finished game as a result, not as a turn in progress", () => {
    const html = renderToStaticMarkup(
      <TurnRail
        round={23}
        status="finished"
        turn={{
          ...TURN,
          phase: undefined,
          reinforcement: { base: 0, continents: [], total: 0, remaining: 0 },
        }}
        phase={undefined}
        activePlayer={undefined}
        names={NAMES}
        statusLine="Ada wins the map"
        controls={<VictoryCard winnerName="Ada" round={23} />}
      />,
    );
    expect(html).toContain("Ada wins the map");
    expect(html).toContain("Ada conquered the map");
    expect(html).toContain("23 rounds played");
    // No phase stepper and no reinforcement pool once the game is over.
    expect(html).not.toContain("phase-stepper");
    expect(html).not.toContain("reinforcement-card");
    expect(html).not.toContain("waiting");
  });

  it("names the map rather than a player when the winner is unknown", () => {
    expect(renderToStaticMarkup(<VictoryCard round={9} />)).toContain("The campaign is over");
  });
});
