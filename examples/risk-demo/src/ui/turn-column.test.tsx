/**
 * The current-turn column rendered for real, not just its helpers.
 *
 * These are server-rendered snapshots of the information architecture the column
 * exists to enforce: exactly one phase carries controls, earlier phases collapse to
 * what they achieved, later ones are inert, and a finished game is a result rather
 * than a turn in progress (D6).
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ProjectedTurnV2 } from "../board/projection-v2.ts";
import type { NameLookup } from "./presentation-v2.ts";
import { TurnColumn, VictoryCard } from "./turn-column.tsx";

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
  reinforcement: { base: 4, continents: [{ continentId: "c1", bonus: 2 }], total: 6, remaining: 0 },
  reinforcementsPlaced: 6,
  attacksDeclared: 1,
  throwsResolved: 1,
  captures: 0,
  eliminations: 0,
};

const ADA = { id: "p1", name: "Ada", color: "#e05a47" } as never;

const CONTROLS = <button className="primary">Declare attack</button>;

describe("current-turn column", () => {
  it("gives the active phase the instructions and the controls", () => {
    const html = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={TURN}
        phase="attack"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Your turn"
        yourTurn
        selfId="p1"
        controls={CONTROLS}
      />,
    );
    expect(html).toContain("Your turn");
    expect(html).toContain('class="phase-section active"');
    expect(html).toContain("Attack a highlighted enemy neighbour");
    expect(html).toContain("Declare attack");
  });

  it("collapses a finished phase to what it achieved and disables a later one", () => {
    const html = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={TURN}
        phase="attack"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Your turn"
        yourTurn
        selfId="p1"
        controls={CONTROLS}
      />,
    );
    // Reinforce is done: its result is in the header, its equation behind a disclosure.
    expect(html).toContain('class="phase-section completed"');
    expect(html).toContain("6 of 6 armies placed.");
    expect(html).toContain("6 total = 4 territory + 2 Northreach");
    // Fortify has not started: visible, explained, and inert.
    expect(html).toContain('class="phase-section upcoming"');
    expect(html).toContain('aria-disabled="true"');
    expect(html).toContain("Opens after the single fortify move");
  });

  it("presents an uncommitted fortification in the active Fortify section", () => {
    const html = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={TURN}
        phase="fortify"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Your turn"
        yourTurn
        selfId="p1"
        controls={<button>← Back</button>}
      />,
    );
    expect(html).toContain("Move armies once between any two countries");
    expect(html).toMatch(/phase-section completed[\s\S]*Attack/);
    expect(html).toMatch(/phase-section active[\s\S]*Fortify[\s\S]*← Back/);
  });

  it("keeps canonical post-fortification copy when only ending the turn remains", () => {
    const html = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={{ ...TURN, phase: "fortify" }}
        phase="fortify"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Your turn"
        yourTurn
        selfId="p1"
      />,
    );
    expect(html).toContain("The manoeuvre is spent. End the turn when you are ready.");
    expect(html).not.toContain("Move armies once");
  });

  it("ranks an open attack above the phases and files a resolved one under Attack", () => {
    const card = <p>combat card</p>;
    const live = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={TURN}
        phase="attack"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Defend your country"
        yourTurn={false}
        selfId="p2"
        combatCard={card}
        combatLive
      />,
    );
    // Above the phase list while the attack is open — it can interrupt anyone.
    expect(live.indexOf("combat card")).toBeLessThan(live.indexOf("phase-list"));

    const resolved = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={TURN}
        phase="attack"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Your turn"
        yourTurn
        selfId="p1"
        combatCard={card}
      />,
    );
    // Once resolved it is evidence, shown once, inside the phase that produced it.
    expect(resolved.indexOf("combat card")).toBeGreaterThan(resolved.indexOf("phase-list"));
    expect(resolved.split("combat card")).toHaveLength(2);
  });

  it("speaks about the active player rather than to a seat that cannot act", () => {
    const html = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={TURN}
        phase="reinforce"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Waiting for Ada"
        yourTurn={false}
        selfId="p2"
      />,
    );
    expect(html).toContain("Ada is placing reinforcements.");
    expect(html).not.toContain("pick a highlighted country of yours");
  });

  it("renders a finished game as a result, not as a turn in progress", () => {
    const html = renderToStaticMarkup(
      <TurnColumn
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
        yourTurn={false}
        controls={<VictoryCard winnerName="Ada" round={23} />}
      />,
    );
    expect(html).toContain("Ada wins the map");
    expect(html).toContain("Ada conquered the map");
    expect(html).toContain("23 rounds played");
    // No phase sections once there is no turn left to take.
    expect(html).not.toContain("phase-section");
    expect(html).not.toContain("reinforcement-card");
  });

  it("names the map rather than a player when the winner is unknown", () => {
    expect(renderToStaticMarkup(<VictoryCard round={9} />)).toContain("The campaign is over");
  });
});
