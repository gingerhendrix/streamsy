/**
 * The current-turn column rendered for real, not just its helpers.
 *
 * These are server-rendered snapshots of the information architecture the column
 * exists to enforce: exactly one phase carries controls, earlier phases collapse to
 * what they achieved, later ones are inert, and a finished game is a result rather
 * than a turn in progress.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ProjectedPlayer, ProjectedTurn } from "../board/projection.ts";
import type { NameLookup } from "./presentation.ts";
import { TurnColumn, VictoryCard } from "./turn-column.tsx";

const NAMES: NameLookup = {
  territory: (id) => ({ t1: "Ashfell", t2: "Northgate" })[id] ?? id,
  player: (id) => ({ p1: "Ada", p2: "Mina" })[id ?? ""] ?? "Nobody",
  continent: (id) => ({ c1: "Northreach" })[id] ?? id,
};

const TURN: ProjectedTurn = {
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

const ADA: ProjectedPlayer = {
  id: "p1",
  name: "Ada",
  color: "#e05a47",
  controller: "human",
  eliminated: false,
  territoryCount: 3,
  armyCount: 12,
};

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

  it("keeps the reinforcement summary to the equation above the placement controls", () => {
    const html = renderToStaticMarkup(
      <TurnColumn
        status="playing"
        turn={{
          ...TURN,
          phase: "reinforce",
          reinforcement: { base: 3, continents: [], total: 3, remaining: 3 },
          reinforcementsPlaced: 0,
        }}
        phase="reinforce"
        activePlayer={ADA}
        names={NAMES}
        statusLine="Your turn"
        yourTurn
        selfId="p1"
        controls={<p>Placement controls</p>}
      />,
    );
    expect(html).toContain("3 total = 3 territory");
    expect(html).not.toContain("0 placed · 3 remaining");
    expect(html).not.toContain("continent-chip");
    expect(html.indexOf("3 total = 3 territory")).toBeLessThan(html.indexOf("Placement controls"));
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
    expect(html).toContain("Opens after attacking, for one optional army move.");
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
        fortifyAction={<button className="phase-back">← Back</button>}
        controls={<p>Fortification controls</p>}
      />,
    );
    expect(html).toContain("Move armies once between any two countries");
    expect(html).toMatch(/phase-section completed[\s\S]*Attack/);
    expect(html).toMatch(/phase-section active[\s\S]*Fortify[\s\S]*← Back/);
    expect(html.indexOf("← Back")).toBeLessThan(
      html.indexOf("Move armies once between any two countries"),
    );
    expect(html.indexOf("Move armies once between any two countries")).toBeLessThan(
      html.indexOf("Fortification controls"),
    );
  });

  it("describes a persisted post-fortification state as automatically ending", () => {
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
    expect(html).toContain("The manoeuvre is spent and the turn ends automatically.");
    expect(html).not.toContain("Move armies once");
  });

  it("keeps both an open attack and a resolved throw inside the Attack phase", () => {
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
      />,
    );
    expect(live.indexOf("combat card")).toBeGreaterThan(live.indexOf("phase-list"));
    expect(live.indexOf("combat card")).toBeLessThan(live.lastIndexOf("Fortify"));
    expect(live.split("combat card")).toHaveLength(2);

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
    expect(resolved.indexOf("combat card")).toBeGreaterThan(resolved.indexOf("phase-list"));
    expect(resolved.indexOf("combat card")).toBeLessThan(resolved.lastIndexOf("Fortify"));
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
