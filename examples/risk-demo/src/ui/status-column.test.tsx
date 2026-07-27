/**
 * The status column rendered for real.
 *
 * It is the half of the screen a player reads *between* decisions, so what matters
 * is that it never disagrees with the board: derived player counters are shown as
 * recorded, continent occupation squares follow projected ownership in stable map
 * order, and their accessible labels communicate the same result without colour.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type {
  ProjectedContinentV2,
  ProjectedPlayerV2,
  ProjectedTerritoryV2,
} from "../board/projection-v2.ts";
import type { NameLookup } from "./presentation-v2.ts";
import { StatusColumn } from "./status-column.tsx";

const NAMES: NameLookup = {
  territory: (id) => id,
  player: (id) => ({ p1: "Ada", p2: "Mina" })[id ?? ""] ?? "Nobody",
  continent: (id) => ({ c1: "Northreach", c2: "Sunder" })[id] ?? id,
};

const PLAYERS: ProjectedPlayerV2[] = [
  {
    id: "p1",
    name: "Ada",
    color: "#e05a47",
    controller: "human",
    eliminated: false,
    territoryCount: 3,
    armyCount: 12,
  },
  {
    id: "p2",
    name: "Mina",
    color: "#3b82f6",
    controller: "external-agent",
    eliminated: false,
    territoryCount: 1,
    armyCount: 4,
  },
];

const CONTINENTS: ProjectedContinentV2[] = [
  {
    id: "c1",
    name: "Northreach",
    territoryIds: ["t1", "t2"],
    reinforcementBonus: 2,
    controllerId: "p1",
    palette: { base: "#101010", accent: "#202020" } as never,
  },
  {
    id: "c2",
    name: "Sunder",
    territoryIds: ["t3", "t4", "t5"],
    reinforcementBonus: 3,
    palette: { base: "#101010", accent: "#202020" } as never,
  },
];

const territory = (id: string, continentId: string, ownerId?: string): ProjectedTerritoryV2 => ({
  id,
  name: id,
  continentId,
  ...(ownerId ? { ownerId } : {}),
  armies: 4,
  hexIds: [],
  adjacentTerritoryIds: [],
  labelAnchor: { q: 0, r: 0 },
});

const TERRITORIES = [
  territory("t1", "c1", "p1"),
  territory("t2", "c1", "p1"),
  territory("t3", "c2", "p1"),
  territory("t4", "c2", "p2"),
  territory("t5", "c2"),
];

const renderStatus = (territories: ProjectedTerritoryV2[] = TERRITORIES) =>
  renderToStaticMarkup(
    <StatusColumn
      players={PLAYERS}
      continents={CONTINENTS}
      territories={territories}
      moves={[]}
      names={NAMES}
      colorOf={(id) => PLAYERS.find((player) => player.id === id)?.color ?? "#5b6a7d"}
      activePlayerId="p1"
      selfId="p2"
    />,
  );

describe("status column", () => {
  const html = renderStatus();

  it("summarizes each seat from the projection's derived counters", () => {
    expect(html).toContain("3 countries · 12 armies");
    expect(html).toContain("1 country · 4 armies");
    expect(html).toContain("Mina (you)");
    expect(html).toContain("Agent");
  });

  it("renders exactly one occupation square per territory in stable continent order", () => {
    expect(html.match(/class="occupation-square/g)).toHaveLength(5);
    expect(html.indexOf('data-territory-id="t3"')).toBeLessThan(
      html.indexOf('data-territory-id="t4"'),
    );
    expect(html.indexOf('data-territory-id="t4"')).toBeLessThan(
      html.indexOf('data-territory-id="t5"'),
    );
    expect(html).toContain('data-territory-id="t3" style="--occupant:#e05a47"');
    expect(html).toContain('data-territory-id="t4" style="--occupant:#3b82f6"');
    expect(html).toContain('class="occupation-square unclaimed"');
  });

  it("updates square colour and holding text directly from projected ownership", () => {
    const updated = renderStatus(
      TERRITORIES.map((row) => (row.id === "t3" ? { ...row, ownerId: "p2" } : row)),
    );
    expect(updated).toContain('data-territory-id="t3" style="--occupant:#3b82f6"');
    expect(updated).toContain('aria-label="Mina 2 territories · Unclaimed 1 territory"');
  });

  it("replaces visible holding labels with a non-colour accessible summary", () => {
    expect(html).not.toContain('class="held-by"');
    expect(html).not.toContain('class="holder-chip"');
    expect(html).toContain('aria-label="Held by Ada · 2 territories"');
    expect(html).toContain(
      'aria-label="Unclaimed 1 territory · Ada 1 territory · Mina 1 territory"',
    );
  });

  it("says plainly that history is still empty rather than inventing filler", () => {
    expect(html).toContain("Moves will appear here as they commit.");
  });
});
