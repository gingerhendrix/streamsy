/**
 * The terrain survey is only honest if it is the sheet the game would actually
 * start on: pure in `(seed, playerCount)`, absent rather than invented when there
 * is no seed, and identical across renders of the same inputs.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { hashGeneratedMap } from "../domain/hex-generator.ts";
import { mapProfileFor } from "../domain/map.ts";
import { LobbyTerrainPreview, surveyMap } from "./lobby-preview.tsx";

describe("surveyMap", () => {
  it("draws nothing without a seed or a valid roster", () => {
    expect(surveyMap(undefined, 2)).toBeNull();
    expect(surveyMap("", 2)).toBeNull();
    expect(surveyMap("lobby-survey-seed", 1)).toBeNull();
    expect(surveyMap("lobby-survey-seed", 5)).toBeNull();
  });

  it("is deterministic in seed and player count", () => {
    const a = surveyMap("lobby-survey-seed", 2)!;
    const b = surveyMap("lobby-survey-seed", 2)!;
    expect(hashGeneratedMap(a)).toBe(hashGeneratedMap(b));
    expect(hashGeneratedMap(surveyMap("another-seed", 2)!)).not.toBe(hashGeneratedMap(a));
  });

  it("matches the map profile and changes for each valid roster increase", () => {
    const maps = [2, 3, 4].map((playerCount) => surveyMap("lobby-survey-seed", playerCount)!);

    for (const [index, playerCount] of [2, 3, 4].entries()) {
      const profile = mapProfileFor(playerCount);
      expect(maps[index]!.territories.length).toBe(profile.territories);
      expect(maps[index]!.continents.length).toBe(profile.continents);
    }

    expect(new Set(maps.map(hashGeneratedMap)).size).toBe(maps.length);
  });
});

describe("LobbyTerrainPreview", () => {
  it("shows the pending sheet without a seed or enough players", () => {
    const withoutSeed = renderToStaticMarkup(
      <LobbyTerrainPreview seed={undefined} playerCount={2} />,
    );
    expect(withoutSeed).toContain("Survey pending");
    expect(withoutSeed).toContain("records its map seed");
    expect(withoutSeed).not.toContain("<svg");

    const onePlayer = renderToStaticMarkup(
      <LobbyTerrainPreview seed="lobby-survey-seed" playerCount={1} />,
    );
    expect(onePlayer).toContain("at least 2 players are seated");
    expect(onePlayer).not.toContain("<svg");
  });

  it("draws every tile, territory border, and continent name for a seed", () => {
    const map = surveyMap("lobby-survey-seed", 2)!;
    const markup = renderToStaticMarkup(
      <LobbyTerrainPreview seed="lobby-survey-seed" playerCount={2} />,
    );
    expect(markup).toContain("<svg");
    expect(markup.match(/<polygon /g)?.length).toBe(map.tiles.length);
    for (const continent of map.continents) {
      expect(markup).toContain(`>${continent.name}</text>`);
    }
    expect(markup).toContain(`${map.territories.length} territories`);
    expect(markup).toContain(`${map.continents.length} continents`);
  });

  it("renders identically for the same inputs", () => {
    const render = () =>
      renderToStaticMarkup(<LobbyTerrainPreview seed="lobby-survey-seed" playerCount={2} />);
    expect(render()).toBe(render());
  });
});
