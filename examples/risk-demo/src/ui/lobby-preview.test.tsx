/**
 * The terrain survey is only honest if it is the sheet the game would actually
 * start on: pure in `(seed, playerCount)`, absent rather than invented when there
 * is no seed, and identical across renders of the same inputs.
 */

import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { hashGeneratedMap } from "../domain/hex-generator.ts";
import { mapProfileFor } from "../domain/map-v2.ts";
import { LobbyTerrainPreview, surveyMap, surveyPlayerCount } from "./lobby-preview.tsx";

describe("surveyPlayerCount", () => {
  it("holds the roster size to the ruleset player bounds", () => {
    expect(surveyPlayerCount(0)).toBe(2);
    expect(surveyPlayerCount(1)).toBe(2);
    expect(surveyPlayerCount(3)).toBe(3);
    expect(surveyPlayerCount(4)).toBe(4);
    expect(surveyPlayerCount(9)).toBe(4);
  });
});

describe("surveyMap", () => {
  it("draws nothing without a seed", () => {
    expect(surveyMap(undefined, 2)).toBeNull();
    expect(surveyMap("", 2)).toBeNull();
  });

  it("is deterministic in seed and player count", () => {
    const a = surveyMap("lobby-survey-seed", 2)!;
    const b = surveyMap("lobby-survey-seed", 2)!;
    expect(hashGeneratedMap(a)).toBe(hashGeneratedMap(b));
    expect(hashGeneratedMap(surveyMap("another-seed", 2)!)).not.toBe(hashGeneratedMap(a));
  });

  it("matches the ruleset profile for the roster size", () => {
    const map = surveyMap("lobby-survey-seed", 3)!;
    const profile = mapProfileFor(3);
    expect(map.territories.length).toBe(profile.territories);
    expect(map.continents.length).toBe(profile.continents);
  });
});

describe("LobbyTerrainPreview", () => {
  it("shows the pending sheet without a seed", () => {
    const markup = renderToStaticMarkup(<LobbyTerrainPreview seed={undefined} playerCount={2} />);
    expect(markup).toContain("Survey pending");
    expect(markup).not.toContain("<svg");
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
