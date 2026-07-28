/**
 * The lobby's terrain survey: the procedural battlefield, sketched before deployment.
 *
 * `generateHexMap` is pure in `(seed, playerCount)` and the projected game carries
 * `mapSeed` from `GameCreated`, so the lobby can draw the very sheet the canonical
 * `GameStarted` snapshot will later record — provided the roster size holds. The
 * sketch is presentational only: no ownership, no armies, no interaction. The
 * canonical snapshot remains the sole authority for the map a game is played on,
 * which is why the sheet is captioned as an advance copy rather than the map.
 */

import { useMemo } from "react";

import type { Axial } from "../domain/hex.ts";
import { generateHexMap } from "../domain/hex-generator.ts";
import type { GeneratedMap } from "../domain/map-v2.ts";
import { RULES_V2 } from "../domain/map-v2.ts";
import {
  hexCenter,
  hexPolygonPoints,
  hexesViewBox,
  regionOutlinePath,
  viewBoxAttribute,
  type Point,
} from "./hex-layout.ts";
import { TerrainDefs } from "./hex-map.tsx";

/** Sheet scale. The viewBox normalizes it; it only fixes stroke/label ratios. */
const SURVEY_HEX_RADIUS = 26;

/**
 * The generated sheet, or `null` when there is nothing honest to draw. A roster
 * below the ruleset minimum does not have a canonical map yet: drawing the
 * two-player profile for a one-player lobby made the survey appear unchanged when
 * the challenger arrived. Invalid seeds and out-of-bounds rosters likewise remain
 * pending rather than inventing terrain.
 */
export function surveyMap(seed: string | undefined, playerCount: number): GeneratedMap | null {
  if (!seed || playerCount < RULES_V2.minPlayers || playerCount > RULES_V2.maxPlayers) {
    return null;
  }
  try {
    return generateHexMap({ seed, playerCount });
  } catch {
    return null;
  }
}

function centroid(points: readonly Point[]): Point {
  let x = 0;
  let y = 0;
  for (const point of points) {
    x += point.x;
    y += point.y;
  }
  const count = Math.max(points.length, 1);
  return { x: x / count, y: y / count };
}

export function LobbyTerrainPreview(props: { seed: string | undefined; playerCount: number }) {
  const map = useMemo(
    () => surveyMap(props.seed, props.playerCount),
    [props.seed, props.playerCount],
  );

  if (!map) {
    const reason = !props.seed
      ? "the game records its map seed"
      : props.playerCount < RULES_V2.minPlayers
        ? `at least ${RULES_V2.minPlayers} players are seated`
        : "the roster is valid";
    return <div className="survey-pending">Survey pending — drawn once {reason}.</div>;
  }

  const axialById = new Map<string, Axial>(map.tiles.map((tile) => [tile.id, tile]));
  const axialsFor = (hexIds: readonly string[]): Axial[] =>
    hexIds.map((id) => axialById.get(id)).filter((hex): hex is Axial => hex !== undefined);
  const hexIdsByTerritory = new Map(map.territories.map((t) => [t.id, t.hexIds]));

  const continentRegions = map.continents.map((continent) => {
    const axials = axialsFor(
      continent.territoryIds.flatMap((id) => hexIdsByTerritory.get(id) ?? []),
    );
    return {
      id: continent.id,
      name: continent.name,
      path: regionOutlinePath(axials, SURVEY_HEX_RADIUS),
      label: centroid(axials.map((axial) => hexCenter(axial, SURVEY_HEX_RADIUS))),
    };
  });

  return (
    <figure className="terrain-survey">
      <svg
        viewBox={viewBoxAttribute(hexesViewBox(map.tiles, SURVEY_HEX_RADIUS))}
        role="img"
        aria-label={`Terrain survey: ${map.territories.length} territories across ${map.continents.length} continents`}
      >
        <TerrainDefs />
        <g aria-hidden="true">
          {map.tiles.map((tile) => (
            <polygon
              key={tile.id}
              points={hexPolygonPoints(tile, SURVEY_HEX_RADIUS)}
              fill={`url(#terrain-${tile.terrain})`}
            />
          ))}
        </g>
        <g className="survey-borders" aria-hidden="true">
          {map.territories.map((territory) => (
            <path
              key={territory.id}
              d={regionOutlinePath(axialsFor(territory.hexIds), SURVEY_HEX_RADIUS)}
            />
          ))}
        </g>
        <g className="survey-continents" aria-hidden="true">
          {continentRegions.map((continent) => (
            <path key={continent.id} d={continent.path} />
          ))}
        </g>
        <g aria-hidden="true">
          {continentRegions.map((continent) => (
            <text
              key={continent.id}
              className="survey-label"
              x={continent.label.x}
              y={continent.label.y}
              textAnchor="middle"
            >
              {continent.name}
            </text>
          ))}
        </g>
      </svg>
      <figcaption>
        {map.territories.length} territories · {map.continents.length} continents · surveyed for{" "}
        {props.playerCount} {props.playerCount === 1 ? "player" : "players"}
      </figcaption>
    </figure>
  );
}
