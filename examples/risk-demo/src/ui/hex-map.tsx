/**
 * The `risk-demo-v2` map: one responsive SVG, drawn from canonical `(q, r)` tiles.
 *
 * Layers, back to front (design spec §8.2):
 *
 *   terrain → ownership → continent boundary → country boundary → highlight →
 *   attack route → labels → interaction
 *
 * Terrain is never signalled by colour alone: each type has its own SVG pattern, so
 * the map stays readable under a translucent owner wash and for a player who cannot
 * separate the hues. Adjacency is not drawn at all — hexes touch, and that *is* the
 * explanation, which is why v1's dotted route lines have no successor here.
 *
 * The interaction layer sits on top as transparent per-country paths carrying
 * `role="button"`, an `aria-label`, and a tab stop. Selection is a click or an
 * Enter/Space on a focused country; nothing depends on hover.
 */

import { useMemo, type CSSProperties, type ReactNode } from "react";

import type { ProjectedContinentV2, ProjectedHexV2 } from "../board/projection-v2.ts";
import type { Terrain } from "../domain/map-v2.ts";
import { TERRAIN_TYPES } from "../domain/map-v2.ts";
import type { Axial } from "../domain/hex.ts";
import {
  attackArrowPath,
  hexCenter,
  hexPolygonPoints,
  hexesViewBox,
  regionOutlinePath,
  viewBoxAttribute,
  type Point,
} from "./hex-layout.ts";

/** Client-chosen tile size. Canonical geometry is coordinates; this is rendering. */
const HEX_RADIUS = 26;

/** How a country should be drawn and what it affords right now. */
export type TerritoryTone =
  | "idle"
  /** A legal starting point for the current intent. */
  | "source"
  /** The chosen source of an in-progress move. */
  | "selected"
  /** A legal destination for the chosen source. */
  | "target"
  /** Not part of the current decision; pushed back so the choice reads. */
  | "dimmed";

export interface MapTerritory {
  id: string;
  name: string;
  continentId: string;
  ownerId?: string;
  armies: number;
  hexIds: string[];
  labelAnchor: Axial;
}

export interface HexMapProps {
  hexes: ProjectedHexV2[];
  territories: MapTerritory[];
  continents: ProjectedContinentV2[];
  colorOf(playerId: string | undefined): string;
  ownerNameOf(playerId: string | undefined): string;
  toneOf(territoryId: string): TerritoryTone;
  /** Countries a player may act on now; everything else is focusable but inert. */
  actionable: ReadonlySet<string>;
  focusedId: string | null;
  onSelect(territoryId: string): void;
  onFocus(territoryId: string): void;
  /** Source → target of the attack being composed, or the throw being revealed. */
  route: { from: string; to: string } | null;
  zoom: number;
  pan: Point;
  children?: ReactNode;
}

const TERRAIN_FILL: Record<Terrain, string> = {
  plains: "#3d5c47",
  forest: "#254a35",
  hills: "#5a5334",
  desert: "#6d5a3a",
  mountains: "#4a4e5c",
};

const TERRAIN_INK: Record<Terrain, string> = {
  plains: "#6f9b7c",
  forest: "#8fc9a2",
  hills: "#a49255",
  desert: "#b79b64",
  mountains: "#9aa2b8",
};

/**
 * A distinct texture per terrain, so the taxonomy survives an owner overlay and
 * does not rely on hue. Patterns are declared once and referenced by every tile.
 */
function terrainMarks(terrain: Terrain): ReactNode {
  const ink = TERRAIN_INK[terrain];
  switch (terrain) {
    case "plains":
      return (
        <>
          <path d="M2 6 h5" stroke={ink} strokeWidth="1" opacity="0.5" />
          <path d="M7 12 h5" stroke={ink} strokeWidth="1" opacity="0.5" />
        </>
      );
    case "forest":
      return (
        <>
          <path d="M4 10 l2.5 -5 l2.5 5 z" fill={ink} opacity="0.62" />
          <path d="M10 15 l1.8 -3.6 l1.8 3.6 z" fill={ink} opacity="0.42" />
        </>
      );
    case "hills":
      return (
        <>
          <path d="M1 11 a4 4 0 0 1 8 0" fill="none" stroke={ink} strokeWidth="1.2" opacity="0.6" />
          <path d="M8 16 a4 4 0 0 1 8 0" fill="none" stroke={ink} strokeWidth="1.2" opacity="0.4" />
        </>
      );
    case "desert":
      return (
        <>
          <circle cx="4" cy="5" r="1.1" fill={ink} opacity="0.55" />
          <circle cx="11" cy="10" r="1.1" fill={ink} opacity="0.45" />
          <circle cx="6" cy="14" r="1.1" fill={ink} opacity="0.35" />
        </>
      );
    case "mountains":
      return (
        <>
          <path d="M2 13 l4 -7 l4 7" fill="none" stroke={ink} strokeWidth="1.3" opacity="0.65" />
          <path d="M9 16 l3 -5 l3 5" fill="none" stroke={ink} strokeWidth="1.3" opacity="0.4" />
        </>
      );
  }
}

function TerrainDefs() {
  return (
    <defs>
      {TERRAIN_TYPES.map((terrain) => (
        <pattern
          key={terrain}
          id={`terrain-${terrain}`}
          width="16"
          height="18"
          patternUnits="userSpaceOnUse"
        >
          <rect width="16" height="18" fill={TERRAIN_FILL[terrain]} />
          {terrainMarks(terrain)}
        </pattern>
      ))}
      <marker
        id="attack-arrowhead"
        viewBox="0 0 10 10"
        refX="8"
        refY="5"
        markerWidth="5"
        markerHeight="5"
        orient="auto-start-reverse"
      >
        <path d="M0 0 L10 5 L0 10 z" fill="#f0b359" />
      </marker>
    </defs>
  );
}

export function HexMap(props: HexMapProps) {
  const { hexes, territories, continents } = props;

  const geometry = useMemo(() => {
    const axialById = new Map(hexes.map((hex) => [hex.id, { q: hex.q, r: hex.r }]));
    const axialsFor = (ids: readonly string[]): Axial[] =>
      ids.map((id) => axialById.get(id)).filter((hex): hex is Axial => hex !== undefined);

    const outlines = new Map(
      territories.map((territory) => [
        territory.id,
        regionOutlinePath(axialsFor(territory.hexIds), HEX_RADIUS),
      ]),
    );
    const hexIdsByTerritory = new Map(territories.map((t) => [t.id, t.hexIds]));
    const continentOutlines = continents.map((continent) => ({
      id: continent.id,
      name: continent.name,
      path: regionOutlinePath(
        axialsFor(continent.territoryIds.flatMap((id) => hexIdsByTerritory.get(id) ?? [])),
        HEX_RADIUS,
      ),
    }));
    const anchors = new Map(
      territories.map((territory) => [territory.id, hexCenter(territory.labelAnchor, HEX_RADIUS)]),
    );
    return {
      outlines,
      continentOutlines,
      anchors,
      viewBox: viewBoxAttribute(hexesViewBox(hexes, HEX_RADIUS)),
    };
  }, [hexes, territories, continents]);

  const routePath = useMemo(() => {
    if (!props.route) return null;
    const from = geometry.anchors.get(props.route.from);
    const to = geometry.anchors.get(props.route.to);
    return from && to ? attackArrowPath(from, to) : null;
  }, [props.route, geometry.anchors]);

  return (
    <div className="hex-map-frame">
      <svg
        className="hex-map"
        viewBox={geometry.viewBox}
        role="group"
        aria-label="Campaign map"
        preserveAspectRatio="xMidYMid meet"
      >
        <TerrainDefs />
        <g transform={`translate(${props.pan.x} ${props.pan.y}) scale(${props.zoom})`}>
          <g className="layer-terrain" aria-hidden="true">
            {hexes.map((hex) => (
              <polygon
                key={hex.id}
                points={hexPolygonPoints(hex, HEX_RADIUS)}
                fill={`url(#terrain-${hex.terrain})`}
              />
            ))}
          </g>

          <g className="layer-ownership" aria-hidden="true">
            {territories.map((territory) => (
              <path
                key={territory.id}
                className="ownership"
                d={geometry.outlines.get(territory.id) ?? ""}
                fill={props.colorOf(territory.ownerId)}
                fillRule="evenodd"
                opacity={territory.ownerId ? 0.44 : 0.12}
              />
            ))}
          </g>

          <g className="layer-continents" aria-hidden="true">
            {geometry.continentOutlines.map((continent) => (
              <g key={continent.id}>
                <path className="continent-edge-outer" d={continent.path} />
                <path className="continent-edge-inner" d={continent.path} />
              </g>
            ))}
          </g>

          <g className="layer-borders" aria-hidden="true">
            {territories.map((territory) => (
              <path
                key={territory.id}
                className="country-edge"
                d={geometry.outlines.get(territory.id) ?? ""}
              />
            ))}
          </g>

          <g className="layer-highlight" aria-hidden="true">
            {territories.map((territory) => {
              const tone = props.toneOf(territory.id);
              if (tone === "idle") return null;
              return (
                <path
                  key={territory.id}
                  className={`highlight ${tone}`}
                  d={geometry.outlines.get(territory.id) ?? ""}
                  fillRule="evenodd"
                  style={{ "--owner": props.colorOf(territory.ownerId) } as CSSProperties}
                />
              );
            })}
          </g>

          {routePath && (
            <g className="layer-route" aria-hidden="true">
              <path className="attack-route" d={routePath} markerEnd="url(#attack-arrowhead)" />
            </g>
          )}

          <g className="layer-labels" aria-hidden="true">
            {territories.map((territory) => {
              const anchor = geometry.anchors.get(territory.id);
              if (!anchor) return null;
              return (
                <g key={territory.id} transform={`translate(${anchor.x} ${anchor.y})`}>
                  <circle
                    className="army-marker"
                    r={HEX_RADIUS * 0.62}
                    style={{ "--owner": props.colorOf(territory.ownerId) } as CSSProperties}
                  />
                  <text className="army-count" y={HEX_RADIUS * 0.22} textAnchor="middle">
                    {territory.armies}
                  </text>
                  <text className="country-label" y={-HEX_RADIUS * 0.82} textAnchor="middle">
                    {territory.name}
                  </text>
                </g>
              );
            })}
          </g>

          <g className="layer-interaction">
            {territories.map((territory) => {
              const tone = props.toneOf(territory.id);
              const selectable = props.actionable.has(territory.id);
              return (
                <path
                  key={territory.id}
                  className={`country-hit ${tone}${selectable ? " selectable" : ""}`}
                  d={geometry.outlines.get(territory.id) ?? ""}
                  fillRule="evenodd"
                  role="button"
                  tabIndex={0}
                  aria-disabled={!selectable}
                  aria-pressed={tone === "selected"}
                  aria-label={countryLabel(territory, props.ownerNameOf(territory.ownerId))}
                  onClick={() => props.onSelect(territory.id)}
                  onFocus={() => props.onFocus(territory.id)}
                  onKeyDown={(event) => {
                    if (event.key !== "Enter" && event.key !== " ") return;
                    event.preventDefault();
                    props.onSelect(territory.id);
                  }}
                  data-focused={props.focusedId === territory.id ? "true" : undefined}
                />
              );
            })}
          </g>
        </g>
      </svg>
      {props.children}
    </div>
  );
}

export function countryLabel(territory: MapTerritory, ownerName: string): string {
  const armies = territory.armies === 1 ? "1 army" : `${territory.armies} armies`;
  return `${territory.name}, ${armies}, held by ${ownerName}`;
}
