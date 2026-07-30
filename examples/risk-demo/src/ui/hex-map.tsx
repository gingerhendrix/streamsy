/**
 * The `Hex Domination` map: one responsive SVG, drawn from canonical `(q, r)` tiles.
 *
 * Layers, back to front:
 *
 *   terrain → ownership → continent boundary → country boundary → highlight →
 *   attack route → resolved throw → labels → interaction
 *
 * Terrain is never signalled by colour alone: each type has its own SVG pattern, so
 * the map stays readable under a translucent owner wash and for a player who cannot
 * separate the hues. Adjacency is not drawn at all — hexes touch, and that *is* the
 * explanation, so the surface does not draw abstract route lines.
 *
 * The interaction layer sits on top as transparent per-country paths carrying
 * `role="button"`, an `aria-label`, and a tab stop. Selection is a click or an
 * Enter/Space on a focused country. Hover supplements the territory summary, but
 * selection and legality never depend on it.
 */

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from "react";

import type { ProjectedContinent, ProjectedHex } from "../board/projection.ts";
import type { AttackTrace } from "./attack-trace.ts";
import type { Terrain } from "../domain/map.ts";
import { TERRAIN_TYPES } from "../domain/map.ts";
import type { Axial } from "../domain/hex.ts";
import { layoutCountryLabels } from "./label-layout.ts";
import {
  clientDeltaToViewBox,
  clientToViewBox,
  panBy,
  pointerDistance,
  wheelZoomFactor,
  zoomAbout,
  type ViewTransform,
} from "./pan-zoom.ts";
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
/** Army badge; also the obstacle no country label may be drawn underneath. */
const BADGE_RADIUS = HEX_RADIUS * 0.62;
/** Matches `.country-label` in the stylesheet, so the layout measures what renders. */
const LABEL_FONT_SIZE = 12;

/** The four public visual states, ordered by `territoryInteractionState`. */
export type TerritoryInteractionState = "normal" | "dimmed" | "hover" | "active";

export interface TerritoryInteractionInput {
  active: boolean;
  hovered: boolean;
  choosing: boolean;
  actionable: boolean;
}

/** Deterministic precedence: active > hover > unavailable while choosing > normal. */
export function territoryInteractionState(
  input: TerritoryInteractionInput,
): TerritoryInteractionState {
  if (input.active) return "active";
  if (input.hovered) return "hover";
  if (input.choosing && !input.actionable) return "dimmed";
  return "normal";
}

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
  hexes: ProjectedHex[];
  territories: MapTerritory[];
  continents: ProjectedContinent[];
  colorOf(playerId: string | undefined): string;
  ownerNameOf(playerId: string | undefined): string;
  stateOf(territoryId: string): TerritoryInteractionState;
  /** Countries a player may act on now; everything else is focusable but inert. */
  actionable: ReadonlySet<string>;
  focusedId: string | null;
  onSelect(territoryId: string): void;
  /** Secondary selection is used to remove one pending reinforcement. */
  onDecrement?(territoryId: string): void;
  pendingReinforcements?: ReadonlyMap<string, number>;
  onFocus(territoryId: string): void;
  /** Pointer hover only supplements detail/visual state; it never changes legality. */
  onHover(territoryId: string | null): void;
  /** Source → target of the attack being composed, or the throw being revealed. */
  route: { from: string; to: string } | null;
  /**
   * The newest resolved throw, drawn as a route and two loss badges that fade on
   * their own. Distinct from `route`, which is the attack still being decided.
   */
  trace?: AttackTrace | null;
  zoom: number;
  pan: Point;
  /** Drag, wheel, and pinch report through here; the buttons set the same state. */
  onView?(next: ViewTransform): void;
  children?: ReactNode;
}

const TERRAIN_FILL: Record<Terrain, string> = {
  plains: "#a3a17a",
  forest: "#6f7955",
  hills: "#9a8963",
  desert: "#b5a276",
  mountains: "#77776b",
};

const TERRAIN_INK: Record<Terrain, string> = {
  plains: "#68694f",
  forest: "#39452d",
  hills: "#5c5139",
  desert: "#756540",
  mountains: "#45463f",
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

export function TerrainDefs() {
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
      {/* A resolved throw gets its own head in map ink: the same shape as a live
          attack, in the colour the surface uses for things that have already
          happened, so the two arrows are never confused for one another. */}
      <marker
        id="throw-arrowhead"
        viewBox="0 0 10 10"
        refX="8"
        refY="5"
        markerWidth="5"
        markerHeight="5"
        orient="auto-start-reverse"
      >
        <path d="M0 0 L10 5 L0 10 z" fill="#20221a" />
      </marker>
      <marker
        id="attack-arrowhead"
        viewBox="0 0 10 10"
        refX="8"
        refY="5"
        markerWidth="5"
        markerHeight="5"
        orient="auto-start-reverse"
      >
        {/* The field manual's signal red — keep in step with `--signal` in styles.css. */}
        <path d="M0 0 L10 5 L0 10 z" fill="#8e2f25" />
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
    // Anchors are canonical; where the *text* sits is not. A procedural map can
    // put two of them close enough that the names collide, so labels are nudged
    // clear of each other and of every army badge before anything is drawn.
    const labels = layoutCountryLabels(
      territories.map((territory) => ({
        id: territory.id,
        anchor: anchors.get(territory.id)!,
        text: territory.name,
      })),
      {
        badgeRadius: BADGE_RADIUS,
        baseOffset: HEX_RADIUS * 0.95,
        fontSize: LABEL_FONT_SIZE,
      },
    );
    const box = hexesViewBox(hexes, HEX_RADIUS);
    return {
      outlines,
      continentOutlines,
      anchors,
      labels,
      box,
      viewBox: viewBoxAttribute(box),
    };
  }, [hexes, territories, continents]);

  // ---- direct manipulation --------------------------------------------------
  // Buttons remain the accessible path; these are the gestures a map is expected
  // to answer to. Everything is computed in viewBox units by `pan-zoom.ts`, so a
  // drag means the same thing at any screen size.
  const svgRef = useRef<SVGSVGElement | null>(null);
  const pointers = useRef(new Map<number, Point>());
  const pinchDistance = useRef<number | null>(null);
  /** A drag that moved must not also count as a click on the country underneath. */
  const dragged = useRef(false);
  const [dragging, setDragging] = useState(false);

  const view: ViewTransform = { zoom: props.zoom, pan: props.pan };
  const viewRef = useRef(view);
  viewRef.current = view;
  const onView = props.onView;

  const rectOf = useCallback(
    () => svgRef.current?.getBoundingClientRect() ?? { left: 0, top: 0, width: 0, height: 0 },
    [],
  );

  const onPointerDown = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      if (!onView || event.button !== 0) return;
      pointers.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
      // Capture is claimed only once a drag really starts (see `onPointerMove`).
      // Capturing here would retarget the click that follows a stationary press,
      // and selecting a country is what that press is for.
      if (pointers.current.size === 1) {
        dragged.current = false;
        setDragging(true);
      }
      if (pointers.current.size === 2) {
        const [a, b] = [...pointers.current.values()];
        pinchDistance.current = pointerDistance(a!, b!);
      }
    },
    [onView],
  );

  const onPointerMove = useCallback(
    (event: ReactPointerEvent<SVGSVGElement>) => {
      if (!onView) return;
      const previous = pointers.current.get(event.pointerId);
      if (!previous) return;
      const current = { x: event.clientX, y: event.clientY };
      pointers.current.set(event.pointerId, current);

      if (pointers.current.size >= 2) {
        const [a, b] = [...pointers.current.values()];
        const distance = pointerDistance(a!, b!);
        const previousDistance = pinchDistance.current;
        pinchDistance.current = distance;
        if (!previousDistance || previousDistance === 0) return;
        dragged.current = true;
        const midpoint = { x: (a!.x + b!.x) / 2, y: (a!.y + b!.y) / 2 };
        const focus = clientToViewBox(midpoint, rectOf(), geometry.box);
        onView(zoomAbout(viewRef.current, focus, distance / previousDistance));
        return;
      }

      const delta = { x: current.x - previous.x, y: current.y - previous.y };
      if (!dragged.current && Math.abs(delta.x) + Math.abs(delta.y) > 2) {
        dragged.current = true;
        // Now that this is a drag, keep receiving moves even past the map's edge.
        svgRef.current?.setPointerCapture?.(event.pointerId);
      }
      onView(panBy(viewRef.current, clientDeltaToViewBox(delta, rectOf(), geometry.box)));
    },
    [onView, rectOf, geometry.box],
  );

  const endPointer = useCallback((event: ReactPointerEvent<SVGSVGElement>) => {
    pointers.current.delete(event.pointerId);
    if (pointers.current.size < 2) pinchDistance.current = null;
    if (pointers.current.size === 0) setDragging(false);
  }, []);

  // React attaches `wheel` passively, so the zoom listener is registered natively
  // to keep the page from scrolling under the gesture.
  useEffect(() => {
    const element = svgRef.current;
    if (!element || !onView) return;
    const onWheel = (event: WheelEvent): void => {
      event.preventDefault();
      const focus = clientToViewBox(
        { x: event.clientX, y: event.clientY },
        element.getBoundingClientRect(),
        geometry.box,
      );
      onView(zoomAbout(viewRef.current, focus, wheelZoomFactor(event.deltaY, event.deltaMode)));
    };
    element.addEventListener("wheel", onWheel, { passive: false });
    return () => element.removeEventListener("wheel", onWheel);
  }, [onView, geometry.box]);

  const routePath = useMemo(() => {
    if (!props.route) return null;
    const from = geometry.anchors.get(props.route.from);
    const to = geometry.anchors.get(props.route.to);
    return from && to ? attackArrowPath(from, to) : null;
  }, [props.route, geometry.anchors]);

  // Where the two loss badges sit for the newest resolved throw: along the route,
  // one step in from each end so neither lands on the army counter it belongs to,
  // and nudged off the line itself so the arrow does not strike the figures through.
  const traceGeometry = useMemo(() => {
    const trace = props.trace;
    if (!trace) return null;
    const from = geometry.anchors.get(trace.from);
    const to = geometry.anchors.get(trace.to);
    if (!from || !to) return null;
    const span = Math.hypot(to.x - from.x, to.y - from.y);
    if (span === 0) return null;
    const along = { x: (to.x - from.x) / span, y: (to.y - from.y) / span };
    const across = { x: -along.y, y: along.x };
    const inset = Math.min(BADGE_RADIUS * 1.9, span / 2);
    const lift = BADGE_RADIUS * 1.05;
    return {
      path: attackArrowPath(from, to),
      attacker: {
        x: from.x + along.x * inset + across.x * lift,
        y: from.y + along.y * inset + across.y * lift,
      },
      defender: {
        x: to.x - along.x * inset + across.x * lift,
        y: to.y - along.y * inset + across.y * lift,
      },
    };
  }, [props.trace, geometry.anchors]);

  return (
    <div className="hex-map-frame">
      <svg
        ref={svgRef}
        className={`hex-map${dragging ? " dragging" : ""}`}
        viewBox={geometry.viewBox}
        role="group"
        aria-label="Campaign map"
        preserveAspectRatio="xMidYMid meet"
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={endPointer}
        onPointerCancel={endPointer}
        onClickCapture={(event) => {
          // Swallow the click that ends a drag; a stationary press still selects.
          if (!dragged.current) return;
          dragged.current = false;
          event.stopPropagation();
        }}
      >
        <TerrainDefs />
        <g transform={`translate(${props.pan.x} ${props.pan.y}) scale(${props.zoom})`}>
          <g className="layer-terrain" aria-hidden="true">
            {hexes.map((hex) => (
              <polygon
                key={hex.id}
                className={`territory-state-${props.stateOf(hex.territoryId)}`}
                points={hexPolygonPoints(hex, HEX_RADIUS)}
                fill={`url(#terrain-${hex.terrain})`}
              />
            ))}
          </g>

          <g className="layer-ownership" aria-hidden="true">
            {territories.map((territory) => (
              <path
                key={territory.id}
                className={`ownership territory-state-${props.stateOf(territory.id)}`}
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
                className={`country-edge territory-state-${props.stateOf(territory.id)}`}
                d={geometry.outlines.get(territory.id) ?? ""}
              />
            ))}
          </g>

          <g className="layer-highlight" aria-hidden="true">
            {territories.map((territory) => {
              const state = props.stateOf(territory.id);
              if (state === "normal" || state === "dimmed") return null;
              return (
                <path
                  key={territory.id}
                  className={`highlight ${state}`}
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

          {/* The throw that just happened. Keyed by `attackId` so a new throw
              remounts the group and restarts the fade — the fade is presentation
              only, and the trace itself is read from recorded losses. */}
          {props.trace && traceGeometry && (
            <g className="layer-throw" key={props.trace.attackId} aria-hidden="true">
              {props.trace.captured && (
                <path
                  className="capture-flash"
                  d={geometry.outlines.get(props.trace.to) ?? ""}
                  fillRule="evenodd"
                />
              )}
              <path
                className={`throw-route${props.trace.captured ? " captured" : ""}`}
                d={traceGeometry.path}
                markerEnd="url(#throw-arrowhead)"
              />
              {props.trace.attackerLosses > 0 && (
                <LossBadge point={traceGeometry.attacker} losses={props.trace.attackerLosses} />
              )}
              {props.trace.defenderLosses > 0 && (
                <LossBadge point={traceGeometry.defender} losses={props.trace.defenderLosses} />
              )}
            </g>
          )}

          <g className="layer-labels" aria-hidden="true">
            {territories.map((territory, index) => {
              const anchor = geometry.anchors.get(territory.id);
              const label = geometry.labels[index];
              if (!anchor || !label) return null;
              return (
                <g
                  key={territory.id}
                  className={`territory-labels territory-state-${props.stateOf(territory.id)}`}
                >
                  <circle
                    className="army-marker"
                    cx={anchor.x}
                    cy={anchor.y}
                    r={BADGE_RADIUS}
                    style={{ "--owner": props.colorOf(territory.ownerId) } as CSSProperties}
                  />
                  <text
                    className="army-count"
                    x={anchor.x}
                    y={anchor.y + HEX_RADIUS * 0.22}
                    textAnchor="middle"
                  >
                    {territory.armies}
                  </text>
                  {(props.pendingReinforcements?.get(territory.id) ?? 0) > 0 && (
                    <text
                      className="pending-army-count"
                      x={anchor.x + BADGE_RADIUS + 5}
                      y={anchor.y - BADGE_RADIUS * 0.55}
                      textAnchor="start"
                    >
                      +{props.pendingReinforcements?.get(territory.id)}
                    </text>
                  )}
                  {label.leader && (
                    <line
                      className="label-leader"
                      x1={anchor.x}
                      y1={anchor.y}
                      x2={label.x}
                      y2={label.y + (label.y > anchor.y ? -label.height / 2 : label.height / 2)}
                    />
                  )}
                  <text
                    className="country-label"
                    x={label.x}
                    y={label.y}
                    textAnchor="middle"
                    dominantBaseline="central"
                  >
                    {territory.name}
                  </text>
                </g>
              );
            })}
          </g>

          <g className="layer-interaction">
            {territories.map((territory) => {
              const state = props.stateOf(territory.id);
              const selectable = props.actionable.has(territory.id);
              return (
                <path
                  key={territory.id}
                  className={`country-hit ${state}${selectable ? " selectable" : ""}`}
                  d={geometry.outlines.get(territory.id) ?? ""}
                  fillRule="evenodd"
                  role="button"
                  tabIndex={0}
                  aria-disabled={!selectable}
                  aria-pressed={state === "active"}
                  aria-label={countryLabel(
                    territory,
                    props.ownerNameOf(territory.ownerId),
                    props.pendingReinforcements?.get(territory.id) ?? 0,
                  )}
                  onClick={() => props.onSelect(territory.id)}
                  onContextMenu={(event) => {
                    if (!selectable || !props.onDecrement) return;
                    event.preventDefault();
                    props.onDecrement(territory.id);
                  }}
                  onFocus={() => props.onFocus(territory.id)}
                  onPointerEnter={() => props.onHover(territory.id)}
                  onPointerLeave={() => props.onHover(null)}
                  onKeyDown={(event) => {
                    if (event.key !== "Enter" && event.key !== " ") return;
                    event.preventDefault();
                    props.onSelect(territory.id);
                  }}
                  data-focused={props.focusedId === territory.id ? "true" : undefined}
                  data-state={state}
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

/** Armies lost by one side of a throw, as a signed figure on its own roundel. */
function LossBadge(props: { point: Point; losses: number }) {
  return (
    <g className="loss-badge">
      <circle cx={props.point.x} cy={props.point.y} r={BADGE_RADIUS * 0.72} />
      <text x={props.point.x} y={props.point.y + BADGE_RADIUS * 0.2} textAnchor="middle">
        {`−${props.losses}`}
      </text>
    </g>
  );
}

export function countryLabel(
  territory: MapTerritory,
  ownerName: string,
  pendingReinforcements = 0,
): string {
  const armies = territory.armies === 1 ? "1 army" : `${territory.armies} armies`;
  const pending =
    pendingReinforcements > 0
      ? `, plus ${pendingReinforcements} pending ${pendingReinforcements === 1 ? "reinforcement" : "reinforcements"}`
      : "";
  return `${territory.name}, ${armies}${pending}, held by ${ownerName}`;
}
