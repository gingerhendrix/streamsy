/* oxlint-disable typescript/no-unsafe-type-assertion, typescript/consistent-return, typescript/no-unnecessary-type-conversion, unicorn/consistent-function-scoping, effecttsgo/extends-native-error -- Remaining assertions are confined to caller-owned generic codecs, framework-generated structural types, or test-owned fixtures; native errors are synchronous Promise/domain exceptions rather than Effect failure-channel values, and exhaustive switches are protected by closed unions. */
/**
 * The status column: who is winning, which continents are contested, and what has
 * already happened.
 *
 * This is the reference half of the screen. Nothing here is a control — it answers
 * the questions a player asks *between* decisions ("am I ahead?", "is that continent
 * about to pay someone a bonus?", "what did I miss while I was away?") so the
 * current-turn column can stay purely about the decision in front of them.
 *
 * Army and country totals are read from the projection's derived player counters,
 * and continent standings are counted from the territory rows, so the panel never
 * disagrees with the map beside it.
 */

import type { CSSProperties, ReactNode } from "react";

import type {
  ProjectedContinent,
  ProjectedMove,
  ProjectedPlayer,
  ProjectedTerritory,
} from "../board/projection.ts";
import {
  armyShare,
  continentOccupationLabel,
  continentStandings,
  controllerLabel,
  moveDetail,
  moveText,
  playerStrengthLabel,
  type NameLookup,
} from "./presentation.ts";

/** How many past moves the feed keeps on screen; the stream itself is unbounded. */
const HISTORY_LIMIT = 14;

function PlayerStandings(props: {
  players: ProjectedPlayer[];
  activePlayerId?: string;
  selfId?: string;
}) {
  return (
    <section className="panel standings-panel" aria-label="Players">
      <div className="panel-heading">
        <h2>Armies</h2>
        <span>{props.players.length} seats</span>
      </div>
      <ul className="standings-list">
        {props.players.map((player) => {
          const controller = controllerLabel(player);
          const classes = [
            "standing",
            player.id === props.activePlayerId ? "active" : "",
            player.eliminated ? "eliminated" : "",
          ]
            .filter(Boolean)
            .join(" ");
          return (
            <li
              key={player.id}
              className={classes}
              style={{ "--player": player.color } as CSSProperties}
            >
              <span className="standing-top">
                <span className="player-color" style={{ background: player.color }} />
                <b>
                  {player.name}
                  {player.id === props.selfId ? " (you)" : ""}
                </b>
                {controller && <span className="controller-tag">{controller}</span>}
                {player.eliminated && <span className="controller-tag out">Out</span>}
              </span>
              <span className="standing-strength">{playerStrengthLabel(player)}</span>
              <span className="strength-track" aria-hidden="true">
                <span
                  className="strength-fill"
                  style={{ width: `${Math.round(armyShare(player, props.players) * 100)}%` }}
                />
              </span>
            </li>
          );
        })}
      </ul>
    </section>
  );
}

function ContinentStandings(props: {
  continents: ProjectedContinent[];
  territories: ProjectedTerritory[];
  names: NameLookup;
  colorOf: (playerId: string | undefined) => string;
}) {
  const standings = continentStandings(props.continents, props.territories);
  return (
    <section className="panel continents-panel" aria-label="Continents">
      <div className="panel-heading">
        <h2>Continents</h2>
        <span>bonus per turn</span>
      </div>
      <ul className="continent-list">
        {standings.map((standing) => (
          <li
            key={standing.continentId}
            className={standing.controllerId ? "continent-row held" : "continent-row"}
            style={{ "--player": props.colorOf(standing.controllerId) } as CSSProperties}
          >
            <span className="continent-top">
              <b>{standing.name}</b>
              <span className="continent-bonus">+{standing.bonus}</span>
            </span>
            <span
              className="continent-occupation"
              role="img"
              aria-label={continentOccupationLabel(standing, props.names)}
            >
              {standing.occupations.map((occupation) => (
                <span
                  aria-hidden="true"
                  className={
                    occupation.ownerId ? "occupation-square" : "occupation-square unclaimed"
                  }
                  data-territory-id={occupation.territoryId}
                  key={occupation.territoryId}
                  style={{ "--occupant": props.colorOf(occupation.ownerId) } as CSSProperties}
                />
              ))}
            </span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function GameHistory(props: { moves: ProjectedMove[]; names: NameLookup }) {
  return (
    <section className="panel feed-panel" aria-label="Game history">
      <div className="panel-heading">
        <h2>History</h2>
        <span>most recent first</span>
      </div>
      <div className="event-feed">
        {props.moves.length === 0 ? (
          <p className="muted">Moves will appear here as they commit.</p>
        ) : (
          props.moves.slice(0, HISTORY_LIMIT).map((move) => {
            const battle = move.kind === "AttackResolved" || move.kind === "AttackDeclared";
            const detail = moveDetail(move, props.names);
            return (
              <article className="event-item" key={move.id}>
                <span className={`event-icon ${battle ? "battle" : ""}`}>{battle ? "⚔" : "◆"}</span>
                <div>
                  <b>{moveText(move, props.names)}</b>
                  {detail && <small>{detail}</small>}
                </div>
              </article>
            );
          })
        )}
      </div>
    </section>
  );
}

export interface StatusColumnProps {
  players: ProjectedPlayer[];
  continents: ProjectedContinent[];
  territories: ProjectedTerritory[];
  moves: ProjectedMove[];
  names: NameLookup;
  colorOf: (playerId: string | undefined) => string;
  activePlayerId?: string;
  selfId?: string;
  footer?: ReactNode;
}

export function StatusColumn(props: StatusColumnProps) {
  return (
    <aside className="status-column" aria-label="Game status">
      <PlayerStandings
        players={props.players}
        activePlayerId={props.activePlayerId}
        selfId={props.selfId}
      />
      <ContinentStandings
        continents={props.continents}
        territories={props.territories}
        names={props.names}
        colorOf={props.colorOf}
      />
      <GameHistory moves={props.moves} names={props.names} />
      {props.footer}
    </aside>
  );
}
