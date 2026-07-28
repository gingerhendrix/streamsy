/**
 * The thin match bar: round, seat, phase.
 *
 * One line, always at the top, answering the three questions that frame every other
 * thing on the screen — *which round is this, whose turn is it, and what phase are
 * they on*. It is deliberately the only place those three facts appear together, so
 * the current-turn column can be about the decision and the status column about the
 * standings.
 *
 * The compact sync pill is passed in rather than built here: it is the one piece of
 * architectural status the game surface keeps.
 */

import type { CSSProperties, ReactNode } from "react";

import type { ProjectedPlayer } from "../board/projection.ts";
import type { GamePhase } from "../domain/aggregate.ts";
import { PHASE_LABELS } from "./presentation.ts";

export interface MatchBarProps {
  gameId: string;
  round: number;
  activePlayer?: ProjectedPlayer;
  phase?: GamePhase;
  /** Shown in place of the seat and phase once the game has a winner. */
  finished?: boolean;
  winnerName?: string;
  selfId?: string;
  children?: ReactNode;
}

export function MatchBar(props: MatchBarProps) {
  const activeColor = props.activePlayer?.color ?? "#65dfb4";
  const yours = props.activePlayer !== undefined && props.activePlayer.id === props.selfId;

  return (
    <header className="match-bar">
      <a className="match-brand" href="/" aria-label="Hex Domination home">
        <span className="brand-mark">S</span>
        <span className="match-game">
          Hex Domination <code>{props.gameId}</code>
        </span>
      </a>

      <div className="match-state" style={{ "--player": activeColor } as CSSProperties}>
        <span className="match-fact">
          <small>Round</small>
          <b>{props.round || "—"}</b>
        </span>
        <span className="match-divider" aria-hidden="true" />
        {props.finished ? (
          <span className="match-fact wide">
            <small>Result</small>
            <b>{props.winnerName ? `${props.winnerName} wins the map` : "Game over"}</b>
          </span>
        ) : (
          <>
            <span className="match-fact wide">
              <small>Turn</small>
              <b>
                <span className="player-color" style={{ background: activeColor }} />
                {props.activePlayer ? props.activePlayer.name : "Waiting"}
                {yours ? " (you)" : ""}
              </b>
            </span>
            <span className="match-divider" aria-hidden="true" />
            <span className="match-fact">
              <small>Phase</small>
              <b className="match-phase">{props.phase ? PHASE_LABELS[props.phase] : "—"}</b>
            </span>
          </>
        )}
      </div>

      <div className="match-right">{props.children}</div>
    </header>
  );
}
