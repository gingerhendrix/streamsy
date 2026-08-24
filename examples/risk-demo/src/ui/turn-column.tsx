/**
 * The current-turn column: one section per phase, in the order a turn happens.
 *
 * The information architecture is the point of this file. A turn is three phases,
 * so the column is three sections and never a scroll of undifferentiated cards:
 *
 *  - the **active** phase is the only one that carries instructions and controls;
 *  - **completed** phases collapse to what they achieved, expandable for the detail;
 *  - **upcoming** phases stay visible but inert, so the shape of a whole turn — and
 *    what is about to be asked — is legible before it is your problem.
 *
 * Everything the sections read comes from the projection's `turn` row, so the column
 * never reconstructs turn state by paging the bounded move feed. Combat belongs to
 * Attack: both an open declaration and its resolved dice stay inside that phase.
 */

import type { ReactNode } from "react";

import type { ProjectedPlayer, ProjectedTurn } from "../board/projection.ts";
import type { GamePhase } from "../domain/aggregate.ts";
import {
  PHASE_LABELS,
  PHASE_ORDER,
  PHASE_STATE_LABELS,
  phaseInstruction,
  phaseState,
  phaseSummary,
  reinforcementEquation,
  turnLedger,
  type NameLookup,
  type PhaseState,
} from "./presentation.ts";
import { customStyle } from "./shared.tsx";

/**
 * What the column shows once the map has an owner.
 *
 * A finished game has no legal action for anybody, so the ordinary "waiting for
 * another player" copy would be a lie. This says who won and makes it explicit
 * that the board beside it is the final one, not a stale view.
 */
export function VictoryCard(props: { winnerName?: string; round: number }) {
  return (
    <section className="controls-card victory">
      <span className="section-label">Game over</span>
      <h3>{props.winnerName ? `${props.winnerName} conquered the map` : "The campaign is over"}</h3>
      <p>
        {props.round} rounds played. The final board stays live — every country, every recorded die,
        and the whole history remain readable.
      </p>
    </section>
  );
}

function PhaseHead(props: { index: number; phase: GamePhase; state: PhaseState; note?: string }) {
  return (
    <span className="phase-head">
      <span className="phase-index" aria-hidden="true">
        {props.index}
      </span>
      <span className="phase-title">
        <b>{PHASE_LABELS[props.phase]}</b>
        {props.note && <small>{props.note}</small>}
      </span>
      <span className="phase-state">{PHASE_STATE_LABELS[props.state]}</span>
    </span>
  );
}

interface PhaseSectionProps {
  phase: GamePhase;
  index: number;
  state: PhaseState;
  instruction: string;
  summary: string;
  /** A compact phase-level action that belongs before the instruction. */
  action?: ReactNode;
  /** Phase-specific evidence: the reinforcement pool, the last throw, and so on. */
  detail?: ReactNode;
  /** The controls for this phase, rendered only while it is active. */
  children?: ReactNode;
}

function PhaseSection(props: PhaseSectionProps) {
  const className = `phase-section ${props.state}`;

  if (props.state === "active") {
    return (
      <li className={className} aria-current="step">
        <PhaseHead index={props.index} phase={props.phase} state={props.state} />
        <div className="phase-body">
          {props.action}
          <p className="phase-instruction">{props.instruction}</p>
          {props.detail}
          {props.children}
        </div>
      </li>
    );
  }

  if (props.state === "completed") {
    // Collapsed, but not hidden: the achievement is in the header and the evidence
    // is one disclosure away, which is cheaper than scrolling the history for it.
    return (
      <li className={className}>
        {props.detail ? (
          <details>
            <summary>
              <PhaseHead
                index={props.index}
                phase={props.phase}
                state={props.state}
                note={props.summary}
              />
            </summary>
            <div className="phase-body">{props.detail}</div>
          </details>
        ) : (
          <PhaseHead
            index={props.index}
            phase={props.phase}
            state={props.state}
            note={props.summary}
          />
        )}
      </li>
    );
  }

  return (
    <li className={className} aria-disabled="true">
      <PhaseHead
        index={props.index}
        phase={props.phase}
        state={props.state}
        note={props.instruction}
      />
    </li>
  );
}

/** The reinforcement pool, explained once above the placement controls. */
function ReinforcementDetail(props: { turn: ProjectedTurn; names: NameLookup }) {
  return (
    <div className="reinforcement-card">
      <b>{reinforcementEquation(props.turn.reinforcement, props.names.continent)}</b>
    </div>
  );
}

export interface TurnColumnProps {
  status: "lobby" | "playing" | "finished";
  turn: ProjectedTurn | null;
  phase?: GamePhase;
  activePlayer?: ProjectedPlayer;
  names: NameLookup;
  statusLine: string;
  /** Whether the reader is the seat being asked to act this turn. */
  yourTurn: boolean;
  selfId?: string;
  /** The open declaration or resolved dice, placed inside the Attack phase. */
  combatCard?: ReactNode;
  /** Compact action placed above the active Fortify instruction. */
  fortifyAction?: ReactNode;
  /** Controls for the phase in progress, or the closing card once the game ends. */
  controls?: ReactNode;
  footer?: ReactNode;
}

export function TurnColumn(props: TurnColumnProps) {
  const { turn, names } = props;
  const activeColor = props.activePlayer?.color ?? "#65dfb4";
  const activePlayerName = props.activePlayer?.name ?? "the active player";
  const playing = props.status === "playing" && props.phase !== undefined;

  const reinforcementDetail =
    turn && turn.reinforcement.total > 0 ? (
      <ReinforcementDetail turn={turn} names={names} />
    ) : undefined;
  return (
    <aside className="turn-column" aria-label="Current turn">
      <div className="column-head" style={customStyle({ "--player": activeColor })}>
        <span className="eyebrow">Current turn</span>
        <h2>{props.statusLine}</h2>
        {props.activePlayer && (
          <p className="column-active">
            <span className="player-color" style={{ background: activeColor }} />
            {props.activePlayer.name}
            {props.activePlayer.id === props.selfId ? " (you)" : ""}
          </p>
        )}
      </div>

      {playing ? (
        <ol className="phase-list" aria-label="Turn phases">
          {PHASE_ORDER.map((phase, index) => {
            const state = phaseState(phase, props.phase);
            return (
              <PhaseSection
                key={phase}
                phase={phase}
                index={index + 1}
                state={state}
                instruction={phaseInstruction(phase, {
                  state,
                  yourTurn: props.yourTurn,
                  activePlayerName,
                  fortifyPending:
                    phase === "fortify" && props.phase === "fortify" && turn?.phase === "attack",
                })}
                summary={phaseSummary(phase, turn)}
                action={phase === "fortify" ? props.fortifyAction : undefined}
                detail={
                  phase === "reinforce"
                    ? reinforcementDetail
                    : phase === "attack"
                      ? props.combatCard
                      : undefined
                }
              >
                {state === "active" ? props.controls : null}
              </PhaseSection>
            );
          })}
        </ol>
      ) : (
        props.controls
      )}

      {turn && (
        <section className="ledger" aria-label="This turn">
          <div className="panel-heading">
            <h2>This turn</h2>
            <span>{props.activePlayer?.name ?? ""}</span>
          </div>
          <ol className="ledger-list">
            {turnLedger(turn, names).map((entry) => (
              <li key={entry.id}>
                <span className="ledger-icon" aria-hidden="true">
                  {entry.icon}
                </span>
                <div>
                  <b>{entry.text}</b>
                  {entry.detail && <small>{entry.detail}</small>}
                </div>
              </li>
            ))}
          </ol>
        </section>
      )}

      {props.footer}
    </aside>
  );
}
