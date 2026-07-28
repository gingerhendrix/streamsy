/**
 * The current-turn column: one section per phase, in the order a turn happens
 * (design spec §8.4).
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
 * never reconstructs turn state by paging the bounded move feed. Combat is an
 * interrupt rather than a phase: its card sits above the sections because it can
 * arrive during someone else's turn and outranks whatever phase is in progress.
 */

import type { CSSProperties, ReactNode } from "react";

import type { ProjectedPlayerV2, ProjectedTurnV2 } from "../board/projection-v2.ts";
import type { GamePhaseV2 } from "../domain/aggregate-v2.ts";
import {
  PHASE_LABELS_V2,
  PHASE_ORDER_V2,
  PHASE_STATE_LABELS,
  phaseInstruction,
  phaseState,
  phaseSummary,
  reinforcementEquation,
  reinforcementProgress,
  turnLedger,
  type NameLookup,
  type PhaseState,
} from "./presentation-v2.ts";

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

function PhaseHead(props: { index: number; phase: GamePhaseV2; state: PhaseState; note?: string }) {
  return (
    <span className="phase-head">
      <span className="phase-index" aria-hidden="true">
        {props.index}
      </span>
      <span className="phase-title">
        <b>{PHASE_LABELS_V2[props.phase]}</b>
        {props.note && <small>{props.note}</small>}
      </span>
      <span className="phase-state">{PHASE_STATE_LABELS[props.state]}</span>
    </span>
  );
}

interface PhaseSectionProps {
  phase: GamePhaseV2;
  index: number;
  state: PhaseState;
  instruction: string;
  summary: string;
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

/** The reinforcement pool, explained as an equation rather than a bare number. */
function ReinforcementDetail(props: { turn: ProjectedTurnV2; names: NameLookup }) {
  const { turn, names } = props;
  return (
    <div className="reinforcement-card">
      <b>{reinforcementEquation(turn.reinforcement, names.continent)}</b>
      <small>
        {reinforcementProgress(turn.reinforcementsPlaced, turn.reinforcement.remaining)}
      </small>
      <div className="continent-chips">
        {turn.reinforcement.continents.map((bonus) => (
          <span className="continent-chip" key={bonus.continentId}>
            {names.continent(bonus.continentId)} +{bonus.bonus}
          </span>
        ))}
      </div>
    </div>
  );
}

export interface TurnColumnProps {
  status: "lobby" | "playing" | "finished";
  turn: ProjectedTurnV2 | null;
  phase?: GamePhaseV2;
  activePlayer?: ProjectedPlayerV2;
  names: NameLookup;
  statusLine: string;
  /** Whether the reader is the seat being asked to act this turn. */
  yourTurn: boolean;
  selfId?: string;
  /**
   * The attack card, when there is one. An *open* attack is an interrupt and is
   * ranked above the phases; a resolved throw is evidence and belongs to the attack
   * phase that produced it, which is why it is placed rather than repeated.
   */
  combatCard?: ReactNode;
  combatLive?: boolean;
  /** Controls for the phase in progress, or the closing card once the game ends. */
  controls?: ReactNode;
  /** A turn-level action rather than a phase one, so it sits under the sections. */
  endTurn?: ReactNode;
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
  const attackDetail = props.combatLive ? undefined : props.combatCard;

  return (
    <aside className="turn-column" aria-label="Current turn">
      <div className="column-head" style={{ "--player": activeColor } as CSSProperties}>
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

      {props.combatLive ? props.combatCard : null}

      {playing ? (
        <ol className="phase-list" aria-label="Turn phases">
          {PHASE_ORDER_V2.map((phase, index) => {
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
                detail={
                  phase === "reinforce"
                    ? reinforcementDetail
                    : phase === "attack"
                      ? attackDetail
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

      {props.endTurn}

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
