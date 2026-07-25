/**
 * The persistent current-turn rail and the combat/dice experience (design spec
 * §8.4–8.5).
 *
 * The rail is the authoritative visual answer to "what is happening now?": round,
 * seat, phase, the reinforcement pool explained as an equation, the live combat
 * card, and this turn's ledger — all read from the projection's `turn` and `combat`
 * rows rather than reconstructed by paging the bounded move feed.
 *
 * Every die face on this surface comes from a recorded event value. There is no
 * code path that produces a placeholder face: an unresolved defence renders a
 * face-*down* silhouette, and motion only ever shakes a cup around values that were
 * already rolled. The countdown likewise displays the canonical
 * `defenseDeadlineAt` — it never decides when the window closes.
 */

import type { CSSProperties, ReactNode } from "react";
import type { PlayerController } from "../domain/events-v2.ts";

import type { ProjectedPlayerV2, ProjectedTurnV2 } from "../board/projection-v2.ts";
import type { GamePhaseV2 } from "../domain/aggregate-v2.ts";
import type { CombatView } from "./combat-view.ts";
import {
  countdownFraction,
  countdownLabel,
  dicePairs,
  reinforcementEquation,
  reinforcementProgress,
  resolutionLabel,
  turnLedger,
  type NameLookup,
  type RevealPlan,
  type SeatMode,
} from "./presentation-v2.ts";

const PHASES: GamePhaseV2[] = ["reinforce", "attack", "fortify"];
const PHASE_LABELS: Record<GamePhaseV2, string> = {
  reinforce: "Reinforce",
  attack: "Attack",
  fortify: "Fortify",
};

/** Pip positions on a 100×100 die face, by value. */
const PIPS: Record<number, Array<[number, number]>> = {
  1: [[50, 50]],
  2: [
    [30, 30],
    [70, 70],
  ],
  3: [
    [28, 28],
    [50, 50],
    [72, 72],
  ],
  4: [
    [30, 30],
    [70, 30],
    [30, 70],
    [70, 70],
  ],
  5: [
    [30, 30],
    [70, 30],
    [50, 50],
    [30, 70],
    [70, 70],
  ],
  6: [
    [30, 26],
    [70, 26],
    [30, 50],
    [70, 50],
    [30, 74],
    [70, 74],
  ],
};

export function Die(props: {
  /** `null` renders a face-down silhouette — never an invented face. */
  value: number | null;
  side: "attacker" | "defender";
  color: string;
  outcome?: "won" | "lost" | null;
  reveal?: RevealPlan;
}) {
  const style = {
    "--die": props.color,
    "--reveal-ms": `${props.reveal?.durationMs ?? 0}ms`,
  } as CSSProperties;
  const classes = [
    "die",
    props.side,
    props.value === null ? "facedown" : "revealed",
    props.outcome ? props.outcome : "",
    props.reveal && props.reveal.mode !== "none" ? props.reveal.mode : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <span
      className={classes}
      style={style}
      role="img"
      aria-label={props.value === null ? "Face-down die" : `Rolled ${props.value}`}
    >
      <svg viewBox="0 0 100 100" aria-hidden="true">
        <rect x="4" y="4" width="92" height="92" rx="20" className="die-body" />
        {props.value === null ? (
          <path d="M34 34 L66 66 M66 34 L34 66" className="die-hidden-mark" />
        ) : (
          (PIPS[props.value] ?? []).map(([cx, cy], index) => (
            <circle key={index} cx={cx} cy={cy} r="9" className="die-pip" />
          ))
        )}
      </svg>
    </span>
  );
}

function Countdown(props: { deadlineAt: number; now: number; windowMs: number }) {
  const fraction = countdownFraction(props.deadlineAt, props.now, props.windowMs);
  return (
    <div className="countdown" role="timer" aria-label="Defence window">
      <div className="countdown-track">
        <div className="countdown-fill" style={{ width: `${Math.round(fraction * 100)}%` }} />
      </div>
      <b>{countdownLabel(props.deadlineAt, props.now)}</b>
    </div>
  );
}

export interface CombatCardProps {
  combat: CombatView;
  names: NameLookup;
  colorOf(playerId: string | undefined): string;
  controllerOf(playerId: string | undefined): PlayerController | undefined;
  selfId?: string;
  mode: SeatMode | null;
  now: number;
  defenseWindowMs: number;
  reveal: RevealPlan;
  busy: boolean;
  onRollDefense(): void;
}

export function CombatCard(props: CombatCardProps) {
  const { combat, names } = props;
  const attackerColor = props.colorOf(combat.attackerId);
  const defenderColor = props.colorOf(combat.defenderId);
  const defenderName = names.player(combat.defenderId);
  const pending = combat.status === "awaiting-defense";
  const pairs = combat.defenderRolls ? dicePairs(combat.attackerRolls, combat.defenderRolls) : null;
  const isDefender = props.mode === "defense" && combat.defenderId === props.selfId;
  const defenderController = props.controllerOf(combat.defenderId);

  return (
    <section className="combat-card" aria-live="polite">
      <div className="combat-head">
        <span className="section-label">
          {pending
            ? "Attack declared"
            : combat.status === "awaiting-occupation"
              ? "Country captured"
              : "Throw resolved"}
        </span>
        <h3>
          {names.territory(combat.from)} <span aria-hidden="true">→</span>{" "}
          {names.territory(combat.to)}
        </h3>
      </div>

      <div className="dice-rows">
        <div className="dice-row">
          <span className="dice-side" style={{ "--player": attackerColor } as CSSProperties}>
            {names.player(combat.attackerId)}
            <small>attacking</small>
          </span>
          <span className="dice-set">
            {combat.attackerRolls
              .toSorted((a, b) => b - a)
              .map((value, index) => (
                <Die
                  key={`a${index}`}
                  value={value}
                  side="attacker"
                  color={attackerColor}
                  outcome={
                    pairs?.[index] ? (pairs[index]!.loser === "attacker" ? "lost" : "won") : null
                  }
                  reveal={props.reveal}
                />
              ))}
          </span>
        </div>

        <div className="dice-row">
          <span className="dice-side" style={{ "--player": defenderColor } as CSSProperties}>
            {defenderName}
            <small>defending {names.territory(combat.to)}</small>
          </span>
          <span className="dice-set">
            {combat.defenderRolls
              ? combat.defenderRolls
                  .toSorted((a, b) => b - a)
                  .map((value, index) => (
                    <Die
                      key={`d${index}`}
                      value={value}
                      side="defender"
                      color={defenderColor}
                      outcome={
                        pairs?.[index]
                          ? pairs[index]!.loser === "defender"
                            ? "lost"
                            : "won"
                          : null
                      }
                      reveal={props.reveal}
                    />
                  ))
              : Array.from({ length: combat.defenderDice }, (_, index) => (
                  <Die key={`h${index}`} value={null} side="defender" color={defenderColor} />
                ))}
          </span>
        </div>
      </div>

      {pending && combat.defenseDeadlineAt !== undefined && (
        <div className="defense-prompt">
          <Countdown
            deadlineAt={combat.defenseDeadlineAt}
            now={props.now}
            windowMs={props.defenseWindowMs}
          />
          {isDefender ? (
            <button
              className="primary roll-defense"
              disabled={props.busy}
              onClick={props.onRollDefense}
            >
              Roll defence · {combat.defenderDice} {combat.defenderDice === 1 ? "die" : "dice"}
            </button>
          ) : (
            <p className="muted">
              {defenderController === "external-agent"
                ? "Agent is deciding…"
                : defenderController === "bot"
                  ? "Bot is rolling…"
                  : `Waiting for ${defenderName} to roll…`}
            </p>
          )}
        </div>
      )}

      {pairs && (
        <div className="combat-outcome">
          <p>
            {combat.territoryCaptured
              ? `${names.territory(combat.to)} falls`
              : `${names.territory(combat.to)} holds`}
            {" · "}
            {combat.attackerLosses ?? 0} attacker / {combat.defenderLosses ?? 0} defender lost
          </p>
          {combat.resolutionSource && (
            <small>{resolutionLabel(combat.resolutionSource, defenderName)}</small>
          )}
        </div>
      )}
    </section>
  );
}

export interface TurnRailProps {
  round: number;
  status: "lobby" | "playing" | "finished";
  turn: ProjectedTurnV2 | null;
  phase?: GamePhaseV2;
  activePlayer?: ProjectedPlayerV2;
  names: NameLookup;
  statusLine: string;
  selfId?: string;
  /** The phase controls for this seat, or nothing for a spectator. */
  controls?: ReactNode;
  combatCard?: ReactNode;
  history?: ReactNode;
  footer?: ReactNode;
}

/**
 * What the rail shows once the map has an owner.
 *
 * A finished game has no legal action for anybody, so the ordinary "waiting for
 * another player" copy would be a lie. This says who won and makes it explicit
 * that the board below is the final one, not a stale view.
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

export function TurnRail(props: TurnRailProps) {
  const { turn, names } = props;
  const activeColor = props.activePlayer?.color ?? "#65dfb4";

  return (
    <aside className="turn-rail" aria-label="Current turn">
      <div className="rail-head" style={{ "--player": activeColor } as CSSProperties}>
        <span className="eyebrow">Round {props.round || "—"}</span>
        <h2>{props.statusLine}</h2>
        {props.activePlayer && (
          <p className="rail-active">
            <span className="player-color" style={{ background: activeColor }} />
            {props.activePlayer.name}
            {props.activePlayer.id === props.selfId ? " (you)" : ""}
          </p>
        )}
      </div>

      {props.status === "playing" && (
        <ol className="phase-stepper" aria-label="Turn phases">
          {PHASES.map((phase) => (
            <li
              key={phase}
              className={
                phase === props.phase
                  ? "current"
                  : PHASES.indexOf(phase) < PHASES.indexOf(props.phase ?? "reinforce")
                    ? "done"
                    : ""
              }
              aria-current={phase === props.phase ? "step" : undefined}
            >
              {PHASE_LABELS[phase]}
            </li>
          ))}
        </ol>
      )}

      {turn && turn.reinforcement.total > 0 && (
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
      )}

      {props.combatCard}
      {props.controls}

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

      {props.history}
      {props.footer}
    </aside>
  );
}
