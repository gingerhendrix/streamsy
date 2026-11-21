/**
 * The combat/dice experience: the live attack card and its defence prompt (design
 * spec §8.5).
 *
 * Every die face on this surface comes from a recorded event value. There is no
 * code path that produces a placeholder face: an unresolved defence renders a
 * face-*down* silhouette, and motion only ever shakes a cup around values that were
 * already rolled. The countdown likewise displays the canonical
 * `defenseDeadlineAt` — it never decides when the window closes.
 *
 * Combat remains inside the Attack phase so its declaration, dice, and repeat
 * controls retain the turn context that produced them.
 */

import { useState } from "react";
import type { PlayerController } from "../domain/events.ts";

import type { CombatView } from "./combat-view.ts";
import {
  countdownFraction,
  countdownLabel,
  dicePairs,
  resolutionLabel,
  type NameLookup,
  type RevealPlan,
  type SeatMode,
} from "./presentation.ts";
import { customStyle } from "./shared.tsx";

/** Pip positions on a 100×100 die face, by value. */
const PIPS = new Map<number, Array<[number, number]>>([
  [1, [[50, 50]]],
  [
    2,
    [
      [30, 30],
      [70, 70],
    ],
  ],
  [
    3,
    [
      [28, 28],
      [50, 50],
      [72, 72],
    ],
  ],
  [
    4,
    [
      [30, 30],
      [70, 30],
      [30, 70],
      [70, 70],
    ],
  ],
  [
    5,
    [
      [30, 30],
      [70, 30],
      [50, 50],
      [30, 70],
      [70, 70],
    ],
  ],
  [
    6,
    [
      [30, 26],
      [70, 26],
      [30, 50],
      [70, 50],
      [30, 74],
      [70, 74],
    ],
  ],
]);

export function Die(props: {
  /** `null` renders a face-down silhouette — never an invented face. */
  value: number | null;
  side: "attacker" | "defender";
  color: string;
  outcome?: "won" | "lost" | null;
  reveal?: RevealPlan;
}) {
  const style = customStyle({
    "--die": props.color,
    "--reveal-ms": `${props.reveal?.durationMs ?? 0}ms`,
  });
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
          (PIPS.get(props.value) ?? []).map(([cx, cy], index) => (
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
  colorOf: (playerId: string | undefined) => string;
  controllerOf: (playerId: string | undefined) => PlayerController | undefined;
  selfId?: string;
  mode: SeatMode | null;
  now: number;
  defenseWindowMs: number;
  reveal: RevealPlan;
  busy: boolean;
  onRollDefense: () => void;
  attackAgain?: {
    maxAttackerDice: number;
    onSubmit: (attackerDice: number) => void;
  };
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
  const repeatMax = props.attackAgain?.maxAttackerDice ?? 1;
  const [repeatSelection, setRepeatSelection] = useState<{
    attackId: string;
    attackerDice: number;
  } | null>(null);
  const requestedRepeatDice =
    repeatSelection?.attackId === combat.attackId
      ? repeatSelection.attackerDice
      : combat.attackerDice;
  const boundedRepeatDice = Math.max(1, Math.min(requestedRepeatDice, repeatMax));

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
          <span className="dice-side" style={customStyle({ "--player": attackerColor })}>
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
                    pairs?.[index] ? (pairs[index].loser === "attacker" ? "lost" : "won") : null
                  }
                  reveal={props.reveal}
                />
              ))}
          </span>
        </div>

        <div className="dice-row">
          <span className="dice-side" style={customStyle({ "--player": defenderColor })}>
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
                        pairs?.[index] ? (pairs[index].loser === "defender" ? "lost" : "won") : null
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
          {props.attackAgain && (
            <div className="attack-again">
              <div className="stepper" role="group" aria-label="Attacking troops">
                <button
                  onClick={() =>
                    setRepeatSelection({
                      attackId: combat.attackId,
                      attackerDice: Math.max(1, boundedRepeatDice - 1),
                    })
                  }
                  disabled={props.busy || boundedRepeatDice <= 1}
                  aria-label="Attacking troops: one fewer"
                >
                  −
                </button>
                <b aria-live="polite">{boundedRepeatDice}</b>
                <button
                  onClick={() =>
                    setRepeatSelection({
                      attackId: combat.attackId,
                      attackerDice: Math.min(repeatMax, boundedRepeatDice + 1),
                    })
                  }
                  disabled={props.busy || boundedRepeatDice >= repeatMax}
                  aria-label="Attacking troops: one more"
                >
                  +
                </button>
                <small>1–{repeatMax}</small>
              </div>
              <button
                className="primary"
                disabled={props.busy}
                onClick={() => props.attackAgain?.onSubmit(boundedRepeatDice)}
              >
                Attack again with {boundedRepeatDice}
              </button>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
