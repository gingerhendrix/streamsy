import type { ReactNode } from "react";

import type { LegalActionV2 } from "../application/legal-actions-v2.ts";
import type { NameLookup } from "./presentation-v2.ts";

export type PendingReinforcements = ReadonlyMap<string, number>;

export function pendingReinforcementTotal(pending: PendingReinforcements): number {
  let total = 0;
  for (const amount of pending.values()) total += amount;
  return total;
}

/**
 * Apply one map or stepper interaction without exceeding the current canonical
 * reinforcement allowance. Zero-value entries are removed so the panel only
 * lists territories that are actually part of the pending placement.
 */
export function adjustPendingReinforcements(
  pending: PendingReinforcements,
  territoryId: string,
  delta: 1 | -1,
  territoryIds: readonly string[],
  maxArmies: number,
): Map<string, number> {
  const next = new Map(pending);
  if (!territoryIds.includes(territoryId)) return next;

  const current = next.get(territoryId) ?? 0;
  if (delta > 0 && pendingReinforcementTotal(next) >= maxArmies) return next;

  const amount = Math.max(0, current + delta);
  if (amount === 0) next.delete(territoryId);
  else next.set(territoryId, amount);
  return next;
}

interface ReinforcementPlacementProps {
  action: Extract<LegalActionV2, { type: "reinforce" }>;
  names: NameLookup;
  pending: PendingReinforcements;
  busy: boolean;
  onAdjust(territoryId: string, delta: 1 | -1): void;
  onFinish(): void;
}

/** A reviewable local allocation that is committed as one canonical command. */
export function ReinforcementPlacement(props: ReinforcementPlacementProps): ReactNode {
  const total = props.action.maxArmies;
  const placed = pendingReinforcementTotal(props.pending);
  const remaining = Math.max(0, total - placed);
  const territories = props.action.territoryIds.filter(
    (territoryId) => (props.pending.get(territoryId) ?? 0) > 0,
  );

  return (
    <section className="controls-card reinforcement-placement">
      <div className="reinforcement-totals" aria-label="Pending reinforcement allocation">
        <span>
          <b>{total}</b>
          <small>Total</small>
        </span>
        <span>
          <b>{placed}</b>
          <small>Pending</small>
        </span>
        <span>
          <b>{remaining}</b>
          <small>Remaining</small>
        </span>
      </div>

      {territories.length > 0 ? (
        <ul className="reinforcement-list" aria-label="Reinforced territories">
          {territories.map((territoryId) => {
            const name = props.names.territory(territoryId);
            const amount = props.pending.get(territoryId) ?? 0;
            return (
              <li key={territoryId}>
                <span>{name}</span>
                <div
                  className="reinforcement-stepper"
                  role="group"
                  aria-label={`${name} reinforcements`}
                >
                  <button
                    type="button"
                    disabled={props.busy || amount <= 0}
                    aria-label={`Remove one pending reinforcement from ${name}`}
                    onClick={() => props.onAdjust(territoryId, -1)}
                  >
                    −
                  </button>
                  <output aria-label={`${name} pending reinforcements`}>{amount}</output>
                  <button
                    type="button"
                    disabled={props.busy || remaining <= 0}
                    aria-label={`Add one pending reinforcement to ${name}`}
                    onClick={() => props.onAdjust(territoryId, 1)}
                  >
                    +
                  </button>
                </div>
              </li>
            );
          })}
        </ul>
      ) : (
        <p className="muted">Left-click one of your highlighted territories to place an army.</p>
      )}

      <small className="reinforcement-help">
        Right-click a highlighted territory to remove one pending army.
      </small>
      <button
        type="button"
        className="primary finish-reinforcements"
        disabled={props.busy || remaining !== 0 || placed === 0}
        onClick={props.onFinish}
      >
        {props.busy ? "Finishing…" : "Finish reinforcements"}
      </button>
    </section>
  );
}
