/**
 * Durable defence-timeout runtime for `risk-demo-v2` (design spec §4.4, §10).
 *
 * A declared attack opens a defence interrupt with a canonical deadline. If no
 * human or agent closes it, this component does — by submitting the internal
 * `resolve-defense-timeout` command, authorized by the game service rather than
 * by any player capability.
 *
 * The scheduling side is deliberately *at least once*; the canonical effect is
 * exactly once. That split is what makes the protocol recoverable:
 *
 *  - **Stable identity.** A timer is named `defense-timeout:<gameId>:<attackId>`,
 *    so re-scheduling after a failed first attempt is safe, and a duplicate
 *    delivery is indistinguishable from a retry.
 *  - **Canonical state is the schedule.** {@link DefenseTimers.ensure} folds
 *    canonical history and derives what should be scheduled. Nothing about the
 *    timer is itself durable state, which is why a process restart can rebuild
 *    every outstanding timer — or resolve an already-expired one immediately —
 *    from the event log alone.
 *  - **Late is harmless.** Firing refolds first. A timer whose attack has already
 *    been resolved by a human or an agent finds nothing pending and returns
 *    `ATTACK_ALREADY_RESOLVED` without consuming a die; a *stale* timer cannot
 *    resolve a later attack because both `attackId` and `turnId` must match.
 *
 * The wall clock is injected so tests can cross a deadline without waiting, and
 * the scheduler is injected so they need not rely on real `setTimeout` firing.
 */

import type { StreamProtocolFactory } from "@streamsy/core";

import { foldAggregateV2 } from "../../src/domain/aggregate-v2.ts";
import type { PendingInteraction } from "../../src/domain/aggregate-v2.ts";
import type { CommandServiceDeps, SubmitResultV2 } from "./command-service.ts";
import { isRulesetV2, readCanonicalV2, submitCommandV2 } from "./command-service.ts";
import { eventStreamId } from "./names.ts";
import type { GameStore } from "../persistence/stores.ts";

/** Stable timer identity, so scheduling is idempotent across retries/restarts. */
export function defenseTimerId(gameId: string, attackId: string): string {
  return `defense-timeout:${gameId}:${attackId}`;
}

/** Stable command identity, so duplicate delivery dedupes in the command log. */
export function defenseTimeoutCommandId(attackId: string): string {
  return `timeout-defense:${attackId}`;
}

/**
 * The delayed-execution primitive. Production uses `setTimeout`; tests supply a
 * manual implementation and fire timers explicitly.
 */
export interface TimerScheduler {
  schedule(timerId: string, delayMs: number, run: () => void): void;
  cancel(timerId: string): void;
  cancelAll(): void;
}

export function createTimeoutScheduler(): TimerScheduler {
  const timers = new Map<string, ReturnType<typeof setTimeout>>();
  return {
    schedule(timerId, delayMs, run) {
      if (timers.has(timerId)) return; // already scheduled: identity is stable
      const handle = setTimeout(
        () => {
          timers.delete(timerId);
          run();
        },
        Math.max(0, delayMs),
      );
      // Never hold the process open for a game nobody is watching.
      (handle as unknown as { unref?: () => void }).unref?.();
      timers.set(timerId, handle);
    },
    cancel(timerId) {
      const handle = timers.get(timerId);
      if (!handle) return;
      clearTimeout(handle);
      timers.delete(timerId);
    },
    cancelAll() {
      for (const handle of timers.values()) clearTimeout(handle);
      timers.clear();
    },
  };
}

/** A manually-driven scheduler for deterministic tests. */
export interface ManualScheduler extends TimerScheduler {
  /** Timer ids currently scheduled, in insertion order. */
  pending(): string[];
  /** Run one scheduled timer by id, whatever its delay. Returns false if unknown. */
  fire(timerId: string): boolean;
  /** Run every timer whose delay is at or below `elapsedMs`. */
  advance(elapsedMs: number): void;
}

export function createManualScheduler(): ManualScheduler {
  const timers = new Map<string, { delayMs: number; run: () => void }>();
  return {
    schedule(timerId, delayMs, run) {
      if (timers.has(timerId)) return;
      timers.set(timerId, { delayMs, run });
    },
    cancel: (timerId) => void timers.delete(timerId),
    cancelAll: () => timers.clear(),
    pending: () => [...timers.keys()],
    fire(timerId) {
      const timer = timers.get(timerId);
      if (!timer) return false;
      timers.delete(timerId);
      timer.run();
      return true;
    },
    advance(elapsedMs) {
      // Snapshot first: a firing timer may schedule the next one.
      const due = [...timers.entries()].filter(([, timer]) => timer.delayMs <= elapsedMs);
      for (const [timerId, timer] of due) {
        timers.delete(timerId);
        timer.run();
      }
    },
  };
}

export interface DefenseTimerDeps {
  protocol: StreamProtocolFactory;
  commandService: CommandServiceDeps;
  games: GameStore;
  scheduler?: TimerScheduler;
  now?: () => number;
  /** Reported when a fired timer throws, so a demo run surfaces the failure. */
  onError?: (error: unknown, context: { gameId: string; attackId: string }) => void;
}

export interface DefenseTimers {
  /**
   * Reconcile the scheduled timers for one game against canonical state. Safe to
   * call after every accepted command and as often as you like.
   */
  ensure(gameId: string): Promise<void>;
  /**
   * Resolve the pending defence for `attackId` now. Returns `null` when there is
   * nothing to do — the ordinary outcome for a duplicate or stale delivery.
   */
  fire(gameId: string, attackId: string): Promise<SubmitResultV2 | null>;
  /**
   * Restart recovery: walk every known v2 game and rebuild its timer from
   * canonical pending state, resolving deadlines that have already passed.
   */
  recover(): Promise<void>;
  stop(): void;
}

async function pendingDefenseFor(
  deps: DefenseTimerDeps,
  gameId: string,
): Promise<Extract<PendingInteraction, { type: "defense" }> | null> {
  const { events } = await readCanonicalV2(deps.protocol, eventStreamId(gameId));
  if (events.length === 0) return null;
  const pending = foldAggregateV2(events).pendingInteraction;
  return pending?.type === "defense" ? pending : null;
}

export function createDefenseTimers(deps: DefenseTimerDeps): DefenseTimers {
  const scheduler = deps.scheduler ?? createTimeoutScheduler();
  const now = deps.now ?? deps.commandService.now;

  async function fire(gameId: string, attackId: string): Promise<SubmitResultV2 | null> {
    // Refold before consuming anything: the attack may already be closed, and a
    // stale timer must not touch whatever is pending now.
    const pending = await pendingDefenseFor(deps, gameId);
    if (!pending || pending.attackId !== attackId) return null;
    return submitCommandV2(deps.commandService, eventStreamId(gameId), {
      type: "resolve-defense-timeout",
      commandId: defenseTimeoutCommandId(attackId),
      turnId: pending.turnId,
      attackId,
    });
  }

  function scheduleFor(
    gameId: string,
    pending: Extract<PendingInteraction, { type: "defense" }>,
  ): void {
    const timerId = defenseTimerId(gameId, pending.attackId);
    scheduler.schedule(timerId, Math.max(0, pending.defenseDeadlineAt - now()), () => {
      void fire(gameId, pending.attackId).catch((error) => {
        deps.onError?.(error, { gameId, attackId: pending.attackId });
      });
    });
  }

  async function ensure(gameId: string): Promise<void> {
    const game = deps.games.get(gameId);
    if (!game || !isRulesetV2(game.ruleset)) return;
    const pending = await pendingDefenseFor(deps, gameId);
    if (!pending) return;
    scheduleFor(gameId, pending);
  }

  async function recover(): Promise<void> {
    for (const game of deps.games.list()) {
      if (!isRulesetV2(game.ruleset)) continue;
      const pending = await pendingDefenseFor(deps, game.gameId);
      if (!pending) continue;
      if (pending.defenseDeadlineAt <= now()) {
        // The deadline passed while the process was down: resolve immediately
        // rather than granting a fresh window the canonical record never had.
        await fire(game.gameId, pending.attackId);
        continue;
      }
      scheduleFor(game.gameId, pending);
    }
  }

  return { ensure, fire, recover, stop: () => scheduler.cancelAll() };
}
