/**
 * Durable defence-timeout runtime for `Hex Domination`.
 *
 * A declared attack opens a defence interrupt with a canonical deadline. External-agent
 * seats are resolved immediately; if no human or demo bot closes it, this component does by
 * submitting the internal
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

import { foldAggregate } from "../../src/domain/aggregate.ts";
import type { AggregateState, PendingInteraction } from "../../src/domain/aggregate.ts";
import type { CommandServiceDeps, SubmitResult } from "./command-service.ts";
import { readCanonical, submitCommand } from "./command-service.ts";
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

/** Stable identity for a server-resolved external-agent defence. */
export function automaticDefenseCommandId(attackId: string): string {
  return `automatic-defense:${attackId}`;
}

/**
 * The delayed-execution primitive. Production uses `setTimeout`; tests supply a
 * manual implementation and fire timers explicitly.
 */
export interface TimerScheduler {
  /**
   * `run` may return a promise. Production fires and forgets — delivery is
   * at-least-once and the canonical effect is exactly-once either way — but a
   * test scheduler can keep the promise so it can await the resolution instead
   * of guessing how many event-loop turns it takes.
   */
  schedule(timerId: string, delayMs: number, run: () => void | Promise<void>): void;
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
          void run();
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
  /**
   * Await everything the timers fired so far have started.
   *
   * Firing is deliberately synchronous — that is what production delivery looks
   * like — so a test that asserts straight after `fire` is racing the resolution.
   * Awaiting the recorded work makes the assertion about the protocol rather than
   * about how many event-loop turns a canonical append happens to take.
   */
  settle(): Promise<void>;
}

export function createManualScheduler(): ManualScheduler {
  const timers = new Map<string, { delayMs: number; run: () => void | Promise<void> }>();
  let inFlight: Promise<unknown>[] = [];

  function run(timer: { run: () => void | Promise<void> }): void {
    inFlight.push(Promise.resolve(timer.run()));
  }

  return {
    schedule(timerId, delayMs, timerRun) {
      if (timers.has(timerId)) return;
      timers.set(timerId, { delayMs, run: timerRun });
    },
    cancel: (timerId) => void timers.delete(timerId),
    cancelAll: () => timers.clear(),
    pending: () => [...timers.keys()],
    fire(timerId) {
      const timer = timers.get(timerId);
      if (!timer) return false;
      timers.delete(timerId);
      run(timer);
      return true;
    },
    advance(elapsedMs) {
      // Snapshot first: a firing timer may schedule the next one.
      const due = [...timers.entries()].filter(([, timer]) => timer.delayMs <= elapsedMs);
      for (const [timerId, timer] of due) {
        timers.delete(timerId);
        run(timer);
      }
    },
    async settle() {
      // A settling timer may have scheduled and fired another one.
      while (inFlight.length > 0) {
        const batch = inFlight;
        inFlight = [];
        await Promise.all(batch);
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
  fire(gameId: string, attackId: string): Promise<SubmitResult | null>;
  /**
   * Restart recovery: walk every known current game and rebuild its timer from
   * canonical pending state, resolving deadlines that have already passed.
   */
  recover(): Promise<void>;
  stop(): void;
}

interface PendingDefenseState {
  state: AggregateState;
  pending: Extract<PendingInteraction, { type: "defense" }>;
}

async function pendingDefenseStateFor(
  deps: DefenseTimerDeps,
  gameId: string,
): Promise<PendingDefenseState | null> {
  const { events } = await readCanonical(deps.protocol, eventStreamId(gameId));
  if (events.length === 0) return null;
  const state = foldAggregate(events);
  const pending = state.pendingInteraction;
  return pending?.type === "defense" ? { state, pending } : null;
}

export function createDefenseTimers(deps: DefenseTimerDeps): DefenseTimers {
  const scheduler = deps.scheduler ?? createTimeoutScheduler();
  const now = deps.now ?? deps.commandService.now;

  async function fire(gameId: string, attackId: string): Promise<SubmitResult | null> {
    // Refold before consuming anything: the attack may already be closed, and a
    // stale timer must not touch whatever is pending now.
    const current = await pendingDefenseStateFor(deps, gameId);
    if (!current || current.pending.attackId !== attackId) return null;
    return submitCommand(deps.commandService, eventStreamId(gameId), {
      type: "resolve-defense-timeout",
      commandId: defenseTimeoutCommandId(attackId),
      turnId: current.pending.turnId,
      attackId,
    });
  }

  function scheduleFor(
    gameId: string,
    pending: Extract<PendingInteraction, { type: "defense" }>,
  ): void {
    const timerId = defenseTimerId(gameId, pending.attackId);
    scheduler.schedule(timerId, Math.max(0, pending.defenseDeadlineAt - now()), () =>
      fire(gameId, pending.attackId).then(
        () => undefined,
        (error: unknown) => {
          deps.onError?.(error, { gameId, attackId: pending.attackId });
        },
      ),
    );
  }

  async function resolveAutomatic(
    gameId: string,
    current: PendingDefenseState,
  ): Promise<SubmitResult | null> {
    const defender = current.state.players.find(
      (player) => player.id === current.pending.defenderId,
    );
    if (!defender || defender.controller !== "external-agent") return null;
    return submitCommand(deps.commandService, eventStreamId(gameId), {
      type: "roll-defense",
      commandId: automaticDefenseCommandId(current.pending.attackId),
      turnId: current.pending.turnId,
      attackId: current.pending.attackId,
      playerId: current.pending.defenderId,
    });
  }

  async function ensure(gameId: string): Promise<void> {
    const game = deps.games.get(gameId);
    if (!game) return;
    const current = await pendingDefenseStateFor(deps, gameId);
    if (!current) return;
    if (await resolveAutomatic(gameId, current)) return;
    scheduleFor(gameId, current.pending);
  }

  async function recover(): Promise<void> {
    for (const game of deps.games.list()) {
      const current = await pendingDefenseStateFor(deps, game.gameId);
      if (!current) continue;
      if (await resolveAutomatic(game.gameId, current)) continue;
      if (current.pending.defenseDeadlineAt <= now()) {
        // The deadline passed while the process was down: resolve immediately
        // rather than granting a fresh window the canonical record never had.
        await fire(game.gameId, current.pending.attackId);
        continue;
      }
      scheduleFor(game.gameId, current.pending);
    }
  }

  return { ensure, fire, recover, stop: () => scheduler.cancelAll() };
}
