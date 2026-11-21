/* oxlint-disable effecttsgo/async-function -- Durable Object alarm writes are Promise-native platform calls behind the synchronous TimerScheduler contract. */
/**
 * A {@link TimerScheduler} backed by the game object's single storage alarm.
 *
 * Bun keeps one process timer for each pending defence and calls its `run`
 * callback. A game object has one alarm and at most one pending defence, so
 * `schedule` only moves that alarm to the deadline. The `run` callback is not
 * kept: the object's `alarm()` handler refolds canonical history through
 * `DefenseTimers.recover`, which resolves an expired defence through the same
 * command path and re-arms a future one. A stale or early alarm finds nothing
 * due and does nothing.
 */
import type { DurableObjectStorage } from "@cloudflare/workers-types";
import type { TimerScheduler } from "../game/defense-timer.ts";

export interface AlarmScheduler extends TimerScheduler {
  /** Wait for every alarm write started so far. Rejects when a write failed. */
  settled(): Promise<void>;
}

export function createAlarmScheduler(
  storage: Pick<DurableObjectStorage, "setAlarm">,
  now: () => number,
): AlarmScheduler {
  let writes: Promise<void>[] = [];
  return {
    schedule(_timerId, delayMs) {
      writes.push(storage.setAlarm(now() + Math.max(0, delayMs)));
    },
    // A resolved defence leaves its alarm in place. It fires, refolds, and
    // finds nothing pending, which is cheaper than tracking ownership here.
    cancel: () => undefined,
    cancelAll: () => undefined,
    async settled() {
      while (writes.length > 0) {
        const batch = writes;
        writes = [];
        await Promise.all(batch);
      }
    },
  };
}
