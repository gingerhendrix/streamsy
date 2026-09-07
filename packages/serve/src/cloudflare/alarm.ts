import type { DurableObjectStorage } from "@cloudflare/workers-types";
import { Clock, Context, Effect, Layer, Option } from "effect";
import { Storage } from "@streamsy/core";

export class Alarm extends Context.Service<
  Alarm,
  {
    readonly current: Effect.Effect<Option.Option<number>>;
    readonly arm: (at: number) => Effect.Effect<void>;
    readonly clear: Effect.Effect<void>;
  }
>()("@streamsy/serve/cloudflare/Alarm") {}

export const alarmLayer = (storage: DurableObjectStorage): Layer.Layer<Alarm> =>
  Layer.succeed(Alarm, {
    current: Effect.promise(() => storage.getAlarm()).pipe(Effect.map(Option.fromNullishOr)),
    arm: (at) => Effect.promise(() => storage.setAlarm(at)),
    clear: Effect.promise(() => storage.deleteAlarm()),
  });

export const reconcileAlarm = Effect.fn("Cloudflare.reconcileAlarm")(function* () {
  const storage = yield* Storage;
  const alarm = yield* Alarm;
  const next = yield* storage.nextExpiry;
  if (Option.isNone(next)) {
    yield* alarm.clear;
    return;
  }

  const current = yield* alarm.current;
  if (Option.isSome(current) && current.value <= next.value.at) return;
  const now = yield* Clock.currentTimeMillis;
  yield* alarm.arm(Math.max(next.value.at, now + 1));
});
