import type { DurableObjectStorage } from "@cloudflare/workers-types";
import { Context, Effect, Layer, Option } from "effect";

export class AlarmSlot extends Context.Service<
  AlarmSlot,
  {
    readonly current: Effect.Effect<Option.Option<number>>;
    readonly arm: (at: number) => Effect.Effect<void>;
    readonly clear: Effect.Effect<void>;
  }
>()("@streamsy/serve/cloudflare/AlarmSlot") {}

export const alarmSlotLayer = (storage: DurableObjectStorage): Layer.Layer<AlarmSlot> =>
  Layer.succeed(AlarmSlot, {
    current: Effect.promise(() => storage.getAlarm()).pipe(Effect.map(Option.fromNullishOr)),
    arm: (at) => Effect.promise(() => storage.setAlarm(at)),
    clear: Effect.promise(() => storage.deleteAlarm()),
  });
