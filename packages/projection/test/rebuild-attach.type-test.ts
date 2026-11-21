import { Context, Effect, Layer, Schema } from "effect";
import { Projection, ProjectionWakeup } from "@streamsy/projection";

class Inventory extends Context.Service<Inventory, ReadonlyArray<{ readonly bucket: number }>>()(
  "RebuildAttach/Inventory",
) {}

const projection = Projection.fold({
  id: "typed-inventory",
  params: { bucket: Schema.FiniteFromString },
  inputs: {},
  state: { schema: Schema.Finite, initial: 0 },
  fold: (state) => state,
});

export const processHost = Projection.attach(projection, { bindings: Inventory });

export const alarmHost = ProjectionWakeup.attach(projection, { bindings: Inventory });

type ProcessServices = Layer.Services<typeof processHost>;

type AlarmServices = Effect.Services<Effect.Success<typeof alarmHost>["next"]>;

export const processRequiresInventory: Inventory extends ProcessServices ? true : false = true;

export const alarmRequiresInventory: Inventory extends AlarmServices ? true : false = true;

Projection.attach(projection, {
  // @ts-expect-error The inventory yields decoded params, so the bucket must be numeric.
  bindings: Effect.succeed([{ bucket: "1" }]),
});

export const invalidAlarm = ProjectionWakeup.attach(projection, {
  // @ts-expect-error Alarm attachments enforce the same params.
  bindings: Effect.succeed([{ bucket: "1" }]),
});
