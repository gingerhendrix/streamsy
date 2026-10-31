import { Effect } from "effect";
import { StorageFault } from "@streamsy/core";
import { Outbox, OutboxStore } from "@streamsy/outbox";
import { Wakeup, type WakeupProgress } from "./wakeup.ts";

/** A host wakeup for one outbox scope. Applications provide its store and delivery services. */
export const OutboxWakeup = {
  make: <Payload, R = never>(
    definition: Outbox.Definition<Payload>,
    deliver: Outbox.Deliver<Payload, R>,
    options: Outbox.DrainOptions = {},
  ): Wakeup<OutboxStore | R> =>
    Wakeup.make({
      name: outboxWakeupName(definition.name, options.partitionId),
      next: OutboxStore.use((store) =>
        store.nextDue({
          name: definition.name,
          partitionId: options.partitionId,
          ordered: options.ordered,
        }),
      ).pipe(
        Effect.mapError(
          (fault) =>
            new StorageFault({
              operation: "outbox.nextDue",
              message: fault.detail,
              retryable: true,
            }),
        ),
      ),
      run: Outbox.drain(definition, deliver, options).pipe(
        Effect.map((report): WakeupProgress => ({ more: report.more })),
      ),
    }),
};

function outboxWakeupName(name: string, partitionId: string | undefined): string {
  const lane = partitionId === undefined ? "" : `/${encodeURIComponent(partitionId)}`;

  return `streamsy/outbox/${encodeURIComponent(name)}${lane}`;
}
