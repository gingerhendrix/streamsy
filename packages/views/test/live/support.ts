import { DurableState, Streamsy } from "@streamsy/core";
import { Array, Effect, Schedule } from "effect";
import { View } from "../../src/index.ts";
import { orders, Shop } from "../circuit/support/shop.ts";
import { memoryHost } from "../support/memory.ts";

export const params = { shop: "live" };

export const liveHost = memoryHost(Streamsy.group(Shop));

export const liveOrders = View.live({
  id: "live-orders",
  params: Shop.paramCodecs,
  views: { orders },
});

export const createShop = Effect.flatMap(Shop, (shop) => shop.create(params));

export const append = (
  changes: Array.NonEmptyReadonlyArray<DurableState.Entry<typeof Shop.collections>>,
) =>
  Effect.flatMap(Shop, (shop) =>
    shop.append(params, DurableState.changes(Shop.at(params), {}, changes)),
  );

export const putOrder = (total: number, orderId = "o1") =>
  append([DurableState.upsert("order", { orderId, customerId: "c1", total })]);

export const awaitMatch = <A, E, R>(effect: Effect.Effect<A, E, R>, match: (value: A) => boolean) =>
  effect.pipe(
    Effect.repeat({ schedule: Schedule.spaced("5 millis"), until: match }),
    Effect.timeout("3 seconds"),
  );
