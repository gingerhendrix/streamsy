import { DurableStream } from "@durable-streams/client";
import { createStateSchema } from "@durable-streams/state";
import { createStreamDB } from "@durable-streams/state/db";

interface Order {
  id: string;
}
const orderSchema = {
  "~standard": {
    version: 1 as const,
    vendor: "streamsy-browser-fixture",
    // oxlint-disable-next-line anti-slop/no-unknown-parameters -- Standard Schema validators receive untrusted browser payloads.
    validate(value: unknown): { value: Order } | { issues: { message: string }[] } {
      // oxlint-disable-next-line anti-slop/no-runtime-typeof -- Validate the external id before constructing the typed record.
      return value instanceof Object && "id" in value && typeof value.id === "string"
        ? { value: { id: value.id } }
        : { issues: [{ message: "Expected an order with a string id" }] };
    },
  },
};
export const orders = createStateSchema({
  orders: { schema: orderSchema, type: "order", primaryKey: "id" },
});

/** Browser caller owns the returned database and its shutdown lifecycle. */
export function openOrders(url: string) {
  return createStreamDB({ streamOptions: { url, contentType: "application/json" }, state: orders });
}
export function appendOrder(url: string, order: Order) {
  const stream = new DurableStream({ url, contentType: "application/json" });
  return stream.append(JSON.stringify(orders.orders.insert({ value: order })));
}
