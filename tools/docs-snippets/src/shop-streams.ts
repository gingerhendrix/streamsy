/**
 * Shop streams for docs code blocks. A block imports them through a hidden
 * `streams.ts` that re-exports this module.
 */
import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";

export const OrderEvent = Schema.Struct({ orderId: Schema.String, amount: Schema.Finite });

export const OrderRow = Schema.Struct({ orderId: Schema.String, total: Schema.Finite });

export class Orders extends Streamsy.json<Orders>()("Shop/Orders", "orders", {
  params: {},
  schema: OrderEvent,
}) {}

export class OrderTotals extends Streamsy.json<OrderTotals>()("Shop/OrderTotals", "order-totals", {
  params: {},
  schema: OrderRow,
}) {}

export class OrderEvents extends Streamsy.json<OrderEvents>()(
  "Shop/OrderEvents",
  "orders/:orderId/events",
  { params: { orderId: Schema.String }, schema: OrderEvent },
) {}

export class Drafts extends Streamsy.json<Drafts>()("Shop/Drafts", "drafts/:draftId", {
  params: { draftId: Schema.String },
  schema: Schema.String,
}) {}

export class Journal extends Streamsy.json<Journal>()("Shop/Journal", "journal", {
  params: {},
  schema: Schema.String,
}) {}

export class Events extends Streamsy.json<Events>()("Game/Events", "events/:seat", {
  params: { seat: Schema.String },
  schema: Schema.String,
}) {}

export const shop = Streamsy.group(Orders, OrderTotals, OrderEvents);

export const game = Streamsy.group(Events);
