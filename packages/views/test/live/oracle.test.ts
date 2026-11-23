/* oxlint-disable effecttsgo/strict-effect-provide -- The oracle loop uses an isolated memory host. */
import { expect, it } from "@effect/vitest";
import { DurableState } from "@streamsy/core";
import { CheckpointStore } from "@streamsy/projection";
import { Array, Effect, Option, Record } from "effect";
import { View } from "../../src/index.ts";
import { compilePlan } from "../../src/relation.ts";
import {
  customerTotals,
  customers as customerView,
  orders as orderView,
  goldOrders,
  Shop,
  topOrders,
  type Customer,
  type Order,
} from "../circuit/support/shop.ts";
import { fullRecompute } from "../support/full-recompute.ts";
import { append, awaitMatch, createShop, liveHost, params } from "./support.ts";

const views = { join: goldOrders, aggregate: customerTotals, top: topOrders };

const plans = Record.map(views, ({ expression }, name) => compilePlan(name, expression, []));

const id = "live-oracle";

const declaration = View.live({ id, params: Shop.paramCodecs, views });

const checkpoint = { id, params, version: 1, generation: 1 };

const orderSource = [...orderView.inputs.keys()][0]!;

const customerSource = [...customerView.inputs.keys()][0]!;

it.live("matches full recomputation for join, aggregate and top over seeded updates", () =>
  Effect.gen(function* () {
    yield* createShop;
    const live = yield* declaration.at(params);
    const checkpoints = yield* CheckpointStore;
    yield* live.caughtUp;
    let seed = 0xcafebabe;

    const random = (bound: number) => {
      seed ^= seed << 13;
      seed ^= seed >>> 17;
      seed ^= seed << 5;

      return (seed >>> 0) % bound;
    };

    const orders = new Map<string, typeof Order.Type>();
    const customers = new Map<string, typeof Customer.Type>();

    const records = checkpoints
      .load(checkpoint)
      .pipe(Effect.map(({ record }) => Option.getOrUndefined(record)));

    for (let unit = 0; unit < 50; unit++) {
      const changes = Array.makeBy(1 + random(6), () => {
        if (random(4) !== 0) {
          const orderId = `o${random(10)}`;

          if (random(5) === 0) {
            orders.delete(orderId);

            return DurableState.remove("order", orderId);
          }

          const row = { orderId, customerId: `c${random(4)}`, total: random(30) - 10 };
          orders.set(orderId, row);

          return DurableState.upsert("order", row);
        }

        const customerId = `c${random(4)}`;

        if (random(5) === 0) {
          customers.delete(customerId);

          return DurableState.remove("customer", customerId);
        }

        const row = {
          customerId,
          name: `Customer ${random(10)}`,
          tier: random(3) === 0 ? "standard" : "gold",
        };

        customers.set(customerId, row);

        return DurableState.upsert("customer", row);
      });

      const result = yield* append(changes);
      yield* awaitMatch(records, (record) => record?.inputs[Shop.key] === result.offset);

      const sources = {
        [orderSource]: Array.map([...orders], ([key, row]) => ({
          key,
          row,
        })),
        [customerSource]: Array.map([...customers], ([key, row]) => ({
          key,
          row,
        })),
      };

      for (const name of Record.keys(views)) {
        const expected = fullRecompute({ plan: plans[name], sources }).rows;
        const snapshot = yield* live.rows(name);
        expect(snapshot.rows, `unit ${unit}, ${name}`).toEqual(
          Array.map(expected, ({ row }) => row),
        );
      }
    }
  }).pipe(Effect.provide(liveHost)),
);
