/* oxlint-disable anti-slop-effect/no-service-constructor-imports -- The suite constructs its isolated engine or test scheduler. */
/* oxlint-disable effecttsgo/strict-effect-provide -- Each test provisions its own database. */
import { expect, it } from "@effect/vitest";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Array as Arr, Effect, Record as Rec } from "effect";
import { makeEngine, type StateChange } from "../../src/sql/engine.ts";
import { fullRecompute } from "../support/full-recompute.ts";
import { canonicalJson } from "../../src/runtime/key.ts";
import type { JsonObject } from "../../src/ir/contracts.ts";
import { shopPlans } from "./fixtures.ts";

import { migratedSqliteBackend, identity, materialize } from "./support.ts";

it.live(
  "matches the independent oracle: 200 seeds × 40 random multi-change units, with exact no-op suppression",
  () =>
    Effect.gen(function* () {
      const started = performance.now();
      const backend = yield* migratedSqliteBackend;
      const plans = shopPlans(true);
      const engine = yield* makeEngine(plans);

      for (let seed = 1; seed <= 200; seed++) {
        let state = seed;

        const random = (max: number) => {
          state ^= state << 13;
          state ^= state >>> 17;
          state ^= state << 5;

          return (state >>> 0) % max;
        };

        const orders = new Map<string, JsonObject>(),
          customers = new Map<string, JsonObject>();

        const actual = new Map<string, string>();

        for (let unit = 0; unit < 40; unit++) {
          const input: StateChange[] = [];

          const change = () => {
            const isOrder = random(4) !== 0;
            const sourceId = isOrder ? "orders" : "customers";
            const key = isOrder ? `o-${random(12)}` : `c-${random(4)}`;
            const rows = isOrder ? orders : customers;

            if (random(5) === 0) {
              rows.delete(key);
              input.push({ sourceId, kind: "delete", key });
            } else {
              const row: JsonObject = isOrder
                ? { orderId: key, customerId: `c-${random(4)}`, total: (random(21) - 10) / 10 }
                : {
                    customerId: key,
                    name: `Customer ${random(3)}`,
                    tier: random(2) ? "gold" : "standard",
                  };

              rows.set(key, row);
              input.push({ sourceId, kind: "upsert", key, value: row });
            }
          };

          for (let n = 1 + random(6); n > 0; n--) change();
          // Explicitly exercise several changes to one key and an unchanged final image.
          const last = input.at(-1);

          if (last && unit % 3 === 0)
            input.push({ sourceId: last.sourceId, kind: "delete", key: last.key }, last);
          const old = new Map(actual);
          const emitted = yield* engine.process(backend, `seed-${seed}`, input);
          const unique = new Set(Arr.map(emitted, (c) => identity(c.view, c.key)));
          expect(unique.size).toBe(emitted.length);
          materialize(actual, emitted);
          const expected = new Map<string, string>();

          for (const [name, p] of Rec.toEntries(plans)) {
            const result = fullRecompute({
              plan: p,
              sources: {
                orders: Arr.map([...orders], ([key, row]) => ({ key, row })),
                customers: Arr.map([...customers], ([key, row]) => ({ key, row })),
              },
            });

            for (const row of result.rows)
              expected.set(identity(name, row.key), canonicalJson(row.row));
          }

          expect(actual, `seed=${seed} unit=${unit}`).toEqual(expected);
          const changed = new Set<string>();

          for (const k of new Set([...old.keys(), ...expected.keys()]))
            if (old.get(k) !== expected.get(k)) changed.add(k);
          expect(unique, `no-op suppression seed=${seed} unit=${unit}`).toEqual(changed);

          if (unit % 10 === 0)
            expect(yield* engine.process(backend, `seed-${seed}`, input)).toEqual([]);
        }
      }

      yield* Effect.log(
        `SQL conformance: 200 seeds × 40 units; ${(performance.now() - started).toFixed(0)} ms`,
      );
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
  120_000,
);
