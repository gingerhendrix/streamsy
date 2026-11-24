/* oxlint-disable effecttsgo/strict-effect-provide -- Each test runs an isolated owner and can replace its checkpoint settlement. */
import { expect, it } from "@effect/vitest";
import { CheckpointStore, Transaction, Projection, ProjectionFault } from "@streamsy/projection";
import { Effect } from "effect";
import { makeLiveOutputs } from "../../src/live-output.ts";
import { prepareLive } from "../../src/live.ts";
import { memoryStateOf } from "../../src/memory/backend.ts";
import { currentBackend } from "../../src/view-backend.ts";
import { orders, Shop } from "../circuit/support/shop.ts";
import { createShop, liveHost, params, putOrder } from "./support.ts";

const plan = prepareLive({
  id: "live-transaction",
  params: Shop.paramCodecs,
  views: { orders },
});

const setup = Effect.gen(function* () {
  yield* createShop;
  yield* putOrder(1);

  const tx = yield* Transaction;
  const store = yield* CheckpointStore;
  const backend = yield* currentBackend;
  const outputs = yield* makeLiveOutputs(plan.names);
  const { binding, namespace } = plan.bind(params, backend, tx, outputs.stage);
  const state = memoryStateOf(backend)!;

  return {
    tx,
    store,
    binding,
    run: Projection.run(binding, { limit: 1 }),
    rows: outputs.rows("orders"),
    hasState: () => state.has(namespace),
  };
});

it.live("drops a failed unit whose enclosing transaction commits", () =>
  Effect.gen(function* () {
    const { tx, store, binding, run, rows, hasState } = yield* setup;
    const initial = yield* rows;

    const rejection = new ProjectionFault({
      phase: "checkpoint",
      reason: "invalid-record",
      message: "reject settlement after output staging",
    });

    yield* tx.withTransaction(
      Effect.gen(function* () {
        const failure = yield* run.pipe(
          Effect.provideService(CheckpointStore, {
            ...store,
            compareAndSet: () => Effect.fail(rejection),
          }),
          Effect.flip,
        );

        expect(failure).toBe(rejection);
        expect(yield* rows).toBe(initial);
        expect(hasState()).toBe(false);
        expect((yield* store.load(binding)).token).toBe("0");
      }),
    );

    expect(yield* rows).toBe(initial);
    expect(hasState()).toBe(false);
    expect((yield* store.load(binding)).token).toBe("0");
    yield* run;
    expect((yield* rows).rows).toEqual([{ orderId: "o1", customerId: "c1", total: 1 }]);
  }).pipe(Effect.provide(liveHost)),
);

it.live("rolls back an enclosing transaction and publishes two successful units in order", () =>
  Effect.gen(function* () {
    const { tx, store, binding, run, rows, hasState } = yield* setup;
    const initial = yield* rows;

    const twoUnits = Effect.gen(function* () {
      expect((yield* run).units).toBe(1);
      yield* putOrder(2);
      expect((yield* run).units).toBe(1);
      expect((yield* store.load(binding)).token).toBe("2");
      expect(yield* rows).toBe(initial);
    });

    const rollback = tx.withTransaction(twoUnits.pipe(Effect.andThen(Effect.fail("abort"))));

    expect(yield* rollback.pipe(Effect.flip)).toBe("abort");
    expect(yield* rows).toBe(initial);
    expect(hasState()).toBe(false);
    expect((yield* store.load(binding)).token).toBe("0");

    yield* tx.withTransaction(twoUnits);
    expect((yield* store.load(binding)).token).toBe("2");
    expect((yield* rows).rows).toEqual([{ orderId: "o1", customerId: "c1", total: 2 }]);
  }).pipe(Effect.provide(liveHost)),
);
