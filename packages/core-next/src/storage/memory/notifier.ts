import { Effect, Queue } from "effect";

/** Private store-wide wakes, with retention isolated to each active subscription. */
export const createNotifier = Effect.gen(function* () {
  const queues = new Set<Queue.Queue<void>>();
  const subscriptions: ReadonlySet<Queue.Queue<void>> = queues;
  let closed = false;
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      const detached = yield* Effect.sync(() => {
        closed = true;
        const active = [...queues];
        queues.clear();
        return active;
      });
      for (const queue of detached) yield* Queue.shutdown(queue);
    }),
  );
  return {
    queues: subscriptions,
    get closed() {
      return closed;
    },
    subscribe: Effect.acquireRelease(
      Effect.gen(function* () {
        const queue = yield* Queue.make<void>({ capacity: 1, strategy: "dropping" });
        const registered = yield* Effect.sync(() => {
          if (closed) return false;
          queues.add(queue);
          return true;
        });
        if (!registered) {
          yield* Queue.shutdown(queue);
          return yield* Effect.interrupt;
        }
        return queue;
      }),
      (queue) =>
        Effect.andThen(
          Effect.sync(() => queues.delete(queue)),
          Queue.shutdown(queue),
        ),
    ),
    // Synchronous, nonblocking O(subscriptions) fan-out. A full queue already has
    // a wake whose authoritative read will see this commit; never stop at it.
    publish: Effect.sync(() => {
      for (const queue of queues) Queue.offerUnsafe(queue, undefined);
    }),
  };
});
export type Notifier = Effect.Success<typeof createNotifier>;
