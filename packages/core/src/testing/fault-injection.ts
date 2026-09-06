import { Effect, Layer } from "effect";
import { StorageFault } from "../fault.ts";
import { Storage } from "../storage/storage.ts";

/** One failure per acquired decorator, counting attempted mutations. */
export function faultyStorage(
  source: Layer.Layer<Storage>,
  options: { readonly failOn: number; readonly when: "before" | "after" },
): Layer.Layer<Storage> {
  return Layer.effect(
    Storage,
    Effect.gen(function* () {
      const storage = yield* Storage;
      if (!Number.isInteger(options.failOn) || options.failOn < 1)
        return yield* Effect.die(new RangeError("failOn must be a positive integer"));
      let calls = 0;
      return Storage.of({
        ...storage,
        mutate: Effect.fn("Testing.faultyStorage.mutate")(function* (mutation) {
          const fail = ++calls === options.failOn;
          const fault = new StorageFault({
            operation: "mutate",
            message: `Injected ${options.when} mutation failure`,
            retryable: true,
          });
          if (fail && options.when === "before") return yield* fault;
          const result = yield* storage.mutate(mutation);
          if (fail && options.when === "after") return yield* fault;
          return result;
        }),
      });
    }),
  ).pipe(Layer.provide(source));
}
