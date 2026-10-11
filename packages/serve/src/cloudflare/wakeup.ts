/* oxlint-disable effecttsgo/any-unknown-in-error-context -- Wakeup.run deliberately accepts arbitrary app failures; the alarm isolates their causes. */
import { Clock, Context, Duration, Effect, Exit, Option, Predicate, Semaphore } from "effect";
import { Storage, type StorageFault } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { AlarmSlot } from "./alarm.ts";

/** One source of deadline-driven work, derived from committed state. */
export interface Wakeup<R = never> {
  readonly name: string;
  readonly next: Effect.Effect<Option.Option<number>, StorageFault, R>;
  /** Bounded, idempotent work. Delivery is at least once. */
  readonly run: Effect.Effect<void, unknown, R>;
  readonly retry?: {
    readonly initial?: Duration.Input;
    readonly max?: Duration.Input;
  };
}

// Each acquired object supplies its own permits to requests and alarms.
const Permits = Context.Reference<ReadonlyMap<string, Semaphore.Semaphore> | undefined>(
  "@streamsy/serve/cloudflare/WakeupPermits",
  { defaultValue: () => undefined },
);

const Guarded = Symbol.for("@streamsy/serve/cloudflare/WakeupPermit");

export const Wakeup = {
  make: <R>(wakeup: Wakeup<R>): Wakeup<R> => {
    if (Predicate.hasProperty(wakeup.run, Guarded) && wakeup.run[Guarded] === wakeup.name)
      return wakeup;

    const run = Effect.flatMap(Permits, (permits) => {
      const permit = permits?.get(wakeup.name);

      return permit === undefined ? wakeup.run : permit.withPermit(wakeup.run);
    });

    const protectedRun = Object.defineProperty(run, Guarded, { value: wakeup.name });

    return { ...wakeup, run: protectedRun };
  },
};

export class Wakeups extends Context.Service<
  Wakeups,
  {
    readonly touch: Effect.Effect<void>;
    readonly reconcile: Effect.Effect<void>;
    readonly status: Effect.Effect<
      ReadonlyArray<{
        readonly name: string;
        readonly next: Option.Option<number>;
        readonly failures: number;
      }>
    >;
  }
>()("@streamsy/serve/cloudflare/Wakeups") {}

/** Request-local marker: simultaneous requests cannot consume each other's touch. */
export const RequestTouch = Context.Reference<{ touched: boolean } | undefined>(
  "@streamsy/serve/cloudflare/RequestTouch",
  { defaultValue: () => undefined },
);

export const expiry: Wakeup<Storage> = Wakeup.make({
  name: "streamsy/expiry",
  next: Storage.use((storage) => storage.nextExpiry).pipe(
    Effect.map(Option.map((deadline) => deadline.at)),
  ),
  run: Protocol.expireDue().pipe(Effect.asVoid),
});

/** Internal object owner. Its context closes over the same services as fetch. */
export const makeWakeups = <R>(
  context: Context.Context<Storage | AlarmSlot | R>,
  definitions: ReadonlyArray<Wakeup<Storage | Wakeups | R>> = [],
) =>
  Effect.gen(function* () {
    const wakeups = [expiry, ...definitions].map(Wakeup.make);
    const names = new Set<string>();

    for (const wakeup of wakeups) {
      if (names.has(wakeup.name))
        return yield* Effect.die(new Error(`Duplicate wakeup name: ${wakeup.name}`));
      names.add(wakeup.name);
    }

    const permits = new Map(wakeups.map((wakeup) => [wakeup.name, Semaphore.makeUnsafe(1)]));
    const reconcilePermit = yield* Semaphore.make(1);
    const turnPermit = yield* Semaphore.make(1);

    const states = wakeups.map((wakeup) => ({
      wakeup,
      failures: 0,
      floor: 0,
      initial: Duration.toMillis(wakeup.retry?.initial ?? "1 second"),
      max: Duration.toMillis(wakeup.retry?.max ?? "1 hour"),
    }));

    const slot = Context.get(context, AlarmSlot);
    // Initialized before any of the effects below are executed.
    const provided = () => Context.add(Context.add(context, Permits, permits), Wakeups, service);

    const status = Effect.forEach(states, (state) =>
      state.wakeup.next.pipe(
        Effect.map((next) => ({ name: state.wakeup.name, next, failures: state.failures })),
      ),
    );

    // A forced pass replaces the in-flight guard, which is always earlier than real work.
    const reconcileWith = (force: boolean) =>
      reconcilePermit.withPermit(
        Effect.gen(function* () {
          let target = Infinity;

          for (const state of states) {
            const next = yield* state.wakeup.next;

            if (Option.isSome(next)) target = Math.min(target, Math.max(next.value, state.floor));
          }

          if (target === Infinity) return yield* slot.clear;
          const now = yield* Clock.currentTimeMillis;
          const current = yield* slot.current;

          if (!force && Option.isSome(current) && now < current.value && current.value <= target)
            return undefined;

          if (Option.isSome(current) && current.value <= now) yield* slot.clear;

          return yield* slot.arm(Math.max(target, now + 1));
        }),
      );

    const reconcile = reconcileWith(false);

    const service = Wakeups.of({
      touch: Effect.flatMap(RequestTouch, (marker) =>
        Effect.sync(() => {
          if (marker !== undefined) marker.touched = true;
        }),
      ),
      reconcile: Effect.suspend(() => reconcile.pipe(Effect.provide(provided()), Effect.orDie)),
      status: Effect.suspend(() => status.pipe(Effect.provide(provided()), Effect.orDie)),
    });

    const alarm = turnPermit.withPermit(
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        yield* slot.arm(now + 30_000);

        for (const state of states) {
          const attempt = Effect.gen(function* () {
            const next = yield* state.wakeup.next;
            const at = yield* Clock.currentTimeMillis;

            if (Option.isNone(next) || next.value > at || state.floor > at) return;
            const exit = yield* Effect.exit(state.wakeup.run);
            const after = yield* state.wakeup.next;
            const finished = yield* Clock.currentTimeMillis;

            if (Exit.isFailure(exit) || (Option.isSome(after) && after.value <= finished)) {
              state.failures += 1;
              state.floor =
                finished + Math.min(state.max, state.initial * 2 ** (state.failures - 1));

              if (Exit.isFailure(exit))
                yield* Effect.logError(`Wakeup ${state.wakeup.name} failed`, exit.cause);
            } else {
              state.failures = 0;
              state.floor = 0;
            }
          });

          yield* attempt.pipe(
            Effect.catchCause((cause) => {
              state.failures += 1;

              return Clock.currentTimeMillis.pipe(
                Effect.flatMap((at) => {
                  state.floor = at + Math.min(state.max, state.initial * 2 ** (state.failures - 1));

                  return Effect.logError(`Wakeup ${state.wakeup.name} deadline failed`, cause);
                }),
              );
            }),
          );
        }
      }).pipe(
        Effect.ensuring(
          reconcileWith(true).pipe(
            Effect.catchCause((cause) => Effect.logError("Wakeup reconciliation failed", cause)),
          ),
        ),
        Effect.catchCause((cause) => Effect.logError("Wakeup alarm failed", cause)),
      ),
    );

    const complete = provided();
    // A failing `next` must not make the object unavailable. The next write or alarm retries.
    yield* reconcile.pipe(
      Effect.provide(complete),
      Effect.catchCause((cause) =>
        Effect.logError("Wakeup reconciliation at acquire failed", cause),
      ),
    );

    return { context: complete, alarm: alarm.pipe(Effect.provide(complete)) };
  });
