/* oxlint-disable effecttsgo/any-unknown-in-error-context -- Wakeup.run deliberately accepts arbitrary app failures; the alarm isolates their causes. */
import {
  Array,
  Clock,
  Context,
  Duration,
  Effect,
  Exit,
  Option,
  Predicate,
  Semaphore,
} from "effect";
import { Storage, type StorageFault } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { AlarmSlot } from "./alarm.ts";

/**
 * A bounded run that stopped at its bound returns `{ more: true }`. The
 * scheduler then re-arms at once and does not count the pass as a failure.
 */
export interface WakeupProgress {
  readonly more: boolean;
}

/** One source of deadline-driven work, derived from committed state. */
export interface Wakeup<R = never> {
  readonly name: string;
  readonly next: Effect.Effect<Option.Option<number>, StorageFault, R>;
  /**
   * Bounded, idempotent work. Delivery is at least once. The permit for this
   * name is not reentrant, so a run must not call its own wakeup's `run`.
   */
  readonly run: Effect.Effect<void | WakeupProgress, unknown, R>;
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

const reportsMore = (value: void | WakeupProgress) =>
  Predicate.hasProperty(value, "more") && value.more;

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
    /**
     * Marks the current request so the host reconciles when the handler
     * returns. Call it before the handler returns. A streamed body that
     * queues work later must call `reconcile` itself.
     */
    readonly touch: Effect.Effect<void>;
    readonly reconcile: Effect.Effect<void>;
    readonly status: Effect.Effect<
      ReadonlyArray<{
        readonly name: string;
        readonly next: Option.Option<number>;
        readonly failures: number;
        /** The backoff floor. The wakeup does not run before this instant. */
        readonly retryAt: Option.Option<number>;
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
    const wakeups = Array.map([expiry, ...definitions], (wakeup) => Wakeup.make(wakeup));
    const names = new Set<string>();

    for (const wakeup of wakeups) {
      if (names.has(wakeup.name))
        return yield* Effect.die(new Error(`Duplicate wakeup name: ${wakeup.name}`));
      names.add(wakeup.name);
    }

    const permits = new Map(Array.map(wakeups, (wakeup) => [wakeup.name, Semaphore.makeUnsafe(1)]));
    const reconcilePermit = yield* Semaphore.make(1);
    const turnPermit = yield* Semaphore.make(1);

    const states = Array.map(wakeups, (wakeup) => ({
      wakeup,
      failures: 0,
      floor: 0,
      initial: Duration.toMillis(wakeup.retry?.initial ?? "1 second"),
      max: Duration.toMillis(wakeup.retry?.max ?? "1 hour"),
    }));

    const slot = Context.get(context, AlarmSlot);
    // Initialized before any of the effects below are executed.
    const provided = () => Context.add(Context.add(context, Permits, permits), Wakeups, service);

    type State = (typeof states)[number];

    const backOff = (state: State, at: number) => {
      state.failures += 1;
      state.floor = at + Math.min(state.max, state.initial * 2 ** (state.failures - 1));
    };

    const recover = (state: State) => {
      state.failures = 0;
      state.floor = 0;
    };

    const status = Effect.forEach(states, (state) =>
      state.wakeup.next.pipe(
        Effect.map((next) => ({
          name: state.wakeup.name,
          next,
          failures: state.failures,
          retryAt: state.floor > 0 ? Option.some(state.floor) : Option.none<number>(),
        })),
      ),
    );

    // A failing `next` must not stop other deadlines from arming. The broken
    // wakeup retries on its own backoff, which grows at most once per floor.
    const deadlineOf = (state: State, now: number) =>
      state.wakeup.next.pipe(
        Effect.map(Option.map((next) => Math.max(next, state.floor))),
        Effect.catchCause((cause) => {
          if (state.floor <= now) backOff(state, now);

          return Effect.logError(`Wakeup ${state.wakeup.name} deadline failed`, cause).pipe(
            Effect.as(Option.some(state.floor)),
          );
        }),
      );

    // A forced pass replaces the in-flight guard, which is always earlier than real work.
    const reconcileWith = (force: boolean) =>
      reconcilePermit.withPermit(
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          let target = Infinity;

          for (const state of states) {
            const next = yield* deadlineOf(state, now);

            if (Option.isSome(next)) target = Math.min(target, next.value);
          }

          if (target === Infinity) return yield* slot.clear;
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
            const at = yield* Clock.currentTimeMillis;

            if (state.floor > at) return;
            const next = yield* state.wakeup.next;

            if (Option.isNone(next) || next.value > at) return;
            const exit = yield* Effect.exit(state.wakeup.run);

            if (Exit.isFailure(exit)) {
              backOff(state, yield* Clock.currentTimeMillis);
              yield* Effect.logError(`Wakeup ${state.wakeup.name} failed`, exit.cause);

              return;
            }

            // A run that stopped at its bound made progress. Work that became
            // due during the run is new work. Only work that was due before
            // the run and is still due shows a run that changed nothing.
            const after = yield* state.wakeup.next;

            if (!reportsMore(exit.value) && Option.isSome(after) && after.value <= at)
              backOff(state, yield* Clock.currentTimeMillis);
            else recover(state);
          });

          yield* attempt.pipe(
            Effect.catchCause((cause) =>
              Clock.currentTimeMillis.pipe(
                Effect.flatMap((at) => {
                  backOff(state, at);

                  return Effect.logError(`Wakeup ${state.wakeup.name} deadline failed`, cause);
                }),
              ),
            ),
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
