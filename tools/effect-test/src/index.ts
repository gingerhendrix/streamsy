// oxlint-disable effecttsgo/strict-effect-provide -- This private test runner is the entry point that supplies test services and suite layers.
import { afterAll, afterEach, beforeAll, describe, test as bunTest } from "bun:test";
import type { TestOptions, Test as BunTest } from "bun:test";
import { Cause, Duration, Effect, Exit, Fiber, Layer, Scope } from "effect";
import { TestClock, TestConsole } from "effect/testing";

export * from "bun:test";

type Timeout = number | TestOptions;

type Test<R> = <A, E>(name: string, self: () => Effect.Effect<A, E, R>, timeout?: Timeout) => void;

export interface Tester<R> {
  <A, E>(name: string, self: () => Effect.Effect<A, E, R>, timeout?: Timeout): void;
  readonly skip: Test<R>;
  readonly only: Test<R>;
  readonly fails: Test<R>;
  readonly skipIf: (condition: boolean) => Test<R>;
  readonly runIf: (condition: boolean) => Test<R>;
  readonly each: <T>(
    cases: ReadonlyArray<T>,
  ) => <A, E>(name: string, self: (value: T) => Effect.Effect<A, E, R>, timeout?: Timeout) => void;
}

interface LayerOptions {
  readonly timeout?: Duration.Input;
  readonly memoMap?: Layer.MemoMap;
  readonly excludeTestServices?: boolean;
}

interface LayerTests<R> {
  (f: (it: MethodsNonLive<R>) => void): void;
  (name: string, f: (it: MethodsNonLive<R>) => void): void;
}

export interface MethodsNonLive<R> extends BunTest<[]> {
  readonly effect: Tester<R | Scope.Scope>;
  readonly layer: <R2, E>(
    layer: Layer.Layer<R2, E, R>,
    options?: Pick<LayerOptions, "timeout">,
  ) => LayerTests<R | R2>;
}

const testEnv = Layer.mergeAll(TestConsole.layer, TestClock.layer());

const running = new Map<Fiber.Fiber<void, unknown>, () => void>();

const run = <A, E>(self: Effect.Effect<A, E>) => {
  const program = Effect.asVoid(self);

  const fiber = Effect.runFork(program);

  let abandoned = false;
  running.set(fiber, () => {
    abandoned = true;
  });

  return Effect.runPromiseExit(Fiber.join(fiber)).then((exit) => {
    running.delete(fiber);

    // Once Bun times out it drops the callback promise. Teardown waits for
    // the interruption exit rather than rejecting that abandoned promise.
    if (Exit.isFailure(exit) && !abandoned) throw Cause.squash(exit.cause);
  });
};

const interruptRunning = Effect.suspend(() =>
  Effect.forEach(
    [...running],
    ([fiber, abandon]) => {
      abandon();

      return Fiber.interrupt(fiber);
    },
    { discard: true },
  ),
);

// Bun caches this module across test files. Each registration installs this
// cleanup in the caller's suite, where it waits for timed-out test finalizers.
const cleanupRunning = () => {
  return running.size > 0 ? Effect.runPromise(interruptRunning) : Promise.resolve();
};

const makeTester = <R>(
  mapEffect: <A, E>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E>,
): Tester<R> => {
  const execute = <A, E>(self: () => Effect.Effect<A, E, R>) =>
    run(mapEffect(Effect.suspend(self)));

  const register =
    (test: typeof bunTest): Test<R> =>
    (name, self, timeout) => {
      // Register in the caller's suite: Bun caches this module across test files.
      afterEach(cleanupRunning);
      test(name, () => execute(self), timeout);
    };

  // Bun's `test.only` getter throws when CI=true, so read it only when a
  // test uses `.only`. An eager read breaks every suite that imports this module.
  const only: Test<R> = (name, self, timeout) => register(bunTest.only)(name, self, timeout);

  return Object.assign(register(bunTest), {
    skip: register(bunTest.skip),
    only,
    fails: register(bunTest.failing),
    skipIf: (condition: boolean) => register(condition ? bunTest.skip : bunTest),
    runIf: (condition: boolean) => register(condition ? bunTest : bunTest.skip),
    each:
      <T>(cases: ReadonlyArray<T>) =>
      <A, E>(name: string, self: (value: T) => Effect.Effect<A, E, R>, timeout?: Timeout) => {
        afterEach(cleanupRunning);
        bunTest.each(cases.map((value) => [value] as const))(
          name,
          (value) => execute(() => self(value)),
          timeout,
        );
      },
  });
};

const makeLayer = <R, E>(
  suiteLayer: Layer.Layer<R, E>,
  options: LayerOptions = {},
): LayerTests<R> => {
  const collect = (f: (it: MethodsNonLive<R>) => void) => {
    const scope = Scope.makeUnsafe();
    const memoMap = options.memoMap ?? Layer.makeMemoMapUnsafe();

    const withTestEnv = options.excludeTestServices
      ? suiteLayer
      : Layer.provideMerge(suiteLayer, testEnv);

    const context = Effect.runSync(
      Effect.cached(Effect.orDie(Layer.buildWithMemoMap(withTestEnv, memoMap, scope))),
    );

    const hookTimeout =
      options.timeout === undefined
        ? undefined
        : Duration.toMillis(Duration.fromInputUnsafe(options.timeout));

    beforeAll(() => run(Effect.asVoid(context)), hookTimeout);
    afterAll(
      () => Effect.runPromise(interruptRunning.pipe(Effect.andThen(Scope.close(scope, Exit.void)))),
      hookTimeout,
    );

    f(
      Object.assign((...args: Parameters<typeof bunTest>) => bunTest(...args), bunTest, {
        effect: makeTester<R | Scope.Scope>((self) =>
          Effect.flatMap(context, (services) => Effect.scoped(Effect.provide(self, services))),
        ),
        layer: <R2, E2>(
          nested: Layer.Layer<R2, E2, R>,
          nestedOptions?: Pick<LayerOptions, "timeout">,
        ) =>
          makeLayer(Layer.provideMerge(nested, withTestEnv), {
            ...nestedOptions,
            memoMap: Layer.forkMemoMapUnsafe(memoMap),
            excludeTestServices: options.excludeTestServices,
          }),
      }),
    );
  };

  return (
    ...args:
      | [f: (it: MethodsNonLive<R>) => void]
      | [name: string, f: (it: MethodsNonLive<R>) => void]
  ) => {
    // Anonymous blocks get an isolated Bun suite so their hooks cannot affect siblings.
    if (args.length === 1) describe("", () => collect(args[0]));
    else describe(args[0], () => collect(args[1]));
  };
};

export const effect = makeTester<Scope.Scope>((self) =>
  Effect.provide(Effect.scoped(self), testEnv),
);

export const live = makeTester<Scope.Scope>(Effect.scoped);

export const layer = makeLayer;

export const it = Object.assign(
  (...args: Parameters<typeof bunTest>) => bunTest(...args),
  bunTest,
  {
    effect,
    live,
    layer,
  },
);
