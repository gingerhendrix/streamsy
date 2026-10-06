import { afterAll, describe, effect, expect, it as baseIt, layer, live } from "../src/index.ts";
import { Console, Context, Effect, Fiber, Layer, Ref } from "effect";
import { TestClock, TestConsole } from "effect/testing";

let releases = 0;

const resource = Effect.acquireRelease(Effect.void, () =>
  Effect.sync(() => {
    releases++;
  }),
);

effect("successful effects release test resources", () => resource);

effect.fails("typed failures release test resources", () =>
  resource.pipe(Effect.andThen(Effect.fail("expected"))),
);

effect.fails("defects release test resources", () =>
  resource.pipe(Effect.andThen(Effect.die("expected"))),
);

effect.fails("assertion failures release test resources", () =>
  resource.pipe(Effect.andThen(Effect.sync(() => expect(1).toBe(2)))),
);

baseIt("all completed test scopes have closed", () => expect(releases).toBe(4));

effect("effect uses a controllable clock and console", () =>
  Effect.gen(function* () {
    const before = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const slept = yield* Ref.make(false);

    const fiber = yield* Effect.sleep("1 hour").pipe(
      Effect.andThen(Ref.set(slept, true)),
      Effect.forkChild,
    );

    yield* TestClock.adjust("1 hour");
    yield* Fiber.join(fiber);
    expect(yield* Ref.get(slept)).toBe(true);
    expect(yield* Effect.clockWith((clock) => clock.currentTimeMillis)).toBe(before + 3_600_000);
    yield* Console.log("captured console");
    expect(yield* TestConsole.logLines).toContain("captured console");
  }),
);

live("live supplies scope and real time", () =>
  Effect.gen(function* () {
    yield* resource;
    const before = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    yield* Effect.sleep(5);
    expect(yield* Effect.clockWith((clock) => clock.currentTimeMillis)).toBeGreaterThan(before);
  }),
);

effect.each([1, 2, 3])("scalar each %s", (value) =>
  Effect.sync(() => expect(value).toBeGreaterThan(0)),
);

effect.each([
  [1, "a"],
  [2, "b"],
] as const)("tuple each %s", ([value, text]) =>
  Effect.sync(() => {
    expect(value).toBeGreaterThan(0);
    expect(text).toHaveLength(1);
  }),
);

effect.skip("skip does not run", () => Effect.die("skipped"));

effect.skipIf(true)("skipIf does not run", () => Effect.die("skipped"));

effect.runIf(false)("runIf false does not run", () => Effect.die("skipped"));

effect.runIf(true)("runIf true runs", () => Effect.void);

class Shared extends Context.Service<Shared, { readonly value: number }>()("effect-test/Shared") {}

class Nested extends Context.Service<Nested, number>()("effect-test/Nested") {}

let acquisitions = 0;

let layerReleases = 0;

const shared = Layer.effect(
  Shared,
  Effect.acquireRelease(
    Effect.sync(() => {
      acquisitions++;

      return { value: acquisitions };
    }),
    () =>
      Effect.sync(() => {
        layerReleases++;
      }),
  ),
);

layer(shared)("named shared layer", (it) => {
  it.effect("builds once", () =>
    Effect.gen(function* () {
      expect((yield* Shared).value).toBe(1);
    }),
  );
  it.effect("reuses the same service", () =>
    Effect.gen(function* () {
      expect((yield* Shared).value).toBe(1);
      expect(acquisitions).toBe(1);
    }),
  );
  it("retains Bun's synchronous API", () => expect(acquisitions).toBe(1));
  it.layer(Layer.effect(Nested, Shared.pipe(Effect.map((service) => service.value + 1))))(
    "nested",
    (nestedIt) => {
      nestedIt.effect("inherits parent services and memoization", () =>
        Effect.gen(function* () {
          expect((yield* Shared).value).toBe(1);
          expect(yield* Nested).toBe(2);
          expect(acquisitions).toBe(1);
        }),
      );
    },
  );
});

layer(shared)((it) => {
  it.effect("anonymous blocks are isolated", () =>
    Effect.gen(function* () {
      expect((yield* Shared).value).toBe(2);
      expect(layerReleases).toBe(1);
    }),
  );
});

layer(Layer.empty, { excludeTestServices: true })("live suite services", (it) => {
  it.effect("excludeTestServices keeps real time", () => Effect.sleep(5));
});

describe("top-level it methods", () => {
  baseIt.effect("it.effect has scope", () => resource);
  baseIt.live("it.live has real clock", () => Effect.sleep(5));
  baseIt.layer(Layer.succeed(Nested, 42))((it) => {
    it.effect("it.layer provides services", () =>
      Effect.gen(function* () {
        expect(yield* Nested).toBe(42);
      }),
    );
  });
});

afterAll(() => {
  expect(acquisitions).toBe(2);
  expect(layerReleases).toBe(2);
  expect(releases).toBe(6);
});
