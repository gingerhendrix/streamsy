import { Effect, Option, Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";

class Input extends Streamsy.json<Input>()("TimerTypes/Input", "timer-types", {
  params: {},
  schema: Schema.Finite,
}) {}

class Accepted extends Streamsy.wake<Accepted>()("TimerTypes/Accepted", "timer-accepted", {
  params: {},
}) {}

const definition = {
  id: "timer",
  params: {},
  inputs: { input: Input },
  process: () => Effect.succeed(Projection.wakeAt(1000)),
};

Projection.process({ ...definition, commit: Commit.atLeastOnce });

Projection.process({ ...definition, commit: Commit.transaction });

Projection.process({ ...definition, outputs: {}, commit: Commit.transaction });

Projection.process({ ...definition, accepted: Accepted, commit: Commit.transaction });

Projection.process({
  ...definition,
  commit: Commit.withCompensation(Commit.atLeastOnce, () => Effect.void),
});

const custom = Commit.make<void>({
  name: "custom",
  guarantee: "at-least-once",
  load: () => Effect.succeed({ record: Option.none(), token: "0" }),
  apply: (pending) => Effect.asVoid(pending.run),
});

// @ts-expect-error a custom void Commit cannot save a wake request
Projection.process({ ...definition, commit: custom });

// Existing custom void handlers still fit.
Projection.process({ ...definition, commit: custom, process: () => Effect.void });

declare const flag: boolean;

// oxlint-disable-next-line typescript/no-explicit-any -- Compatibility with type-erased existing handlers.
declare const anyValue: any;

declare const unknownValue: unknown;

// 1 conditional wake in a generator
Projection.process({
  ...definition,
  commit: Commit.atLeastOnce,
  process: () =>
    Effect.gen(function* () {
      yield* Effect.void;

      if (flag) return Projection.wakeAt(1);

      return undefined;
    }),
});

// 2 same handler, custom void: must be rejected
Projection.process({
  ...definition,
  // @ts-expect-error conditional wake under a custom void Commit
  commit: custom,
  process: () =>
    Effect.gen(function* () {
      yield* Effect.void;

      if (flag) return Projection.wakeAt(1);

      return undefined;
    }),
});

// 3 number handler under each commit
Projection.process({ ...definition, commit: custom, process: () => Effect.succeed(1) });

Projection.process({ ...definition, commit: Commit.transaction, process: () => Effect.succeed(1) });

Projection.process({
  ...definition,
  outputs: {},
  commit: Commit.transaction,
  process: () => Effect.succeed(1),
});

// 4 failure-only under custom
Projection.process({ ...definition, commit: custom, process: () => Effect.fail("x" as const) });

// 5 any and unknown under custom void
export const anyCase = Projection.process({
  ...definition,
  commit: custom,
  process: () => Effect.succeed(anyValue),
});

export const unknownCase = Projection.process({
  ...definition,
  commit: custom,
  process: () => Effect.succeed(unknownValue),
});

// 7 compensation around a custom void commit, timer handler: rejected
Projection.process({
  ...definition,
  // @ts-expect-error compensated custom void Commit cannot save a wake
  commit: Commit.withCompensation(custom, () => Effect.void),
  process: () => Effect.succeed(Projection.wakeAt(1)),
});

// 8 timer with outputs and prepare
Projection.process({
  ...definition,
  outputs: {},
  commit: Commit.transaction,
  prepare: () => Effect.succeed(3),
  process: (_b, _u, context) => Effect.succeed(Projection.wakeAt(context.prepared)),
});

// 9 number | WakeAt
Projection.process({
  ...definition,
  commit: Commit.atLeastOnce,
  process: () => Effect.succeed(flag ? 1 : Projection.wakeAt(1)),
});

// 10 run and attach accept a timer declaration
const timer = Projection.process({
  ...definition,
  commit: Commit.atLeastOnce,
  process: () => Effect.succeed(Projection.wakeAt(1)),
});

export const ran = Projection.run(timer.at({}));

export const attached = Projection.attach(timer);

Projection.process({ ...definition, commit: Commit.atLeastOnce, process: () => Effect.succeed(1) });
