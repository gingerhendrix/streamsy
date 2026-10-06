import { effect, expect, it } from "../src/index.ts";
import { Effect } from "effect";
import { TestClock } from "effect/testing";

let released = false;

effect.fails("Bun's default timeout interrupts the test", () =>
  Effect.acquireRelease(Effect.void, () =>
    TestClock.withLive(Effect.sleep(5)).pipe(
      Effect.andThen(
        Effect.sync(() => {
          released = true;
        }),
      ),
    ),
  ).pipe(Effect.andThen(Effect.never)),
);

it("the next test waits for finalizers", () => expect(released).toBe(true));
