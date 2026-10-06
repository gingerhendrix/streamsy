import { effect, expect, it } from "../src/index.ts";
import { Effect } from "effect";
import { TestClock } from "effect/testing";

let releases = 0;

const resource = Effect.acquireRelease(Effect.void, () =>
  TestClock.withLive(Effect.sleep(5)).pipe(
    Effect.andThen(
      Effect.sync(() => {
        releases++;
      }),
    ),
  ),
);

effect("numeric timeout", () => resource.pipe(Effect.andThen(Effect.never)), 30);

it("numeric timeout finalizers finished", () => expect(releases).toBe(1));

effect("options timeout", () => resource.pipe(Effect.andThen(Effect.never)), { timeout: 30 });

it("options timeout finalizers finished", () => expect(releases).toBe(2));
