import { afterAll, expect, layer } from "../src/index.ts";
import { Effect, Layer } from "effect";

let released = false;

layer(
  Layer.effectDiscard(
    Effect.acquireRelease(Effect.void, () =>
      Effect.sync(() => {
        released = true;
      }),
    ).pipe(Effect.andThen(Effect.die("expected setup failure"))),
  ),
)("failed suite setup", (it) => {
  it.effect("never runs", () => Effect.die("test body must not run"));
});

afterAll(() => {
  expect(released).toBe(true);
  process.stdout.write("setup finalizers completed\n");
});
