import { expect, it } from "@streamsy/effect-test";
import { Deferred, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { supervise } from "../server/file-index.ts";

it.effect("index supervision restarts a defective follower and remains scoped", () =>
  Effect.gen(function* () {
    let attempts = 0;
    const first = yield* Deferred.make<void>();
    const recovered = yield* Deferred.make<void>();

    const work = Effect.gen(function* () {
      attempts++;

      if (attempts === 1) {
        yield* Deferred.succeed(first, undefined);

        return yield* Effect.die("injected handler defect");
      }

      yield* Deferred.succeed(recovered, undefined);

      return yield* Effect.never;
    });

    const follower = yield* supervise("test", work).pipe(Effect.forkScoped);
    yield* Deferred.await(first);
    yield* TestClock.adjust("1 second");
    yield* Deferred.await(recovered);
    expect(attempts).toBe(2);
    yield* Fiber.interrupt(follower);
    yield* TestClock.adjust("2 seconds");
    expect(attempts).toBe(2);
  }),
);
