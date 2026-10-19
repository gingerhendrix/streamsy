import { it } from "@effect/vitest";
import { expect } from "vitest";
import { Deferred, Effect, Fiber, Logger } from "effect";
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

it.effect("a clean follower finish is logged as stopped and restarts with fresh progress", () =>
  Effect.gen(function* () {
    const messages: unknown[] = [];
    const first = yield* Deferred.make<void>();
    const resumed = yield* Deferred.make<void>();
    let checkpoint = "startup";
    const offsets: string[] = [];

    const work = Effect.gen(function* () {
      offsets.push(checkpoint);

      if (offsets.length === 1) {
        checkpoint = "advanced";
        yield* Deferred.succeed(first, undefined);

        return yield* Effect.void;
      }

      yield* Deferred.succeed(resumed, undefined);

      return yield* Effect.never;
    });

    const logger = Logger.make(({ message }) => messages.push(message));

    const follower = yield* supervise("clean", work).pipe(
      Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
      Effect.forkScoped,
    );

    yield* Deferred.await(first);
    yield* TestClock.adjust("1 second");
    yield* Deferred.await(resumed);
    expect(offsets).toEqual(["startup", "advanced"]);
    expect(messages.flat().map(String).join(" ")).toContain("clean stopped; restarting");
    expect(messages.flat().map(String).join(" ")).not.toContain("failed");
    yield* Fiber.interrupt(follower);
  }),
);
