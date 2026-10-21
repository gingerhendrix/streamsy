import { it } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Deferred, Effect, Fiber, Logger } from "effect";
import { TestClock } from "effect/testing";
import {
  IndexBootstrapUnavailable,
  supervise,
  superviseFolder,
} from "../server/index-followers.ts";

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
    expect(Array.join(Array.map(messages.flat(), String), " ")).toContain(
      "clean stopped; restarting",
    );
    expect(Array.join(Array.map(messages.flat(), String), " ")).not.toContain("failed");
    yield* Fiber.interrupt(follower);
  }),
);

it.effect("gone folder bootstraps back off to sixty seconds, warn once, and recover", () =>
  Effect.gen(function* () {
    let attempts = 0;
    let available = false;
    const first = yield* Deferred.make<void>();
    const recovered = yield* Deferred.make<void>();
    const otherStarted = yield* Deferred.make<void>();
    const messages: unknown[] = [];
    const logger = Logger.make(({ message }) => messages.push(message));

    const work = Effect.gen(function* () {
      attempts++;
      yield* Deferred.succeed(first, undefined);

      if (!available) return yield* new IndexBootstrapUnavailable({ folderId: "gone" });
      yield* Deferred.succeed(recovered, undefined);

      return yield* Effect.never;
    });

    const follower = yield* superviseFolder("gone", work).pipe(
      Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
      Effect.forkScoped,
    );

    yield* Deferred.await(first);

    // A separate folder starts while the failed folder is waiting for its next attempt.
    const other = yield* superviseFolder(
      "healthy",
      Deferred.succeed(otherStarted, undefined).pipe(Effect.andThen(Effect.never)),
    ).pipe(Effect.forkScoped);

    yield* Deferred.await(otherStarted);

    for (const delay of [1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000]) {
      const before = attempts;
      yield* TestClock.adjust(delay - 1);
      expect(attempts).toBe(before);
      yield* TestClock.adjust(1);
      expect(attempts).toBe(before + 1);
    }

    expect(messages).toHaveLength(1);
    expect(Array.join(Array.map(messages.flat(), String), " ")).toContain(
      "gone history is unavailable at bootstrap",
    );
    available = true;
    yield* TestClock.adjust("60 seconds");
    yield* Deferred.await(recovered);
    expect(attempts).toBe(10);
    yield* TestClock.adjust("5 minutes");
    expect(attempts).toBe(10);
    expect(messages).toHaveLength(1);
    yield* Fiber.interrupt(follower);
    yield* Fiber.interrupt(other);
  }),
);
