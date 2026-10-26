import { it } from "@effect/vitest";
import { expect } from "vitest";
import { Deferred, Effect, Fiber } from "effect";
import { folderTasks } from "../../sync/folder-tasks.ts";

it.effect("folder tasks serialize starts and catch-ups per key while other keys progress", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const other = yield* Deferred.make<void>();
    const reads: string[] = [];

    const folders = yield* folderTasks(
      (key) =>
        key === "a"
          ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Deferred.succeed(other, undefined).pipe(Effect.asVoid),
      (key) =>
        Effect.sync(() => {
          reads.push(key);
        }),
    );

    const start = yield* folders.addFolder("a").pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    expect(folders.ready.has("a")).toBe(false);
    const read = yield* folders.catchUp("a").pipe(Effect.forkScoped);
    yield* folders.addFolder("b");
    yield* Deferred.await(other);
    expect(reads).toEqual([]);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(start);
    yield* Fiber.join(read);
    expect(folders.ready.has("a")).toBe(true);
    yield* folders.addFolder("a");
    expect(reads).toEqual(["a"]);
  }),
);

it.effect("a failed folder start is retryable and catch-up waits for a successful start", () =>
  Effect.gen(function* () {
    let attempts = 0;
    const reads: string[] = [];

    const folders = yield* folderTasks(
      () => Effect.suspend(() => (++attempts === 1 ? Effect.fail("start failed") : Effect.void)),
      (key) =>
        Effect.sync(() => {
          reads.push(key);
        }),
    );

    yield* folders.addFolder("a").pipe(Effect.flip);
    expect(folders.ready.has("a")).toBe(false);
    yield* folders.catchUp("a");
    expect(reads).toEqual([]);
    yield* folders.addFolder("a");
    yield* folders.catchUp("a");
    expect(attempts).toBe(2);
    expect(reads).toEqual(["a"]);
  }),
);
