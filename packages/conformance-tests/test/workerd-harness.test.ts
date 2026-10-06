import { expect, layer } from "@streamsy/effect-test";
import { NodeServices } from "@effect/platform-node";
import { Cause, Effect, Exit, FileSystem, Path } from "effect";
import { createWorkerdRunnerLifecycle } from "../src/workerd-harness.ts";

layer(NodeServices.layer, { excludeTestServices: true })((it) => {
  it.effect("a construction failure reclaims its registered root and preserves the cause", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "workerd-lifecycle-" });
      const root = path.join(directory, "state");
      yield* fs.makeDirectory(root);
      const construction = new Error("construction failed");

      const lifecycle = createWorkerdRunnerLifecycle({
        createRoot: () => root,
        createInstance: () => {
          throw construction;
        },
        ready: () => Promise.resolve("unused"),
      });

      const result = yield* Effect.promise(() => lifecycle.beforeAll()).pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);

      if (Exit.isFailure(result)) expect(Cause.squash(result.cause)).toBe(construction);
      expect(lifecycle.registry.states.size).toBe(0);
      expect(yield* fs.exists(root)).toBe(false);
    }),
  );

  it.effect(
    "failed startup retains a root until disposal succeeds and aggregates both causes",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "workerd-lifecycle-" });
        const root = path.join(directory, "state");
        yield* fs.makeDirectory(root);
        const startup = new Error("startup failed");
        const disposal = new Error("disposal failed");
        let attempts = 0;

        const lifecycle = createWorkerdRunnerLifecycle({
          createRoot: () => root,
          createInstance: () => ({
            dispose: () => (++attempts === 1 ? Promise.reject(disposal) : Promise.resolve()),
          }),
          ready: () => Promise.reject(startup),
        });

        const result = yield* Effect.promise(() => lifecycle.beforeAll()).pipe(Effect.exit);
        expect(Exit.isFailure(result)).toBe(true);

        if (Exit.isFailure(result)) {
          const error = Cause.squash(result.cause);
          expect(error).toBeInstanceOf(AggregateError);

          if (error instanceof AggregateError) {
            expect(error.cause).toBe(startup);
            expect(error.errors).toContain(startup);
            expect(error.errors).toContain(disposal);
          }
        }

        expect(lifecycle.registry.states.size).toBe(1);
        expect(yield* fs.exists(root)).toBe(true);
        yield* Effect.promise(() => lifecycle.afterAll());
        expect(attempts).toBe(2);
        expect(lifecycle.registry.states.size).toBe(0);
        expect(yield* fs.exists(root)).toBe(false);
      }),
  );

  it.effect("inspection failure still disposes the instance and reclaims its root", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "workerd-lifecycle-" });
      const root = path.join(directory, "state");
      yield* fs.makeDirectory(root);
      const inspection = new Error("inspection failed");
      let disposed = false;

      const lifecycle = createWorkerdRunnerLifecycle({
        createRoot: () => root,
        createInstance: () => ({
          dispose: () => {
            disposed = true;

            return Promise.resolve();
          },
        }),
        ready: () => Promise.resolve("http://fixture.test"),
        inspect: () => Promise.reject(inspection),
      });

      expect(yield* Effect.promise(() => lifecycle.beforeAll())).toBe("http://fixture.test");
      const result = yield* Effect.promise(() => lifecycle.afterAll()).pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);

      if (Exit.isFailure(result)) {
        const error = Cause.squash(result.cause);
        expect(error).toBeInstanceOf(AggregateError);

        if (error instanceof AggregateError) expect(error.errors).toEqual([inspection]);
      }

      expect(disposed).toBe(true);
      expect(lifecycle.registry.states.size).toBe(0);
      expect(yield* fs.exists(root)).toBe(false);
    }),
  );
});
