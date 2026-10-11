import { BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { CheckpointStore, Projection, ProjectionFault } from "@streamsy/projection";
import { Effect, Layer, Result } from "effect";
import { emitted, Files, host, Shared, temp } from "./fixtures.ts";

layer(BunServices.layer)((it) => {
  it.effect("shared producer output survives a crash between append and settlement", () =>
    Effect.gen(function* () {
      const dir = yield* temp;
      const filename = `${dir}/shared.sqlite`;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(host(filename));
          yield* Effect.gen(function* () {
            const files = yield* Files;
            const shared = yield* Shared;
            yield* shared.create({});

            for (const folderId of ["a", "b"]) {
              yield* files.create({ folderId });
              yield* files.append({ folderId }, [folderId]);
            }

            const store = yield* CheckpointStore;
            let saves = 0;

            const failed = yield* Projection.run(emitted.at({ folderId: "a" })).pipe(
              Effect.provideService(CheckpointStore, {
                ...store,
                compareAndSet: (key, token, record) =>
                  ++saves === 2
                    ? Effect.fail(
                        new ProjectionFault({
                          phase: "checkpoint",
                          reason: "storage-failure",
                          message: "crash after append",
                        }),
                      )
                    : store.compareAndSet(key, token, record),
              }),
              Effect.result,
            );

            expect(Result.isFailure(failed)).toBe(true);
            yield* Projection.run(emitted.at({ folderId: "b" }));
            expect((yield* shared.collect({})).items).toHaveLength(2);
          }).pipe(Effect.provideContext(context));
        }),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(host(filename));
          yield* Effect.gen(function* () {
            yield* Projection.run(emitted.at({ folderId: "a" }));
            yield* Projection.run(emitted.at({ folderId: "b" }));
            const shared = yield* Shared;
            expect((yield* shared.collect({})).items).toEqual([
              { folderId: "a", text: "a" },
              { folderId: "b", text: "b" },
            ]);
            const files = yield* Files;
            yield* files.append({ folderId: "a" }, ["after restart"]);
            yield* Projection.run(emitted.at({ folderId: "a" }));
            expect((yield* shared.collect({})).items).toHaveLength(3);
          }).pipe(Effect.provideContext(context));
        }),
      );
    }),
  );
});
