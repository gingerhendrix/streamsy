import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Commit, Projection } from "@streamsy/projection";
import { Deferred, Effect, Fiber, Scope } from "effect";
import { emitted, Files, host, Registry, Shared } from "./fixtures.ts";

layer(host(":memory:"), { excludeTestServices: true })((it) => {
  it.effect("registry starts folder followers, including a folder added after startup", () =>
    Effect.gen(function* () {
      const seen = yield* Deferred.make<void>();
      const registry = yield* Registry;
      const files = yield* Files;
      const shared = yield* Shared;
      yield* registry.create({});
      yield* shared.create({});
      yield* files.create({ folderId: "before" });
      yield* files.append({ folderId: "before" }, ["first"]);
      yield* registry.append({}, [{ folderId: "before" }]);
      const scope = yield* Effect.scope;
      const started = new Set<string>();

      const triggers = Projection.process({
        id: "assumption-triggers",
        params: {},
        inputs: { registry: Registry },
        commit: Commit.atLeastOnce,
        process: Projection.each(({ item }) =>
          Effect.gen(function* () {
            if (started.has(item.folderId)) return;
            const bound = emitted.at(item);
            yield* Projection.run(bound);
            yield* Projection.follow(bound).pipe(Effect.provideService(Scope.Scope, scope));
            started.add(item.folderId);

            if (item.folderId === "after") yield* Deferred.succeed(seen, undefined);
          }),
        ),
      }).at({});

      yield* Projection.run(triggers);
      const follower = yield* Projection.follow(triggers);
      yield* files.create({ folderId: "after" });
      yield* files.append({ folderId: "after" }, ["second"]);
      yield* registry.append({}, [{ folderId: "after" }]);
      yield* Deferred.await(seen);
      expect(started).toEqual(new Set(["before", "after"]));
      expect((yield* shared.collect({})).items).toEqual([
        { folderId: "before", text: "first" },
        { folderId: "after", text: "second" },
      ]);
      yield* Fiber.interrupt(follower);
    }),
  );
});
