import { BunServices } from "@effect/platform-bun";
import { describe, expect, layer } from "@effect/vitest";
import { Effect, Exit, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { spawnServer, UncleanStop } from "../server.ts";

const services = Layer.mergeAll(BunServices.layer, FetchHttpClient.layer);

const fixture = (mode: string) =>
  spawnServer({
    cwd: import.meta.dirname,
    entry: "fixtures/server.ts",
    env: { FIXTURE_MODE: mode },
  });

layer(services, { excludeTestServices: true })((it) => {
  describe("spawnServer", () => {
    it.effect("stop and the scope finalizer fail with UncleanStop on a non-zero exit code", () =>
      Effect.gen(function* () {
        const exit = yield* Effect.scoped(
          Effect.flatMap(fixture("interrupted"), (server) => Effect.flip(server.stop)),
        ).pipe(Effect.exit);

        expect(Exit.isSuccess(exit)).toBe(true);

        if (Exit.isSuccess(exit)) {
          expect(exit.value).toBeInstanceOf(UncleanStop);
          expect(exit.value.exitCode).toBe(130);
        }

        const finalized = yield* Effect.scoped(Effect.asVoid(fixture("interrupted"))).pipe(
          Effect.exit,
        );

        expect(Exit.isFailure(finalized)).toBe(true);
        expect(String(finalized)).toContain("UncleanStop");
      }),
    );
    it.effect("stop fails with UncleanStop on stderr output", () =>
      Effect.gen(function* () {
        const exit = yield* Effect.scoped(
          Effect.flatMap(fixture("stderr"), (server) => Effect.flip(server.stop)),
        ).pipe(Effect.exit);

        expect(Exit.isSuccess(exit)).toBe(true);

        if (Exit.isSuccess(exit)) {
          expect(exit.value.exitCode).toBe(0);
          expect(exit.value.stderr).toContain("fixture wrote to stderr");
        }
      }),
    );
  });
});
