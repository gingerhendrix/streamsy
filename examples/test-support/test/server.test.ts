import { BunServices } from "@effect/platform-bun";
import { describe, expect, test } from "bun:test";
import { Effect, Exit, Layer } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { ServerStartError, spawnServer, UncleanStop } from "../server.ts";

const services = Layer.mergeAll(BunServices.layer, FetchHttpClient.layer);

const fixture = (mode: string) =>
  spawnServer({ cwd: import.meta.dir, entry: "fixtures/server.ts", env: { FIXTURE_MODE: mode } });

const run = <A, E>(effect: Effect.Effect<A, E, BunServices.BunServices | HttpClient.HttpClient>) =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test is an entry point.
  Effect.runPromise(Effect.exit(effect.pipe(Effect.provide(services))));

describe("spawnServer", () => {
  test("waits for the port, then stops cleanly", async () => {
    const exit = await run(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* fixture("clean");
          const response = yield* HttpClient.get(server.url);
          expect(yield* response.text).toBe("ok");
          yield* server.stop;
          return server.port;
        }),
      ),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
  });

  test("stop fails with UncleanStop on a non-zero exit code", async () => {
    const exit = await run(
      Effect.scoped(Effect.flatMap(fixture("interrupted"), (server) => Effect.flip(server.stop))),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    if (Exit.isSuccess(exit)) {
      expect(exit.value).toBeInstanceOf(UncleanStop);
      expect(exit.value.exitCode).toBe(130);
    }
  });

  test("stop fails with UncleanStop on stderr output", async () => {
    const exit = await run(
      Effect.scoped(Effect.flatMap(fixture("stderr"), (server) => Effect.flip(server.stop))),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    if (Exit.isSuccess(exit)) {
      expect(exit.value.exitCode).toBe(0);
      expect(exit.value.stderr).toContain("fixture wrote to stderr");
    }
  });

  test("the scope finalizer applies the same check", async () => {
    const exit = await run(Effect.scoped(Effect.asVoid(fixture("interrupted"))));
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).toContain("UncleanStop");
  });

  test("fails with ServerStartError when the process exits first", async () => {
    const exit = await run(Effect.scoped(Effect.flip(fixture("crash"))));
    expect(Exit.isSuccess(exit)).toBe(true);
    if (Exit.isSuccess(exit)) {
      expect(exit.value).toBeInstanceOf(ServerStartError);
      expect(exit.value.message).toContain("exit code 1");
    }
  });
});
