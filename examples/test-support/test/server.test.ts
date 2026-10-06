import { BunServices } from "@effect/platform-bun";
import { describe, expect, test } from "bun:test";
import { Effect, Exit, Layer } from "effect";
import { FetchHttpClient, HttpClient } from "effect/http";
import { spawnServer, UncleanStop } from "../server.ts";

const services = Layer.mergeAll(BunServices.layer, FetchHttpClient.layer);

const fixture = (mode: string) =>
  spawnServer({ cwd: import.meta.dir, entry: "fixtures/server.ts", env: { FIXTURE_MODE: mode } });

const run = <A, E>(effect: Effect.Effect<A, E, BunServices.BunServices | HttpClient.HttpClient>) =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test is an entry point.
  Effect.runPromise(Effect.exit(effect.pipe(Effect.provide(services))));

describe("spawnServer", () => {
  test("stop and the scope finalizer fail with UncleanStop on a non-zero exit code", async () => {
    const exit = await run(
      Effect.scoped(Effect.flatMap(fixture("interrupted"), (server) => Effect.flip(server.stop))),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    if (Exit.isSuccess(exit)) {
      expect(exit.value).toBeInstanceOf(UncleanStop);
      expect(exit.value.exitCode).toBe(130);
    }
    const finalized = await run(Effect.scoped(Effect.asVoid(fixture("interrupted"))));
    expect(Exit.isFailure(finalized)).toBe(true);
    expect(String(finalized)).toContain("UncleanStop");
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
});
