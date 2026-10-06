import { expect, it as baseIt } from "@streamsy/effect-test";
import { Cause, Effect, Exit } from "effect";
import { spawnHost } from "./support/spawned-host.ts";

baseIt.live("readiness hands off a running child whose stop waits for its exit", () =>
  Effect.gen(function* () {
    const source =
      'process.stdout.write(JSON.stringify({ listening: "http://fixture.test" }) + "\\n"); setInterval(() => {}, 1000);';

    const host = yield* Effect.acquireRelease(
      Effect.promise(() => spawnHost(["bun", "-e", source], {})),
      (child) => Effect.promise(() => child.stop()),
    );

    expect(host.url).toBe("http://fixture.test");
    expect(host.output()).toContain('"listening":"http://fixture.test"');
    yield* Effect.promise(() => host.stop());
    // Repeated cleanup resolves through the already-exited child path.
    yield* Effect.promise(() => host.stop());
  }),
);

baseIt.live("a child that exits before readiness preserves its startup output", () =>
  Effect.gen(function* () {
    const result = yield* Effect.promise(() =>
      spawnHost(["bun", "-e", 'process.stderr.write("startup failed\\n"); process.exit(7);'], {}),
    ).pipe(Effect.exit);

    expect(Exit.isFailure(result)).toBe(true);

    if (Exit.isFailure(result))
      expect(String(Cause.squash(result.cause))).toContain(
        "Host exited with 7 before it listened:\nstartup failed",
      );
  }),
);

baseIt.live("a child that never becomes ready is killed at the startup deadline", () =>
  Effect.gen(function* () {
    const result = yield* Effect.promise(() =>
      spawnHost(["bun", "-e", "setInterval(() => {}, 1000);"], {}, 100),
    ).pipe(Effect.exit);

    expect(Exit.isFailure(result)).toBe(true);

    if (Exit.isFailure(result))
      expect(String(Cause.squash(result.cause))).toContain("Host did not start within 100 ms");
  }),
);
