import { effect, expect } from "../src/index.ts";
import { Effect } from "effect";

const fixtures = new URL("../fixtures/", import.meta.url).pathname;

const child = (names: ReadonlyArray<string>, args: ReadonlyArray<string> = []) =>
  Effect.gen(function* () {
    const process = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.spawn(["bun", "test", ...names.map((name) => fixtures + name), ...args], {
          stdout: "pipe",
          stderr: "pipe",
        }),
      ),
      (childProcess) =>
        Effect.sync(() => {
          if (childProcess.exitCode === null) childProcess.kill();
        }),
    );

    const [exitCode, stdout, stderr] = yield* Effect.all(
      [
        Effect.promise(() => process.exited),
        Effect.promise(() => Bun.readableStreamToText(process.stdout)),
        Effect.promise(() => Bun.readableStreamToText(process.stderr)),
      ],
      { concurrency: "unbounded" },
    );

    return { exitCode, output: stdout + stderr };
  });

effect("Bun timeouts finish finalizers across module-cached test files", () =>
  Effect.gen(function* () {
    const result = yield* child(
      ["timeout-first.fixture.ts", "timeout-second.fixture.ts"],
      ["--timeout", "30"],
    );

    expect(result.exitCode).toBe(1);
    expect(result.output).toContain("2 pass");
    expect(result.output).toContain("2 fail");
    expect(result.output).not.toContain("Unhandled error");
    expect(result.output).not.toContain("Received: false");
  }),
);

effect.each(["setup-failure.fixture.ts", "setup-timeout.fixture.ts"])(
  "suite setup releases its resources: %s",
  (name) =>
    Effect.gen(function* () {
      const result = yield* child([name]);
      expect(result.exitCode).toBe(1);
      expect(result.output).toContain("setup finalizers completed");
      expect(result.output).not.toContain("Received: false");
    }),
);

effect("numeric and options timeouts pass through and await finalizers", () =>
  Effect.gen(function* () {
    const result = yield* child(["explicit-timeouts.fixture.ts"]);
    expect(result.exitCode).toBe(1);
    expect(result.output).toContain("2 pass");
    expect(result.output).toContain("2 fail");
    expect(result.output).toContain("timed out after 30ms");
    expect(result.output).not.toContain("Unhandled error");
    expect(result.output).not.toContain("Received: false");
  }),
);
