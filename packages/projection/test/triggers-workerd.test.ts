/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/global-fetch -- The test owns a real workerd process and persistent object storage. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test } from "vitest";
import { Array, Config, Effect, Schema } from "effect";
import { Miniflare } from "miniflare";

const scratch = Effect.runSync(
  Config.String("STREAMSY_PROJECTION_SCRATCH").pipe(Config.withDefault(tmpdir())),
);

const cleanups: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  for (const cleanup of Array.reverse(cleanups.splice(0))) await cleanup();
});

const Status = Schema.Struct({
  runs: Schema.Array(Schema.Struct({ folder: Schema.String, count: Schema.Finite })),
  runners: Schema.Array(Schema.Struct({ queued: Schema.Finite, active: Schema.Finite })),
});

const start = async (root: string, scriptPath: string) => {
  const mf = new Miniflare({
    rootPath: root,
    modulesRoot: join(root, "bundle"),
    scriptPath,
    modules: true,
    compatibilityDate: "2026-08-06",
    cf: false,
    host: "127.0.0.1",
    port: 0,
    durableObjects: { INDEX: { className: "TriggerObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  });

  const url = await mf.ready;
  let closed = false;

  const stop = async () => {
    if (!closed) {
      closed = true;
      await mf.dispose();
    }
  };

  cleanups.push(stop);

  const send = async (path: string, method = "GET") => {
    const response = await fetch(new URL(path, url), { method });

    if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`);

    return response;
  };

  const status = async () => Schema.decodeUnknownSync(Status)(await (await send("/status")).json());

  return { send, status, stop };
};

test("trigger-only commits arm the object alarm; persisted cursor resumes a row written before eviction", async () => {
  const root = mkdtempSync(join(scratch, "triggers-workerd-"));
  cleanups.push(() => rmSync(root, { recursive: true, force: true }));

  const built = await Bun.build({
    entrypoints: [join(import.meta.dirname, "support/triggers-worker.ts")],
    outdir: join(root, "bundle"),
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });

  if (!built.success || built.outputs[0] === undefined)
    throw new Error("No trigger workerd bundle");
  const scriptPath = built.outputs[0].path;
  const first = await start(root, scriptPath);
  expect((await first.status()).runs).toEqual([]);
  await first.send("/request/docs", "POST");

  const until = async (host: Awaited<ReturnType<typeof start>>, length: number) => {
    for (let n = 0; n < 200; n++) {
      const status = await host.status();

      if (
        status.runs.length === length &&
        Array.every(status.runners, (runner) => runner.queued === 0 && runner.active === 0)
      )
        return status.runs;
      await Bun.sleep(25);
    }

    throw new Error("Trigger did not drain");
  };

  expect(await until(first, 1)).toEqual([{ folder: "docs", count: 1 }]);
  await first.send("/quiet/later");
  await first.stop();
  const second = await start(root, scriptPath);
  expect(await until(second, 2)).toEqual([
    { folder: "docs", count: 1 },
    { folder: "later", count: 1 },
  ]);
  await second.stop();
  const third = await start(root, scriptPath);
  expect((await third.status()).runs).toEqual([
    { folder: "docs", count: 1 },
    { folder: "later", count: 1 },
  ]);
}, 30_000);

for (const after of [false, true]) {
  test(`workerd rolls back trigger marks and cursor on failure ${after ? "after" : "before"} the cursor write`, async () => {
    const root = mkdtempSync(join(scratch, "triggers-atomic-"));
    cleanups.push(() => rmSync(root, { recursive: true, force: true }));

    const built = await Bun.build({
      entrypoints: [join(import.meta.dirname, "support/triggers-worker.ts")],
      outdir: join(root, "bundle"),
      target: "browser",
      format: "esm",
      external: ["cloudflare:workers"],
    });

    if (!built.success || built.outputs[0] === undefined)
      throw new Error("No trigger workerd bundle");
    const first = await start(root, built.outputs[0].path);
    await first.send("/quiet/docs");
    expect(
      Schema.decodeUnknownSync(
        Schema.Struct({
          failed: Schema.Boolean,
          cursor: Schema.NullOr(Schema.String),
          marks: Schema.Finite,
        }),
      )(await (await first.send(`/atomic/${after}`)).json()),
    ).toEqual({ failed: true, cursor: null, marks: 0 });
    expect((await first.status()).runs).toEqual([]);
    await first.stop();
    const second = await start(root, built.outputs[0].path);

    for (let n = 0; n < 200; n++) {
      if ((await second.status()).runs.length === 1) break;
      await Bun.sleep(25);
    }

    expect((await second.status()).runs).toEqual([{ folder: "docs", count: 1 }]);
  }, 30_000);
}
