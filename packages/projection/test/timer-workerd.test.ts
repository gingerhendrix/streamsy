/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- Miniflare owns this local workerd lifecycle; state is retained across eviction. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test } from "vitest";
import { Array, Config, Effect, Schema } from "effect";
import { Miniflare } from "miniflare";

const scratch = Effect.runSync(
  Config.String("STREAMSY_PROJECTION_SCRATCH").pipe(Config.withDefault(tmpdir())),
);

const cleanup: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  for (const close of Array.reverse(cleanup.splice(0))) await close();
});

const Status = Schema.Struct({
  now: Schema.Finite,
  wakes: Schema.Array(Schema.Finite),
  alarm: Schema.NullOr(Schema.Finite),
  status: Schema.Struct({
    marks: Schema.Struct({
      sleeping: Schema.Finite,
      nextDueAt: Schema.Struct({ _tag: Schema.String, value: Schema.optionalKey(Schema.Finite) }),
    }),
  }),
});

test("workerd snooze moves, cancels, arms at sleep time and survives eviction", async () => {
  const root = mkdtempSync(join(scratch, ".timer-workerd-"));
  cleanup.push(() => rmSync(root, { recursive: true, force: true }));

  const built = await Bun.build({
    entrypoints: [join(import.meta.dirname, "support/timer-worker.ts")],
    outdir: join(root, "bundle"),
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });

  if (!built.success || !built.outputs[0]) throw new Error("timer worker build failed");

  const options = {
    rootPath: root,
    modulesRoot: join(root, "bundle"),
    scriptPath: built.outputs[0].path,
    modules: true,
    compatibilityDate: "2026-08-06",
    cf: false,
    durableObjects: { TIMERS: { className: "TimerObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  };

  let mf = new Miniflare(options);
  cleanup.push(async () => {
    try {
      await mf.dispose();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
  await mf.ready;

  const send = async (path: string) => {
    const response = await mf.dispatchFetch(`http://timer.test${path}`);

    if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`);

    return response;
  };

  const status = async () => Schema.decodeUnknownSync(Status)(await (await send("/status")).json());
  const start = Date.UTC(2099, 0, 1);
  await send(`/snooze/${start + 100}`);
  await send("/tick");
  expect(await status()).toMatchObject({
    alarm: start + 100,
    status: { marks: { sleeping: 1 } },
    wakes: [],
  });
  await send(`/time/${start + 50}`);
  await send(`/snooze/${start + 200}`);
  await send("/tick");
  expect((await status()).status.marks.nextDueAt.value).toBe(start + 200);
  await send(`/time/${start + 100}`);
  await send("/tick");
  expect((await status()).wakes).toEqual([]);
  await send("/snooze/0");
  await send("/tick");
  expect(await status()).toMatchObject({ alarm: null, status: { marks: { sleeping: 0 } } });
  await send(`/snooze/${start + 300}`);
  await send("/tick");
  expect((await status()).alarm).toBe(start + 300);
  await mf.dispose();
  mf = new Miniflare(options);
  await mf.ready;
  expect(await status()).toMatchObject({
    alarm: start + 300,
    status: { marks: { sleeping: 1 } },
    wakes: [],
  });
  await send(`/time/${start + 299}`);
  await send("/tick");
  expect((await status()).wakes).toEqual([]);
  await send(`/time/${start + 300}`);
  await send("/tick");
  expect(await status()).toMatchObject({
    alarm: null,
    wakes: [start + 300],
    status: { marks: { sleeping: 0 } },
  });
}, 30_000);
