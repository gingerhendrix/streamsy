/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/global-fetch -- This test owns the native local workerd boundary. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test } from "vitest";
import { Miniflare } from "miniflare";
import { Array, Config, Effect, Option, Schema } from "effect";

const scratch = Effect.runSync(
  Config.String("STREAMSY_PROJECTION_SCRATCH").pipe(Config.withDefault(tmpdir())),
);

const cleanups: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  for (const cleanup of Array.reverse(cleanups.splice(0))) await cleanup();
});

const start = async () => {
  const root = mkdtempSync(join(scratch, "registry-workerd-"));
  cleanups.push(() => rmSync(root, { recursive: true, force: true }));

  const built = await Bun.build({
    entrypoints: [join(import.meta.dirname, "support/registry-worker.ts")],
    outdir: join(root, "bundle"),
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });

  if (!built.success || built.outputs[0] === undefined)
    throw new Error("No registry workerd bundle");

  const mf = new Miniflare({
    rootPath: root,
    modulesRoot: join(root, "bundle"),
    scriptPath: built.outputs[0].path,
    modules: true,
    compatibilityDate: "2026-08-06",
    host: "127.0.0.1",
    port: 0,
    cf: false,
    durableObjects: {
      DURABLE: { className: "DurableRegistryObject", useSQLite: true },
      MEMORY: { className: "MemoryRegistryObject", useSQLite: true },
      EXHAUSTED: { className: "ExhaustedRegistryObject", useSQLite: true },
    },
  });

  cleanups.push(() => mf.dispose());
  const url = await mf.ready;

  const send = async (path: string) => {
    const response = await fetch(new URL(path, url));

    if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`);

    return response;
  };

  return send;
};

const RetryAt = Schema.Struct({ _tag: Schema.String, value: Schema.optionalKey(Schema.Finite) });

const Status = Schema.Struct({
  runs: Schema.Finite,
  seen: Schema.Array(Schema.String),
  attempts: Schema.NullOr(Schema.Finite),
  alarm: Schema.NullOr(Schema.Finite),
  status: Schema.Struct({ failures: Schema.Finite, retryAt: RetryAt, state: Schema.String }),
  mark: Schema.optionalKey(Schema.Struct({ attempts: Schema.Finite, dueAt: Schema.Finite })),
  host: Schema.Array(
    Schema.Struct({ name: Schema.String, failures: Schema.Finite, retryAt: RetryAt }),
  ),
});

type Status = typeof Status.Type;

const statusOf = async (response: Promise<Response>) =>
  Schema.decodeUnknownSync(Status)(await (await response).json());

const scheduledOf = async (response: Promise<Response>) =>
  Schema.decodeUnknownSync(
    Schema.Struct({ alarm: Schema.NullOr(Schema.Finite), status: Status.fields.status }),
  )(await (await response).json());

const retriedOf = async (response: Promise<Response>) =>
  Schema.decodeUnknownSync(
    Schema.Struct({
      count: Schema.Finite,
      alarm: Schema.NullOr(Schema.Finite),
      status: Status.fields.status,
    }),
  )(await (await response).json());

const until = async (read: () => Promise<Status>, check: (status: Status) => boolean) => {
  for (let n = 0; n < 100; n += 1) {
    const status = await read();

    if (check(status)) return status;
    await Bun.sleep(25);
  }

  throw new Error(`Timed out: ${JSON.stringify(await read())}`);
};

for (const kind of ["durable", "memory"]) {
  test(`registry emits Ran while a request waits on events (${kind})`, async () => {
    const send = await start();
    const path = `/${kind}/event`;
    await send(`${path}/setup`);

    const event = Schema.decodeUnknownSync(
      Schema.TaggedStruct("Ran", {
        projection: Schema.String,
        params: Schema.Record(Schema.String, Schema.String),
        status: Schema.String,
        units: Schema.Finite,
        offsets: Schema.Struct({ source: Schema.String }),
      }),
    )(await (await send(`${path}/event`)).json());

    expect(event).toMatchObject({
      _tag: "Ran",
      projection: "workerd-registry",
      params: { id: "a" },
      status: "caught-up",
      units: 1,
    });
    expect(event.offsets.source).toMatch(/^\d{16}_\d{16}$/);
  }, 30_000);

  test(`registry Failed carries only a durable binding deadline (${kind})`, async () => {
    const send = await start();
    const path = `/${kind}/failed-event`;
    await send(`${path}/setup`);
    await send(`${path}/mode/binding`);

    const event = Schema.decodeUnknownSync(
      Schema.TaggedStruct("Failed", {
        projection: Schema.String,
        params: Schema.Record(Schema.String, Schema.String),
        attempts: Schema.Finite,
        dueAt: RetryAt,
      }),
    )(await (await send(`${path}/event`)).json());

    expect(event).toMatchObject({
      _tag: "Failed",
      projection: "workerd-registry",
      params: { id: "a" },
      attempts: 1,
    });
    const snapshot = await statusOf(send(`${path}/status`));
    expect(event.dueAt).toEqual(
      kind === "durable" ? { _tag: "Some", value: snapshot.mark?.dueAt } : { _tag: "None" },
    );
  }, 30_000);

  test(`registry.schedule from a request arms and runs without another commit (${kind})`, async () => {
    const send = await start();
    const path = `/${kind}/schedule`;
    await send(`${path}/setup`);
    const scheduled = await scheduledOf(send(`${path}/schedule`));
    expect(scheduled.alarm).not.toBeNull();

    const status = await until(
      () => statusOf(send(`${path}/status`)),
      (snapshot) => snapshot.runs === 1 && snapshot.alarm === null,
    );

    expect(status.status).toMatchObject({
      state: "running",
      failures: 0,
      retryAt: { _tag: "None" },
    });
  }, 30_000);
}

test("durable retry from a request preserves attempts, arms the alarm, and runs the binding", async () => {
  const send = await start();
  const path = "/durable/retry";
  await send(`${path}/setup`);
  await send(`${path}/mode/binding`);
  await send(`${path}/schedule`);
  await until(
    () => statusOf(send(`${path}/status`)),
    (snapshot) => snapshot.mark?.attempts === 1,
  );
  await send(`${path}/defer`);
  await send(`${path}/mode/ok`);
  const retried = await retriedOf(send(`${path}/retry`));
  expect(retried.count).toBe(1);
  expect(retried.alarm).not.toBeNull();

  const status = await until(
    () => statusOf(send(`${path}/status`)),
    (snapshot) => snapshot.runs >= 2 && snapshot.mark === undefined,
  );

  expect(status.status.failures).toBe(0);
  expect(status.attempts).toBe(4);
}, 30_000);

for (const mode of ["binding", "storage"]) {
  test(`non-durable retry clears ${mode} failure backoff on workerd`, async () => {
    const send = await start();
    const path = `/memory/${mode}`;
    await send(`${path}/setup`);
    await send(`${path}/mode/${mode}`);
    await send(`${path}/schedule`);
    const read = () => statusOf(send(`${path}/status`));
    const failed = await until(read, (status) => status.status.state === "retrying");

    const host = Option.getOrThrow(
      Array.findFirst(failed.host, (entry) => entry.name.startsWith("streamsy/projection/")),
    );

    expect(failed.status.failures).toBe(1);
    expect(failed.status.retryAt).toEqual(host.retryAt);
    expect(failed.status.failures).toBe(host.failures);
    await send(`${path}/mode/ok`);
    const retried = await retriedOf(send(`${path}/retry`));
    expect(retried.count).toBe(1);
    expect(retried.alarm).not.toBeNull();
    expect(retried.status).toMatchObject({
      state: "retrying",
      retryAt: { _tag: "Some", value: retried.alarm },
    });

    const recovered = await until(
      read,
      (status) => status.runs >= 2 && status.status.failures === 0,
    );

    expect(recovered.status).toMatchObject({ state: "running", retryAt: { _tag: "None" } });
  }, 30_000);
}

test("workerd delivers an alarm while fetch waits on registry.changes", async () => {
  const send = await start();
  const path = "/durable/wait";
  await send(`${path}/setup`);
  expect(await (await send(`${path}/wait`)).json()).toEqual({ observed: true });
}, 30_000);

for (const kind of ["memory", "exhausted"]) {
  test(`schedule runs a healthy binding during application backoff (${kind})`, async () => {
    const send = await start();
    const path = `/${kind}/schedule-after-failure`;
    await send(`${path}/setup`);
    await send(`${path}/mode/binding`);
    await send(`${path}/schedule`);
    const read = () => statusOf(send(`${path}/status`));
    const failed = await until(read, (snapshot) => snapshot.status.failures === 1);
    expect(failed.status.state).toBe(kind === "exhausted" ? "failed" : "retrying");
    expect(failed.alarm === null).toBe(kind === "exhausted");

    const scheduled = await scheduledOf(send(`${path}/schedule-good`));
    expect(scheduled.alarm).not.toBeNull();
    expect(scheduled.status).toMatchObject({
      state: "retrying",
      failures: 1,
      retryAt: { _tag: "Some", value: scheduled.alarm },
    });
    const ran = await until(read, (snapshot) => snapshot.seen.includes("good"));
    expect(ran.seen).toEqual(["a", "a", "good"]);
  }, 30_000);
}

for (const kind of ["memory", "exhausted"]) {
  for (const request of ["schedule-good", "retry"]) {
    test(`${request} starts one pass after a storage fault (${kind})`, async () => {
      const send = await start();
      const path = `/${kind}/storage-${request}`;
      await send(`${path}/setup`);
      await send(`${path}/mode/storage`);
      await send(`${path}/schedule`);
      const read = () => statusOf(send(`${path}/status`));
      await until(read, (snapshot) => snapshot.status.failures === 1);
      await send(`${path}/mode/ok`);
      const requested = await scheduledOf(send(`${path}/${request}`));
      expect(requested.alarm).not.toBeNull();
      expect(requested.status).toMatchObject({ state: "retrying", retryAt: { _tag: "Some" } });
      const recovered = await until(read, (snapshot) => snapshot.status.failures === 0);
      expect(recovered.seen).toEqual(request === "retry" ? ["a", "a"] : ["a", "a", "good"]);
    }, 30_000);
  }
}
