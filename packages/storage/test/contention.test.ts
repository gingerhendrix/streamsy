/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/global-fetch, effecttsgo/prefer-schema-over-json -- Real Bun/process/HTTP contention edge writes exact numeric evidence JSON. */
import { Database } from "bun:sqlite";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { expect, test } from "bun:test";
import {
  Cause,
  Config,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Scope,
  Stream,
} from "effect";
import {
  Offset,
  Protocol,
  Storage,
  StreamId,
  ZERO_OFFSET,
  type Mutation,
  type StorageFault,
} from "@streamsy/core";
import {
  testHost,
  type TestHost,
  type TestHostOptions,
} from "../../serve/test/support/scoped-host.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError } from "effect/unstable/sql/SqlError";
import { layer as bunStorageLayer } from "@streamsy/storage/bun";
import { CommitBoundary } from "@streamsy/storage";
import { makeBoundaryTestProbe, sharedSqlClientLayer } from "../src/boundary.ts";
import { layerWithTestProbe } from "../src/storage.ts";

/**
 * Start one Bun host for this suite. `testHost` owns the host scope, so the
 * listener stays up until the returned `stop` closes it.
 */
const startHost = (options: TestHostOptions<StorageFault>): Promise<TestHost> =>
  Effect.runPromise(Effect.orDie(testHost(options)));

const stopHost = (host: TestHost): Promise<void> => Effect.runPromise(host.stop);

const scratch = Effect.runSync(
  Config.String("STREAMSY_STORAGE_SCRATCH").pipe(Config.withDefault("/tmp")),
);
const holderWorker = new URL("./support/contention-process-worker.ts", import.meta.url).pathname;
const one = Offset.make("0000000000000001_0000000000000000");

const record = (id: string) => ({
  id: StreamId.make(id),
  config: { contentType: "text/plain", createdAt: 0 },
  lifecycle: { closed: false, softDeleted: false },
  currentOffset: ZERO_OFFSET,
});

const create = (id: string): Mutation => ({
  operations: [{ _tag: "Create" as const, record: record(id), initialMessages: [] }],
});

const append = (id: string, text: string): Mutation => ({
  operations: [
    {
      _tag: "Append" as const,
      streamId: StreamId.make(id),
      expectedOffset: ZERO_OFFSET,
      messages: [{ offset: one, timestamp: 1, data: new TextEncoder().encode(text) }],
      patch: { currentOffset: one },
    },
  ],
});

const startHolder = async (
  filename: string,
  mode: "write" | "read" | "exclusive",
  holdMs: number,
) => {
  const child = Bun.spawn([process.execPath, holderWorker, mode, filename, String(holdMs)], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const reader = child.stdout.getReader();
  const ready = await reader.read();
  reader.releaseLock();
  if (ready.done || new TextDecoder().decode(ready.value).trim() !== "locked") {
    const stderr = await new Response(child.stderr).text();
    throw new Error(`Lock holder failed: ${stderr}`);
  }
  return child;
};

const makeTestLayer = (
  filename: string,
  options: { readonly disableWAL?: boolean; readonly repairIntervalMs?: number } = {},
) => {
  const probe = makeBoundaryTestProbe();
  const client = Layer.effectContext(
    sharedSqlClientLayer(
      SqliteClient.make({
        filename,
        busyTimeout: 0,
        disableWAL: options.disableWAL,
      }),
    ),
  );
  const storage = layerWithTestProbe(
    {
      repairIntervalMs: options.repairIntervalMs ?? 60_000,
      transactionRetryAttempts: 16,
      transactionRetryDelayMs: 25,
    },
    probe,
  ).pipe(Layer.provideMerge(client));
  return { storage, probe };
};

const waitFor = async (predicate: () => boolean, timeoutMs = 1_000) => {
  const deadline = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() >= deadline) throw new Error("Timed out waiting for lifecycle state");
    await Bun.sleep(5);
  }
};

const prepareWalFile = (filename: string) => {
  const database = new Database(filename, { create: true });
  database.run("PRAGMA journal_mode=WAL");
  database.run("CREATE TABLE batch_c_lock(value INTEGER NOT NULL)");
  database.run("INSERT INTO batch_c_lock VALUES (0)");
  database.close(false);
};

const prepareRollbackFile = (filename: string) => {
  const database = new Database(filename, { create: true });
  database.run("PRAGMA journal_mode=DELETE");
  database.run("CREATE TABLE batch_c_lock(value INTEGER NOT NULL)");
  database.run("INSERT INTO batch_c_lock VALUES (0)");
  database.close(false);
};

const streamsyTables = (filename: string) => {
  const database = new Database(filename, { readonly: true });
  const tables = database
    .query<{ name: string }, []>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'streamsy_%' ORDER BY name",
    )
    .all()
    .map(({ name }) => name);
  database.close(false);
  return tables;
};

const buildPublicLayer = (filename: string, disableWAL = false) =>
  Effect.runPromiseExit(
    Effect.scoped(
      Layer.build(
        bunStorageLayer({
          client: { filename, disableWAL },
          transactionRetryAttempts: 16,
          transactionRetryDelayMs: 25,
        }),
      ),
    ),
  );

test("public Bun acquisition bounds preflight, WAL preparation and migration contention", async () => {
  const measure = async (
    filename: string,
    mode: "write" | "exclusive",
    holdMs: number,
    disableWAL: boolean,
  ) => {
    const holder = await startHolder(filename, mode, holdMs);
    const started = performance.now();
    const exit = await buildPublicLayer(filename, disableWAL);
    const elapsedMs = performance.now() - started;
    await holder.exited;
    return { exit, elapsedMs };
  };

  const preflightProgress = `${scratch}/contention-public-preflight-progress-${process.pid}-${crypto.randomUUID()}.sqlite`;
  expect(Exit.isSuccess(await buildPublicLayer(preflightProgress, true))).toBe(true);
  const preflightDatabase = new Database(preflightProgress, { readwrite: true });
  preflightDatabase.run("PRAGMA journal_mode=DELETE");
  preflightDatabase.close(false);
  const progressedPreflight = await measure(preflightProgress, "exclusive", 150, true);
  expect(Exit.isSuccess(progressedPreflight.exit)).toBe(true);
  expect(progressedPreflight.elapsedMs).toBeGreaterThanOrEqual(100);
  expect(progressedPreflight.elapsedMs).toBeLessThanOrEqual(750);

  const preflightExhaustion = `${scratch}/contention-public-preflight-exhaustion-${process.pid}-${crypto.randomUUID()}.sqlite`;
  expect(Exit.isSuccess(await buildPublicLayer(preflightExhaustion, true))).toBe(true);
  const preflightExhausted = await measure(preflightExhaustion, "exclusive", 650, true);
  expect(Exit.isFailure(preflightExhausted.exit)).toBe(true);
  expect(preflightExhausted.elapsedMs).toBeLessThanOrEqual(750);
  if (Exit.isFailure(preflightExhausted.exit)) {
    const error = Option.getOrThrow(Cause.findErrorOption(preflightExhausted.exit.cause));
    expect(error).toMatchObject({ _tag: "StorageFault", retryable: true });
  }

  const walProgress = `${scratch}/contention-public-wal-progress-${process.pid}-${crypto.randomUUID()}.sqlite`;
  prepareRollbackFile(walProgress);
  const progressedWal = await measure(walProgress, "write", 150, false);
  expect(Exit.isSuccess(progressedWal.exit)).toBe(true);
  expect(progressedWal.elapsedMs).toBeGreaterThanOrEqual(100);
  expect(progressedWal.elapsedMs).toBeLessThanOrEqual(750);

  const walExhaustion = `${scratch}/contention-public-wal-exhaustion-${process.pid}-${crypto.randomUUID()}.sqlite`;
  prepareRollbackFile(walExhaustion);
  const exhaustedWal = await measure(walExhaustion, "write", 650, false);
  expect(Exit.isFailure(exhaustedWal.exit)).toBe(true);
  expect(exhaustedWal.elapsedMs).toBeLessThanOrEqual(750);
  if (Exit.isFailure(exhaustedWal.exit)) {
    const error = Option.getOrThrow(Cause.findErrorOption(exhaustedWal.exit.cause));
    expect(error).toMatchObject({ _tag: "StorageFault", retryable: true });
  }
  expect(streamsyTables(walExhaustion)).toEqual([]);

  const migrationProgress = `${scratch}/contention-public-migration-progress-${process.pid}-${crypto.randomUUID()}.sqlite`;
  prepareWalFile(migrationProgress);
  const progressedMigration = await measure(migrationProgress, "write", 150, true);
  expect(Exit.isSuccess(progressedMigration.exit)).toBe(true);
  expect(progressedMigration.elapsedMs).toBeGreaterThanOrEqual(100);
  expect(progressedMigration.elapsedMs).toBeLessThanOrEqual(750);

  const migrationExhaustion = `${scratch}/contention-public-migration-exhaustion-${process.pid}-${crypto.randomUUID()}.sqlite`;
  prepareWalFile(migrationExhaustion);
  const exhaustedMigration = await measure(migrationExhaustion, "write", 650, true);
  expect(Exit.isFailure(exhaustedMigration.exit)).toBe(true);
  expect(exhaustedMigration.elapsedMs).toBeLessThanOrEqual(750);
  if (Exit.isFailure(exhaustedMigration.exit)) {
    const error = Option.getOrThrow(Cause.findErrorOption(exhaustedMigration.exit.cause));
    expect(error).toMatchObject({ _tag: "StorageFault", retryable: true });
  }
  expect(streamsyTables(migrationExhaustion)).toEqual([]);

  await Bun.write(
    `${scratch}/contention-public-acquisition-metrics-${process.pid}.json`,
    JSON.stringify(
      {
        preflightProgressMs: progressedPreflight.elapsedMs,
        preflightExhaustionMs: preflightExhausted.elapsedMs,
        walProgressMs: progressedWal.elapsedMs,
        walExhaustionMs: exhaustedWal.elapsedMs,
        migrationProgressMs: progressedMigration.elapsedMs,
        migrationExhaustionMs: exhaustedMigration.elapsedMs,
      },
      undefined,
      2,
    ),
  );
});

test("owned retries yield, progress after release, exhaust exactly, and preserve state/wakes", async () => {
  const filename = `${scratch}/contention-owned-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const { storage: storageLayer, probe } = makeTestLayer(filename);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(storageLayer);
        const storage = Context.get(context, Storage);
        const sql = Context.get(context, SqlClient.SqlClient);
        const boundary = Context.get(context, CommitBoundary);
        yield* storage.mutate(create("eventual"));

        const released = yield* Effect.promise(() => startHolder(filename, "write", 150));
        const beforeProgress = probe.transactionAttempts;
        const progressStarted = performance.now();
        const progressed = yield* storage.mutate(append("eventual", "ok"));
        const progressElapsedMs = performance.now() - progressStarted;
        const progressAttempts = probe.transactionAttempts - beforeProgress;
        yield* Effect.promise(() => released.exited);
        expect(progressed).toMatchObject({ _tag: "Applied" });
        expect(progressAttempts).toBeGreaterThan(1);
        expect(progressAttempts).toBeLessThanOrEqual(16);
        expect(progressElapsedMs).toBeLessThanOrEqual(750);

        const scope = yield* Scope.make();
        const pull = yield* Stream.toPull(storage.changes(StreamId.make("exhausted"))).pipe(
          Scope.provide(scope),
        );
        yield* pull;
        const pending = yield* pull.pipe(Effect.forkScoped, Scope.provide(scope));
        const held = yield* Effect.promise(() => startHolder(filename, "write", 650));
        const rawBusy = yield* sql
          .unsafe("UPDATE batch_c_lock SET value=value+1")
          .pipe(Effect.exit);
        expect(Exit.isFailure(rawBusy) && Option.isSome(Cause.findErrorOption(rawBusy.cause))).toBe(
          true,
        );
        if (Exit.isFailure(rawBusy)) {
          const error = Option.getOrThrow(Cause.findErrorOption(rawBusy.cause));
          expect(isSqlError(error) && error.isRetryable).toBe(true);
        }
        const beforeCallerOwned = probe.transactionAttempts;
        const callerOwned = yield* boundary
          .withTransaction(sql.unsafe("UPDATE batch_c_lock SET value=value+1"))
          .pipe(Effect.exit);
        expect(probe.transactionAttempts - beforeCallerOwned).toBe(1);
        expect(Exit.isFailure(callerOwned)).toBe(true);
        const beforeExhaustion = probe.transactionAttempts;
        const exhaustionStarted = performance.now();
        const exhausted = yield* storage.mutate(create("exhausted")).pipe(Effect.exit);
        const exhaustionElapsedMs = performance.now() - exhaustionStarted;
        const exhaustionAttempts = probe.transactionAttempts - beforeExhaustion;
        expect(exhaustionAttempts).toBe(16);
        expect(exhaustionElapsedMs).toBeLessThanOrEqual(750);
        expect(Exit.isFailure(exhausted)).toBe(true);
        if (Exit.isFailure(exhausted)) {
          const error = Option.getOrThrow(Cause.findErrorOption(exhausted.cause));
          expect(error).toMatchObject({ _tag: "StorageFault", retryable: true });
        }
        expect(Option.isNone(yield* storage.record(StreamId.make("exhausted")))).toBe(true);
        expect(pending.pollUnsafe()).toBeUndefined();
        expect([...probe.queues].map((queue) => Queue.sizeUnsafe(queue))).toEqual([0]);
        yield* Effect.promise(() => held.exited);
        yield* Scope.close(scope, Exit.void);

        yield* sql.unsafe(
          "CREATE TRIGGER reject_batch_c BEFORE INSERT ON streamsy_streams BEGIN SELECT RAISE(ABORT, 'batch-c-reject'); END",
        );
        const beforePermanent = probe.transactionAttempts;
        const permanent = yield* storage.mutate(create("permanent")).pipe(Effect.exit);
        expect(probe.transactionAttempts - beforePermanent).toBe(1);
        expect(Exit.isFailure(permanent)).toBe(true);
        if (Exit.isFailure(permanent)) {
          const error = Option.getOrThrow(Cause.findErrorOption(permanent.cause));
          expect(error).toMatchObject({ _tag: "StorageFault", retryable: false });
        }
        yield* sql.unsafe("DROP TRIGGER reject_batch_c");

        const cancelHolder = yield* Effect.promise(() => startHolder(filename, "write", 650));
        const beforeCancellation = probe.transactionAttempts;
        const cancellationStarted = performance.now();
        const cancelledFiber = yield* storage.mutate(create("cancelled")).pipe(Effect.forkScoped);
        yield* Effect.sleep(50);
        yield* Fiber.interrupt(cancelledFiber);
        const cancelled = yield* Fiber.await(cancelledFiber);
        const cancellationElapsedMs = performance.now() - cancellationStarted;
        const cancellationAttempts = probe.transactionAttempts - beforeCancellation;
        expect(Exit.isFailure(cancelled) && Cause.hasInterrupts(cancelled.cause)).toBe(true);
        expect(cancellationAttempts).toBeGreaterThanOrEqual(1);
        expect(cancellationAttempts).toBeLessThan(16);
        expect(cancellationElapsedMs).toBeLessThanOrEqual(1_000);
        expect(Option.isNone(yield* storage.record(StreamId.make("cancelled")))).toBe(true);
        yield* Effect.promise(() => cancelHolder.exited);

        yield* Effect.promise(() =>
          Bun.write(
            `${scratch}/contention-owned-metrics-${process.pid}.json`,
            JSON.stringify(
              {
                progressElapsedMs,
                progressAttempts,
                exhaustionElapsedMs,
                exhaustionAttempts,
                cancellationElapsedMs,
                cancellationAttempts,
              },
              undefined,
              2,
            ),
          ),
        );
      }),
    ),
  );
});

test("migration acquisition retries are bounded and never expose partial schema", async () => {
  const eventualFile = `${scratch}/contention-migration-eventual-${process.pid}-${crypto.randomUUID()}.sqlite`;
  prepareWalFile(eventualFile);
  const eventual = makeTestLayer(eventualFile);
  const released = await startHolder(eventualFile, "write", 150);
  const started = performance.now();
  const eventualExit = await Effect.runPromiseExit(Effect.scoped(Layer.build(eventual.storage)));
  const elapsedMs = performance.now() - started;
  await released.exited;
  expect(Exit.isSuccess(eventualExit)).toBe(true);
  expect(eventual.probe.migrationAttempts).toBeGreaterThan(1);
  expect(eventual.probe.migrationAttempts).toBeLessThanOrEqual(16);
  expect(elapsedMs).toBeLessThanOrEqual(750);

  const exhaustedFile = `${scratch}/contention-migration-exhausted-${process.pid}-${crypto.randomUUID()}.sqlite`;
  prepareWalFile(exhaustedFile);
  const exhaustedLayer = makeTestLayer(exhaustedFile);
  const held = await startHolder(exhaustedFile, "write", 650);
  const exhaustionStarted = performance.now();
  const exhausted = await Effect.runPromiseExit(Effect.scoped(Layer.build(exhaustedLayer.storage)));
  const exhaustionElapsedMs = performance.now() - exhaustionStarted;
  expect(exhaustedLayer.probe.migrationAttempts).toBe(16);
  expect(exhaustionElapsedMs).toBeLessThanOrEqual(750);
  expect(Exit.isFailure(exhausted)).toBe(true);
  if (Exit.isFailure(exhausted)) {
    const error = Option.getOrThrow(Cause.findErrorOption(exhausted.cause));
    expect(error).toMatchObject({ _tag: "StorageFault", retryable: true });
  }
  await held.exited;
  const reopened = makeTestLayer(exhaustedFile);
  expect(await Effect.runPromiseExit(Effect.scoped(Layer.build(reopened.storage)))).toEqual(
    expect.objectContaining({ _tag: "Success" }),
  );
  const database = new Database(exhaustedFile, { readonly: true });
  expect(
    database.query("SELECT name FROM sqlite_master WHERE name='streamsy_streams'").get(),
  ).not.toBeNull();
  database.close(false);
  await Bun.write(
    `${scratch}/contention-migration-metrics-${process.pid}.json`,
    JSON.stringify(
      {
        eventualElapsedMs: elapsedMs,
        eventualAttempts: eventual.probe.migrationAttempts,
        exhaustionElapsedMs,
        exhaustionAttempts: exhaustedLayer.probe.migrationAttempts,
      },
      undefined,
      2,
    ),
  );
});

test("commit contention defects once and is never replayed", async () => {
  const filename = `${scratch}/contention-commit-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const { storage: storageLayer, probe } = makeTestLayer(filename, { disableWAL: true });
  const scope = await Effect.runPromise(Scope.make());
  const context = await Effect.runPromise(Layer.build(storageLayer).pipe(Scope.provide(scope)));
  const storage = Context.get(context, Storage);
  const reader = await startHolder(filename, "read", 300);
  const before = probe.transactionAttempts;
  const result = await Effect.runPromise(storage.mutate(create("uncertain")).pipe(Effect.exit));
  expect(Exit.isFailure(result) && Cause.hasDies(result.cause)).toBe(true);
  expect(probe.transactionAttempts - before).toBe(1);
  await Effect.runPromise(Scope.close(scope, Exit.void));
  await reader.exited;
  const database = new Database(filename, { readonly: true });
  expect(
    database
      .query<{ count: number }, []>(
        "SELECT COUNT(*) count FROM streamsy_streams WHERE stream_id='uncertain'",
      )
      .get()?.count,
  ).toBe(0);
  database.close(false);
  await Bun.write(
    `${scratch}/contention-commit-metrics-${process.pid}.json`,
    JSON.stringify(
      { attempts: probe.transactionAttempts - before, defected: true, finalRows: 0 },
      undefined,
      2,
    ),
  );
});

test("SQL host keeps unrelated HTTP responsive and cleans retries, SSE, long-poll and client scope", async () => {
  const filename = `${scratch}/contention-host-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const { storage: storageLayer, probe } = makeTestLayer(filename, { repairIntervalMs: 1_000 });
  const application = Protocol.layer({ longPollTimeoutMs: 30_000 }).pipe(
    Layer.provide(storageLayer),
  );
  const host = await startHost({ layer: application, port: 0 });
  const streamUrl = new URL("host", host.url);
  const created = await fetch(streamUrl, {
    method: "PUT",
    headers: { "content-type": "text/plain" },
  });
  expect(created.status).toBe(201);
  const tail = created.headers.get("stream-next-offset");
  if (tail === null) throw new Error("Missing tail");

  const longAbort = new AbortController();
  const sseAbort = new AbortController();
  const longUrl = new URL(streamUrl);
  longUrl.search = `offset=${tail}&live=long-poll`;
  const sseUrl = new URL(streamUrl);
  sseUrl.search = `offset=${tail}&live=sse`;
  const longPending = fetch(longUrl, { signal: longAbort.signal }).catch(() => undefined);
  const sseResponse = await fetch(sseUrl, { signal: sseAbort.signal });
  const sseReader = sseResponse.body?.getReader();
  if (sseReader === undefined) throw new Error("Missing SSE body");
  await sseReader.read();
  const ssePending = sseReader.read().catch(() => undefined);
  await waitFor(() => probe.activeRegistrations === 2 && probe.activeRepairFibers === 2);
  longAbort.abort();
  sseAbort.abort();
  await Promise.all([longPending, ssePending]);
  await waitFor(() => probe.activeRegistrations === 0 && probe.activeRepairFibers === 0);

  const held = await startHolder(filename, "write", 650);
  const appendPending = fetch(streamUrl, {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: "blocked",
  });
  const samples: Array<number> = [];
  for (let index = 0; index < 40; index++) {
    const sampleStarted = performance.now();
    const response = await fetch(new URL("latency", host.url), { method: "OPTIONS" });
    expect(response.status).toBe(204);
    samples.push(performance.now() - sampleStarted);
  }
  const blocked = await appendPending;
  expect(blocked.status).toBe(500);
  const ordered = samples.toSorted((left, right) => left - right);
  const p95 = ordered[Math.ceil(ordered.length * 0.95) - 1];
  const maximum = ordered.at(-1);
  expect(p95).toBeLessThanOrEqual(100);
  expect(maximum).toBeLessThanOrEqual(250);
  await held.exited;
  expect((await fetch(streamUrl)).headers.get("stream-next-offset")).toBe(tail);

  const stopLongPending = fetch(longUrl).catch(() => undefined);
  const stopSseResponse = await fetch(sseUrl);
  const stopSseReader = stopSseResponse.body?.getReader();
  if (stopSseReader === undefined) throw new Error("Missing shutdown SSE body");
  await stopSseReader.read();
  const stopSsePending = stopSseReader.read().catch(() => undefined);
  await waitFor(() => probe.activeRegistrations === 2 && probe.activeRepairFibers === 2);

  const shutdownHolder = await startHolder(filename, "write", 650);
  const beforeShutdownRetry = probe.transactionAttempts;
  const beforeShutdownInvalidations = probe.invalidations;
  const beforeShutdownCompletions = probe.completedMutations;
  const retrying = fetch(streamUrl, {
    method: "POST",
    headers: { "content-type": "text/plain" },
    body: "shutdown",
  }).catch(() => undefined);
  await waitFor(() => probe.transactionAttempts > beforeShutdownRetry);
  const stopStarted = performance.now();
  await stopHost(host);
  const stopElapsedMs = performance.now() - stopStarted;
  expect(stopElapsedMs).toBeLessThanOrEqual(1_000);
  expect(probe.completedMutations).toBe(beforeShutdownCompletions + 1);
  expect(probe.closed).toBe(true);
  expect(probe.activeRegistrations).toBe(0);
  expect(probe.activeRepairFibers).toBe(0);
  await Promise.all([retrying, stopLongPending, stopSsePending]);
  const attemptsAfterStop = probe.transactionAttempts;
  await Bun.sleep(100);
  expect(probe.transactionAttempts).toBe(attemptsAfterStop);
  await shutdownHolder.exited;
  await Bun.sleep(100);
  expect(probe.transactionAttempts).toBe(attemptsAfterStop);
  expect(probe.invalidations).toBe(beforeShutdownInvalidations);
  const stoppedDatabase = new Database(filename, { readonly: true });
  expect(
    stoppedDatabase
      .query<{ current_offset: string }, []>(
        "SELECT current_offset FROM streamsy_streams WHERE stream_id='host'",
      )
      .get()?.current_offset,
  ).toBe(tail);
  stoppedDatabase.close(false);
  await Bun.write(
    `${scratch}/contention-host-metrics-${process.pid}.json`,
    JSON.stringify(
      {
        samples: samples.length,
        p95Ms: p95,
        maximumMs: maximum,
        stopElapsedMs,
        retryAttemptsBeforeStop: attemptsAfterStop - beforeShutdownRetry,
        attemptsStableAfterStop: true,
        shutdownMutationCompleted: true,
      },
      undefined,
      2,
    ),
  );

  const rebound = await startHost({ layer: application, port: host.port });
  try {
    expect(rebound.port).toBe(host.port);
    expect((await fetch(new URL("rebound", rebound.url), { method: "PUT" })).status).toBe(201);
  } finally {
    await stopHost(rebound);
  }
});

test("a host-scoped held transaction is interrupted and rolled back on stop", async () => {
  const filename = `${scratch}/contention-held-shutdown-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const { storage: storageLayer, probe } = makeTestLayer(filename);
  const trigger = Deferred.makeUnsafe<void>();
  const acquired = Deferred.makeUnsafe<void>();
  const transactionHolder = Layer.effectDiscard(
    Effect.gen(function* () {
      const boundary = yield* CommitBoundary;
      yield* Deferred.await(trigger).pipe(
        Effect.andThen(
          boundary.withTransaction(
            Deferred.succeed(acquired, undefined).pipe(Effect.andThen(Effect.never)),
          ),
        ),
        Effect.forkScoped,
      );
    }),
  );
  const application = Layer.mergeAll(
    Protocol.layer({ longPollTimeoutMs: 30_000 }),
    transactionHolder,
  ).pipe(Layer.provide(storageLayer));
  const host = await startHost({ layer: application, port: 0 });
  expect((await fetch(new URL("held", host.url), { method: "PUT" })).status).toBe(201);
  await Effect.runPromise(Deferred.succeed(trigger, undefined));
  await Effect.runPromise(Deferred.await(acquired).pipe(Effect.timeout(1_000)));
  const stopStarted = performance.now();
  await stopHost(host);
  const stopElapsedMs = performance.now() - stopStarted;
  expect(stopElapsedMs).toBeLessThanOrEqual(1_000);
  expect(probe.closed).toBe(true);
  expect(probe.activeRegistrations).toBe(0);
  expect(probe.activeRepairFibers).toBe(0);
  const database = new Database(filename, { readwrite: true });
  database.run("BEGIN IMMEDIATE");
  database.run("ROLLBACK");
  database.close(false);
  const reboundStorage = makeTestLayer(filename).storage;
  const rebound = await startHost({
    layer: Protocol.layer().pipe(Layer.provide(reboundStorage)),
    port: host.port,
  });
  try {
    expect(rebound.port).toBe(host.port);
    expect((await fetch(new URL("rebound", rebound.url), { method: "PUT" })).status).toBe(201);
  } finally {
    await stopHost(rebound);
  }
  await Bun.write(
    `${scratch}/contention-held-shutdown-metrics-${process.pid}.json`,
    JSON.stringify({ stopElapsedMs, attempts: probe.transactionAttempts }, undefined, 2),
  );
});
