/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import -- This is the real local workerd boundary test. */
import { afterEach, expect, test } from "bun:test";
import { cpSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { basename, join } from "node:path";
import { Miniflare } from "miniflare";

const scratch =
  Bun.env.STREAMSY_STORAGE_SCRATCH ??
  "/home/gareth/Documents/Personal/scratch/2026-09-07-step-3-batch-a";
mkdirSync(scratch, { recursive: true });

interface Harness {
  readonly miniflare: Miniflare;
  readonly root: string;
  readonly namespace: Awaited<ReturnType<Miniflare["getDurableObjectNamespace"]>>;
}

interface ProbeResult {
  readonly layerAcquisitions: number;
  readonly migrationAttempts: number;
  readonly hostCommandRuns: number;
  readonly activeReads: number;
  readonly alarmInfo: ReadonlyArray<{ readonly isRetry: boolean; readonly retryCount: number }>;
  readonly alarm: number | null;
  readonly rows: ReadonlyArray<ReadonlyArray<unknown>>;
}

const open: Array<Harness> = [];

const makeHarness = async (
  entry = "worker.ts",
  root = mkdtempSync(".streamsy-cloudflare-workerd-"),
) => {
  const bundle = join(root, `bundle-${crypto.randomUUID()}`);
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, "cloudflare", entry)],
    outdir: bundle,
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });
  expect(built.success).toBe(true);
  const output = built.outputs[0];
  if (output === undefined) throw new Error("Cloudflare host proof produced no bundle");
  const miniflare = new Miniflare({
    scriptPath: output.path,
    modules: true,
    compatibilityDate: "2026-08-06",
    durableObjects: { STREAMS: { className: "ProbeObject", useSQLite: true } },
    durableObjectsPersist: join(root, "state"),
  });
  try {
    await miniflare.ready;
  } catch (error) {
    await miniflare.dispose();
    throw error;
  }
  const namespace = await miniflare.getDurableObjectNamespace("STREAMS");
  const harness = { miniflare, root, namespace };
  open.push(harness);
  return harness;
};

afterEach(async () => {
  for (const harness of open.splice(0)) {
    await harness.miniflare.dispose();
    cpSync(harness.root, join(scratch, basename(harness.root)), { recursive: true });
    rmSync(harness.root, { recursive: true });
  }
});

const dispatch = (harness: Harness, path: string, init?: RequestInit) =>
  harness.miniflare.dispatchFetch(`https://streams.test${path}`, init);

const direct = (harness: Harness, name: string, path: string, init?: RequestInit) =>
  harness.namespace
    .get(harness.namespace.idFromName(name))
    .fetch(new Request(`https://object.test${path}`, init));

const probe = async (harness: Harness, name: string) =>
  // SAFETY: The fixture's `/__probe` branch always returns this fixed JSON shape.
  (await direct(harness, name, "/__probe")).json() as ProbeResult;

const create = (harness: Harness, path: string, ttl?: number) => {
  const headers = { "content-type": "text/plain" };
  if (ttl !== undefined) Object.assign(headers, { "stream-ttl": String(ttl) });
  return dispatch(harness, path, { method: "PUT", body: "source", headers });
};

const waitUntil = async (predicate: () => Promise<boolean>, timeoutMs = 6_000) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(50);
  }
  throw new Error(`Timed out after ${timeoutMs} ms`);
};

test("byStream isolates objects and reuses the same object identity", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/a")).status).toBe(201);
  expect((await create(harness, "/streams/b")).status).toBe(201);
  expect((await direct(harness, "b", "/streams/a")).status).toBe(404);
  expect((await direct(harness, "b", "/streams/b")).status).toBe(200);
  expect((await dispatch(harness, "/streams/a", { method: "HEAD" })).status).toBe(200);
  expect((await probe(harness, "a")).layerAcquisitions).toBe(1);
});

test("byKey co-locates same-family forks and refuses cross-family forks before object dispatch", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect((await create(harness, "/streams/t1/x")).status).toBe(201);
  const same = await dispatch(harness, "/streams/t1/y", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/t1/x" },
  });
  expect(same.status).toBe(201);
  expect(await (await dispatch(harness, "/streams/t1/y")).text()).toContain("source");

  const cross = await dispatch(harness, "/streams/t2/z", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/t1/x" },
  });
  expect(cross.status).toBe(400);
  expect(cross.headers.get("stream-not-supported")).toBe("fork");
  expect(await cross.text()).toBe("Feature not supported: fork");
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(1);
});

test("the configured prefix keeps raw encoded and slash paths distinct", async () => {
  const harness = await makeHarness();
  for (const path of ["/streams", "/streams/", "/other/x"]) {
    const response = await dispatch(harness, path);
    expect(response.status).toBe(400);
    expect(await response.text()).toBe("Stream path required: /streams/{path}");
  }
  expect((await dispatch(harness, "/streams/a/b?offset=-1")).status).toBe(404);
  expect((await create(harness, "/streams/a%2Fb")).status).toBe(201);
  expect((await create(harness, "/streams/a/b")).status).toBe(201);
  expect((await dispatch(harness, "/streams/a%2Fb", { method: "HEAD" })).status).toBe(200);
  expect((await dispatch(harness, "/streams/a/b", { method: "HEAD" })).status).toBe(200);
});

test("the private Context command is unreachable through routes, stubs, and forged headers", async () => {
  const harness = await makeHarness();
  const routed = await dispatch(harness, "/streams/alarm", {
    method: "POST",
    headers: { "streamsy-host-command": "ExpireDue" },
  });
  expect(routed.status).toBe(404);
  const directResponse = await direct(harness, "alarm", "/alarm", {
    method: "POST",
    headers: { "streamsy-expire-due": "true" },
  });
  expect([400, 404]).toContain(directResponse.status);
  expect((await probe(harness, "alarm")).hostCommandRuns).toBe(0);
});

test("one object scope serves mixed requests and one real alarm turn", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/reuse", 3)).status).toBe(201);
  for (let index = 0; index < 20; index += 1) {
    const response = await dispatch(
      harness,
      "/streams/reuse",
      index % 3 === 0 ? { method: "HEAD" } : index % 3 === 1 ? undefined : { method: "OPTIONS" },
    );
    expect([200, 204, 404]).toContain(response.status);
  }
  await waitUntil(async () => (await probe(harness, "reuse")).hostCommandRuns > 0);
  const observation = await probe(harness, "reuse");
  expect(observation.layerAcquisitions).toBe(1);
  expect(observation.migrationAttempts).toBe(1);
  expect(observation.hostCommandRuns).toBeGreaterThanOrEqual(1);
});

test("mutating requests reconcile the minimum alarm and clearing the last TTL clears it", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect((await create(harness, "/streams/t1/no-ttl")).status).toBe(201);
  expect((await probe(harness, "t1")).alarm).toBeNull();
  expect((await create(harness, "/streams/t1/slow", 3)).status).toBe(201);
  const slow = await probe(harness, "t1");
  expect(slow.alarm).not.toBeNull();
  expect((await create(harness, "/streams/t1/fast", 1)).status).toBe(201);
  const fast = await probe(harness, "t1");
  expect(fast.alarm).not.toBeNull();
  if (fast.alarm === null || slow.alarm === null) throw new Error("TTL alarm was not armed");
  expect(fast.alarm).toBeLessThan(slow.alarm);
  expect((await dispatch(harness, "/streams/t1/fast", { method: "DELETE" })).status).toBe(204);
  expect((await dispatch(harness, "/streams/t1/slow", { method: "DELETE" })).status).toBe(204);
  expect((await probe(harness, "t1")).alarm).toBeNull();
});

test("a real alarm purges an expired row without a read", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/expiry", 1)).status).toBe(201);
  await waitUntil(async () => (await probe(harness, "expiry")).rows.length === 0, 5_000);
  const observation = await probe(harness, "expiry");
  expect(observation.alarm).toBeNull();
  expect((await dispatch(harness, "/streams/expiry", { method: "HEAD" })).status).toBe(404);
});

test("an append renews the deadline and the stream survives the original alarm", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/renewed", 2)).status).toBe(201);
  const before = await probe(harness, "renewed");
  if (before.alarm === null) throw new Error("Initial TTL alarm was not armed");
  const initialAlarm = before.alarm;
  await Bun.sleep(1_000);
  expect(
    (
      await dispatch(harness, "/streams/renewed", {
        method: "POST",
        body: "renewed",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(204);
  const after = await probe(harness, "renewed");
  const beforeExpiry = before.rows[0]?.[1];
  const afterExpiry = after.rows[0]?.[1];
  if (beforeExpiry !== Number(beforeExpiry) || afterExpiry !== Number(afterExpiry))
    throw new Error("TTL expiry was not recorded in the SQL probe");
  expect(afterExpiry).toBeGreaterThan(beforeExpiry);
  await waitUntil(async () => {
    const observation = await probe(harness, "renewed");
    return observation.alarm !== null && observation.alarm > initialAlarm;
  }, 3_000);
  expect((await dispatch(harness, "/streams/renewed", { method: "HEAD" })).status).toBe(200);
  await waitUntil(async () => (await probe(harness, "renewed")).rows.length === 0, 4_000);
});

test("a failed alarm turn is visible and either retries locally or falls back to lazy expiry", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/failing-alarm", 1)).status).toBe(201);
  await direct(harness, "failing-alarm", "/__probe?fail-next-expiry=1");
  await waitUntil(async () => (await probe(harness, "failing-alarm")).alarmInfo.length > 0, 5_000);
  const observed = await probe(harness, "failing-alarm");
  expect(observed.alarmInfo[0]).toBeDefined();
  if (observed.alarmInfo.some((info) => info.isRetry))
    expect(observed.alarmInfo.some((info) => info.retryCount > 0)).toBe(true);
  await waitUntil(
    async () =>
      (await dispatch(harness, "/streams/failing-alarm", { method: "HEAD" })).status === 404,
    4_000,
  );
});

test("a recreated Miniflare instance receives the persisted alarm", async () => {
  const first = await makeHarness();
  expect((await create(first, "/streams/recreated", 1)).status).toBe(201);
  const root = first.root;
  await first.miniflare.dispose();
  open.splice(open.indexOf(first), 1);
  const second = await makeHarness("worker.ts", root);
  await waitUntil(async () => (await probe(second, "recreated")).rows.length === 0, 5_000);
  const observation = await probe(second, "recreated");
  expect(observation.alarm).toBeNull();
  expect(observation.layerAcquisitions).toBe(1);
});

test("workerd request cancellation releases long-poll and SSE reads", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/cancel")).status).toBe(201);
  const tail = (await dispatch(harness, "/streams/cancel", { method: "HEAD" })).headers.get(
    "stream-next-offset",
  );
  if (tail === null) throw new Error("missing stream tail");
  const failures: Array<string> = [];

  const longPollAbort = new AbortController();
  const longPoll = harness.miniflare
    .dispatchFetch(
      new Request(`https://streams.test/streams/cancel?offset=${tail}&live=long-poll`, {
        signal: longPollAbort.signal,
      }),
    )
    .catch(() => undefined);
  await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 1);
  longPollAbort.abort();
  await longPoll;
  try {
    await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 0, 2_000);
  } catch {
    failures.push("long-poll active read remained after abort");
  }

  const sseAbort = new AbortController();
  const sseResponse = await harness.miniflare.dispatchFetch(
    new Request(`https://streams.test/streams/cancel?offset=${tail}&live=sse`, {
      signal: sseAbort.signal,
    }),
  );
  const reader = sseResponse.body?.getReader();
  if (reader === undefined) throw new Error("SSE response has no body");
  await reader.read();
  const expectedActiveReads = failures.length === 0 ? 1 : 2;
  await waitUntil(async () => (await probe(harness, "cancel")).activeReads >= expectedActiveReads);
  sseAbort.abort();
  try {
    await reader.cancel();
  } catch {
    // The local workerd boundary may reject the client-side body cancellation.
  }
  try {
    await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 0, 2_000);
  } catch {
    failures.push("SSE active read remained after abort");
  }
  expect(failures).toEqual([]);
});

test("a failed Layer build is not cached", async () => {
  const harness = await makeHarness();
  await direct(harness, "retry-layer", "/__probe?fail-layer-once=1");
  const first = await create(harness, "/streams/retry-layer");
  expect(first.status).toBe(503);
  expect(first.headers.get("retry-after")).toBe("1");
  expect(await first.text()).toBe("Storage unavailable");
  expect((await create(harness, "/streams/retry-layer")).status).toBe(201);
  expect((await probe(harness, "retry-layer")).layerAcquisitions).toBe(2);
});
