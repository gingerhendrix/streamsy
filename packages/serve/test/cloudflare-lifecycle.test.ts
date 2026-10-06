/* oxlint-disable effecttsgo/async-function -- These tests own the real local workerd boundary. */
import { afterEach, expect, test } from "bun:test";
import { Effect } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { testHost } from "./support/scoped-host.ts";
import {
  cleanupHarnesses,
  closeAfterTest,
  direct,
  dispatch,
  disposeHarness,
  makeHarness,
  probe,
  waitUntil,
  type Harness,
  type ProbeResult,
} from "./support/cloudflare/harness.ts";

afterEach(cleanupHarnesses);

// SAFETY: JSON probe values are untrusted here; Number.isFinite is the parser for this narrow test domain.
// oxlint-disable-next-line anti-slop/no-unknown-parameters
const isFiniteNumber = (value: unknown): value is number => Number.isFinite(value);

const put = (
  harness: Harness,
  path: string,
  body = "source",
  contentType = "text/plain",
  headers: Record<string, string> = {},
) =>
  dispatch(harness, path, {
    method: "PUT",
    body,
    headers: { "content-type": contentType, ...headers },
  });

const append = (harness: Harness, path: string, body: string, contentType = "text/plain") =>
  dispatch(harness, path, {
    method: "POST",
    body,
    headers: { "content-type": contentType },
  });

const head = async (harness: Harness, path: string) => dispatch(harness, path, { method: "HEAD" });

const rowFor = (observation: ProbeResult, streamId: string) =>
  observation.rows.find((row) => row[0] === streamId);

const messageRowsFor = (observation: ProbeResult, streamId: string) =>
  observation.messages.filter((row) => row[0] === streamId);

const zeroOffset = "0000000000000000_0000000000000000";

const twoOffset = "0000000000000000_0000000000000002";

const largeJson = (prefix: string) =>
  JSON.stringify(Array.from({ length: 3_000 }, (_, index) => `${prefix}-${index}`));

test("B2 Cloudflare fork classifications are byte-parity with Bun", async () => {
  const harness = await makeHarness("worker-by-key.ts");

  const bun = await Effect.runPromise(
    testHost({ pathPrefix: "/streams", layer: MemoryStorage.layerProtocol(), port: 0 }),
  );

  closeAfterTest(() => Effect.runPromise(bun.stop));
  const bunRequest = (path: string, init?: RequestInit) => fetch(new URL(path, bun.url), init);

  const observe = async (response: Response) => {
    const contentType = response.headers.get("content-type")?.toLowerCase() ?? null;
    const nextOffset = response.headers.get("stream-next-offset");
    const notSupported = response.headers.get("stream-not-supported");
    const rawLocation = response.headers.get("location");
    const location = rawLocation === null ? null : new URL(rawLocation).pathname;

    return {
      status: response.status,
      body: await response.text(),
      contentType,
      nextOffset,
      notSupported,
      location,
    };
  };

  const compare = async (path: string, init?: RequestInit) => {
    const cloudflare = await dispatch(harness, path, init);
    const memory = await bunRequest(path, init);
    const observedCloudflare = await observe(cloudflare);
    const observedMemory = await observe(memory);
    expect(observedCloudflare).toEqual(observedMemory);
  };

  const pairedSource = async (
    source: string,
    body: string,
    contentType: string,
    forkPath: string,
    headers: Record<string, string>,
  ) => {
    expect((await put(harness, `/streams/p/${source}`, body, contentType)).status).toBe(201);
    expect(
      (
        await bunRequest(`/streams/p/${source}`, {
          method: "PUT",
          body,
          headers: { "content-type": contentType },
        })
      ).status,
    ).toBe(201);
    await compare(`/streams/p/${forkPath}`, {
      method: "PUT",
      headers,
    });
  };

  expect((await put(harness, "/streams/p/offset", "a")).status).toBe(201);
  expect((await append(harness, "/streams/p/offset", "b")).status).toBe(204);
  expect(
    (
      await bunRequest("/streams/p/offset", {
        method: "PUT",
        body: "a",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(201);
  expect(
    (
      await bunRequest("/streams/p/offset", {
        method: "POST",
        body: "b",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(204);
  const offsetTail = (await head(harness, "/streams/p/offset")).headers.get("stream-next-offset");

  if (offsetTail === null) throw new Error("B2 offset tail missing");
  await compare("/streams/p/explicit-offset", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/p/offset", "stream-fork-offset": offsetTail },
  });

  await pairedSource("json", "[1,2]", "application/json", "json-child", {
    "stream-forked-from": "/streams/p/json",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "1",
  });
  await pairedSource("text", "hello", "text/plain", "text-child", {
    "stream-forked-from": "/streams/p/text",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "3",
  });
  const largeSubOffset = "x".repeat(10_001);
  await pairedSource("large-text", largeSubOffset, "text/plain", "large-text-child", {
    "stream-forked-from": "/streams/p/large-text",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "10001",
  });
  await pairedSource(
    "large-binary",
    largeSubOffset,
    "application/octet-stream",
    "large-binary-child",
    {
      "stream-forked-from": "/streams/p/large-binary",
      "stream-fork-offset": zeroOffset,
      "stream-fork-sub-offset": "10001",
    },
  );
  await pairedSource(
    "large-json",
    JSON.stringify(Array.from({ length: 10_001 }, (_, index) => index)),
    "application/json",
    "large-json-child",
    {
      "stream-forked-from": "/streams/p/large-json",
      "stream-fork-offset": zeroOffset,
      "stream-fork-sub-offset": "10001",
    },
  );
  await pairedSource("invalid", "body", "text/plain", "invalid-child", {
    "stream-forked-from": "/streams/p/invalid",
    "stream-fork-offset": "bad",
  });
  await pairedSource("beyond", "body", "text/plain", "beyond-child", {
    "stream-forked-from": "/streams/p/beyond",
    "stream-fork-offset": twoOffset,
  });
  await pairedSource("mismatch", "body", "text/plain", "mismatch-child", {
    "stream-forked-from": "/streams/p/mismatch",
    "content-type": "application/json",
  });
  await pairedSource("overshoot", "hi", "text/plain", "overshoot-child", {
    "stream-forked-from": "/streams/p/overshoot",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "3",
  });
  await pairedSource("sub-without-offset", "hello", "text/plain", "sub-without-offset-child", {
    "stream-forked-from": "/streams/p/sub-without-offset",
    "stream-fork-sub-offset": "1",
  });
  await compare("/streams/p/absolute-child", {
    method: "PUT",
    headers: { "stream-forked-from": "https://streams.test/streams/p/offset" },
  });

  expect((await put(harness, "/streams/p/retry-source", "body")).status).toBe(201);
  expect(
    (
      await bunRequest("/streams/p/retry-source", {
        method: "PUT",
        body: "body",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(201);
  await compare("/streams/p/retry-child", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/p/retry-source" },
  });
  await compare("/streams/p/retry-child", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/p/retry-source" },
  });
});

test("B3 byKey same-object forks chain and retain a soft-deleted source", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect((await put(harness, "/streams/t1/x", "x")).status).toBe(201);

  const fork = await put(harness, "/streams/t1/y", "", "text/plain", {
    "stream-forked-from": "/streams/t1/x",
  });

  expect(fork.status).toBe(201);
  expect(await (await dispatch(harness, "/streams/t1/y")).text()).toBe("x");
  const observation = await probe(harness, "t1");
  expect(messageRowsFor(observation, "t1/y")).toHaveLength(0);
  expect(rowFor(observation, "t1/y")?.[2]).toBe("t1/x");
  expect((await dispatch(harness, "/streams/t1/x", { method: "DELETE" })).status).toBe(204);
  expect((await head(harness, "/streams/t1/x")).status).toBe(410);
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(1);
});

test("B7 missing, soft-deleted, and expired source classifications are exact", async () => {
  const missing = await makeHarness();

  const missingResponse = await put(missing, "/streams/child", "", "text/plain", {
    "stream-forked-from": "/streams/nowhere",
  });

  expect(missingResponse.status).toBe(404);
  expect(await missingResponse.text()).toBe("Source stream not found: nowhere");
  await disposeHarness(missing);

  const deleted = await makeHarness("worker-by-key.ts");
  expect((await put(deleted, "/streams/t1/x", "x")).status).toBe(201);
  expect(
    (
      await put(deleted, "/streams/t1/y", "", "text/plain", {
        "stream-forked-from": "/streams/t1/x",
      })
    ).status,
  ).toBe(201);
  expect((await dispatch(deleted, "/streams/t1/x", { method: "DELETE" })).status).toBe(204);

  const deletedResponse = await put(deleted, "/streams/t1/z", "", "text/plain", {
    "stream-forked-from": "/streams/t1/x",
  });

  expect(deletedResponse.status).toBe(409);
  expect(await deletedResponse.text()).toBe("Source stream is soft-deleted: t1/x");

  const expired = await makeHarness("worker-by-key.ts");
  expect(
    (await put(expired, "/streams/e/expired", "x", "text/plain", { "stream-ttl": "1" })).status,
  ).toBe(201);
  await Bun.sleep(2_000);

  const expiredResponse = await put(expired, "/streams/e/expired-child", "", "text/plain", {
    "stream-forked-from": "/streams/e/expired",
  });

  expect(expiredResponse.status).toBe(404);
  expect(await expiredResponse.text()).toBe("Source stream not found: e/expired");
});

test("B9 byKey rejects cross-stream forks without changing the source", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect((await put(harness, "/streams/t1/x", "x")).status).toBe(201);
  const before = await probe(harness, "t1");

  const fork = await put(harness, "/streams/t2/z", "", "text/plain", {
    "stream-forked-from": "/streams/t1/x",
  });

  expect(fork.status).toBe(404);
  expect(await fork.text()).toBe("Source stream not found: t1/x");
  expect((await probe(harness, "t2")).rows).toHaveLength(0);
  const after = await probe(harness, "t1");
  expect(after.rows).toEqual(before.rows);
  expect(after.messages).toEqual(before.messages);
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(2);
});

test("byStream returns 404 for a fork even when its source exists", async () => {
  const harness = await makeHarness();
  expect((await put(harness, "/streams/source", "source")).status).toBe(201);

  const fork = await put(harness, "/streams/child", "", "text/plain", {
    "stream-forked-from": "/streams/source",
  });

  expect(fork.status).toBe(404);
  expect(await fork.text()).toBe("Source stream not found: source");
  expect((await head(harness, "/streams/source")).status).toBe(200);
  expect((await probe(harness, "child")).rows).toHaveLength(0);
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(2);
});

test("B11 chain-fork TTL and absolute expiry inherit into the child alarm", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect(
    (await put(harness, "/streams/t1/ttl-source", "x", "text/plain", { "stream-ttl": "2" })).status,
  ).toBe(201);
  const now = Date.now();
  expect(
    (
      await put(harness, "/streams/t1/ttl-child", "", "text/plain", {
        "stream-forked-from": "/streams/t1/ttl-source",
      })
    ).status,
  ).toBe(201);
  const child = await probe(harness, "t1");
  const expiry = rowFor(child, "t1/ttl-child")?.[1];

  if (!isFiniteNumber(expiry)) throw new Error("B11 inherited TTL was not persisted");
  expect(expiry - now).toBeGreaterThan(500);
  expect(expiry - now).toBeLessThan(2_500);
  await waitUntil(async () => (await probe(harness, "t1")).rows.length === 0, 4_000);
  const expired = await probe(harness, "t1");
  expect(expired.alarm).toBeNull();
  expect(expired.alarmInvocations).toBeGreaterThanOrEqual(1);

  expect(
    (await put(harness, "/streams/t1/ttl-override", "x", "text/plain", { "stream-ttl": "2" }))
      .status,
  ).toBe(201);
  expect(
    (
      await put(harness, "/streams/t1/override-child", "", "text/plain", {
        "stream-forked-from": "/streams/t1/ttl-override",
        "stream-ttl": "60",
      })
    ).status,
  ).toBe(201);
  const override = await probe(harness, "t1");
  const overrideExpiry = rowFor(override, "t1/override-child")?.[1];

  if (!isFiniteNumber(overrideExpiry)) throw new Error("B11 override expiry was not persisted");
  expect(overrideExpiry - Date.now()).toBeGreaterThan(58_000);

  const absolute = new Date(Date.now() + 2_000).toISOString();
  expect(
    (
      await put(harness, "/streams/t1/date-source", "x", "text/plain", {
        "stream-expires-at": absolute,
      })
    ).status,
  ).toBe(201);
  expect(
    (
      await put(harness, "/streams/t1/date-child", "", "text/plain", {
        "stream-forked-from": "/streams/t1/date-source",
      })
    ).status,
  ).toBe(201);
  const dateChild = await probe(harness, "t1");
  const dateExpiry = rowFor(dateChild, "t1/date-child")?.[1];

  if (!isFiniteNumber(dateExpiry))
    throw new Error("B11 inherited absolute expiry was not persisted");
  expect(dateExpiry).toBe(new Date(absolute).getTime());
});

test("B12 TTL zero uses the one-millisecond alarm floor", async () => {
  const harness = await makeHarness();
  const started = Date.now();
  expect(
    (await put(harness, "/streams/floor", "x", "text/plain", { "stream-ttl": "0" })).status,
  ).toBe(201);
  const observation = await probe(harness, "floor");
  const scheduled = observation.alarm ?? observation.alarmAfterMutation;

  if (scheduled === null) throw new Error("B12 floor alarm was not armed");
  expect(scheduled).toBeGreaterThanOrEqual(started + 1);
  await waitUntil(async () => (await probe(harness, "floor")).rows.length === 0, 1_500);
  const expired = await probe(harness, "floor");
  expect(expired.alarmInvocations).toBe(1);
  expect(expired.alarm).toBeNull();
  expect((await head(harness, "/streams/floor")).status).toBe(404);
});

test("B13 a stale deadline re-arms after a sliding read without rebuilding the scope", async () => {
  const harness = await makeHarness();
  expect(
    (await put(harness, "/streams/stale", "x", "text/plain", { "stream-ttl": "2" })).status,
  ).toBe(201);
  const before = await probe(harness, "stale");

  if (before.alarm === null) throw new Error("B13 initial alarm was not armed");
  await Bun.sleep(1_000);
  expect((await dispatch(harness, "/streams/stale")).status).toBe(200);
  const renewed = await probe(harness, "stale");
  const renewedExpiry = renewed.rows[0]?.[1];
  const initialExpiry = before.rows[0]?.[1];

  if (!isFiniteNumber(renewedExpiry) || !isFiniteNumber(initialExpiry))
    throw new Error("B13 expiry rows missing");
  expect(renewedExpiry).toBeGreaterThan(initialExpiry);
  await waitUntil(async () => (await probe(harness, "stale")).alarmInvocations >= 1, 3_000);
  const early = await probe(harness, "stale");
  expect(early.rows).toHaveLength(1);
  expect(early.alarm).toBeGreaterThan(before.alarm);
  expect(early.layerAcquisitions).toBe(1);
  await waitUntil(async () => (await probe(harness, "stale")).rows.length === 0, 3_000);
});

const recreateB15Harnesses = async (
  byKey: Harness,
  byStream: Harness,
): Promise<{ readonly byKey: Harness; readonly byStream: Harness }> => {
  const retentionErrors: Array<unknown> = [];

  for (const retained of [byKey, byStream]) {
    try {
      await disposeHarness(retained, true);
    } catch (error) {
      retentionErrors.push(error);
    }
  }

  if (retentionErrors.length > 0)
    throw new AggregateError(retentionErrors, "B15 retention cleanup failed");

  return {
    byKey: await makeHarness("worker-by-key.ts", byKey.root),
    byStream: await makeHarness("worker.ts", byStream.root),
  };
};

test("B15 recreation under load restores and purges alarms in both object layouts", async () => {
  const byKey = await makeHarness("worker-by-key.ts");
  const byStream = await makeHarness("worker.ts");

  for (let ttl = 1; ttl <= 3; ttl += 1)
    expect(
      (await put(byKey, `/streams/t1/ttl-${ttl}`, "x", "text/plain", { "stream-ttl": String(ttl) }))
        .status,
    ).toBe(201);
  expect(
    (await put(byStream, "/streams/source", "x", "text/plain", { "stream-ttl": "2" })).status,
  ).toBe(201);
  expect(
    (
      await put(byStream, "/streams/child", "", "text/plain", {
        "stream-ttl": "2",
      })
    ).status,
  ).toBe(201);

  const { byKey: recreatedByKey, byStream: recreatedByStream } = await recreateB15Harnesses(
    byKey,
    byStream,
  );

  await waitUntil(async () => (await probe(recreatedByKey, "t1")).rows.length === 0, 5_000);
  await waitUntil(async () => (await probe(recreatedByStream, "child")).rows.length === 0, 5_000);
  expect((await probe(recreatedByKey, "t1")).alarm).toBeNull();
  expect((await probe(recreatedByStream, "child")).alarm).toBeNull();
  expect((await probe(recreatedByKey, "t1")).layerAcquisitions).toBe(1);
  expect((await probe(recreatedByStream, "child")).layerAcquisitions).toBe(1);
});

test("B16 chain forks reconcile an inherited alarm before the PUT returns", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect((await direct(harness, "t1", "/streams/t1/alarm-child", { method: "HEAD" })).status).toBe(
    404,
  );
  expect((await probe(harness, "t1")).alarm).toBeNull();
  expect(
    (await put(harness, "/streams/t1/alarm-source", "x", "text/plain", { "stream-ttl": "2" }))
      .status,
  ).toBe(201);
  expect(
    (
      await put(harness, "/streams/t1/alarm-child", "", "text/plain", {
        "stream-forked-from": "/streams/t1/alarm-source",
      })
    ).status,
  ).toBe(201);
  const after = await probe(harness, "t1");
  expect(after.alarm).not.toBeNull();
  expect(after.alarmAfterMutation).not.toBeNull();
  expect(rowFor(after, "t1/alarm-child")?.[1]).toBeGreaterThan(Date.now());
});

test(
  "Batch B F2 keeps concurrent large same-object creates bounded",
  async () => {
    const harness = await makeHarness("worker-by-key.ts");
    const first = largeJson("first");
    const second = largeJson("second");

    const concurrent = await Promise.all([
      put(harness, "/streams/t1/x", first, "application/json"),
      put(harness, "/streams/t1/y", second, "application/json"),
    ]);

    expect(concurrent.map((response) => response.status)).toEqual([201, 201]);
    const stream = await probe(harness, "t1");
    expect(messageRowsFor(stream, "t1/x")).toHaveLength(3_000);
    expect(messageRowsFor(stream, "t1/y")).toHaveLength(3_000);
    expect(stream.layerAcquisitions).toBe(1);
  },
  { timeout: 35_000 },
);
