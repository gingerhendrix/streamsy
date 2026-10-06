/* oxlint-disable effecttsgo/async-function -- This is the real local workerd boundary test. */
import { afterEach, expect, test } from "bun:test";
import {
  cleanupHarnesses,
  direct,
  dispatch,
  dispatchFetch,
  makeHarness,
  probe,
  waitUntil,
  type Harness,
} from "./support/cloudflare/harness.ts";

afterEach(cleanupHarnesses);

const create = (harness: Harness, path: string, ttl?: number) => {
  const headers = { "content-type": "text/plain" };

  if (ttl !== undefined) Object.assign(headers, { "stream-ttl": String(ttl) });

  return dispatch(harness, path, { method: "PUT", body: "source", headers });
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

test("the alarm effect is unreachable through routes, stubs, and forged headers", async () => {
  const harness = await makeHarness();

  const routed = await dispatch(harness, "/streams/alarm", {
    method: "POST",
    headers: { "streamsy-host-command": "ExpireDue" },
  });

  // Empty POST validation precedes the missing-stream lookup.
  expect(routed.status).toBe(400);

  const directResponse = await direct(harness, "alarm", "/alarm", {
    method: "POST",
    headers: { "streamsy-expire-due": "true" },
  });

  expect([400, 404]).toContain(directResponse.status);
  expect((await probe(harness, "alarm")).alarmInvocations).toBe(0);
});

test("one object scope serves mixed requests and one real alarm turn", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/reuse", 3)).status).toBe(201);

  const requests: ReadonlyArray<RequestInit | undefined> = [
    { method: "HEAD" },
    undefined,
    { method: "OPTIONS" },
  ];

  for (let index = 0; index < 20; index += 1) {
    const response = await dispatch(harness, "/streams/reuse", requests[index % requests.length]);

    expect([200, 204, 404]).toContain(response.status);
  }

  await waitUntil(async () => (await probe(harness, "reuse")).alarmInvocations > 0);
  const observation = await probe(harness, "reuse");
  expect(observation.layerAcquisitions).toBe(1);
  expect(observation.migrationAttempts).toBe(1);
  expect(observation.alarmInvocations).toBeGreaterThanOrEqual(1);
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

test("a reconcile failure preserves the committed response and standard headers", async () => {
  const harness = await makeHarness();
  await direct(harness, "reconcile-failure", "/__probe?fail-next-expiry=1");
  const response = await create(harness, "/streams/reconcile-failure", 3);
  expect(response.status).toBe(201);
  expect(response.headers.get("x-content-type-options")).toBe("nosniff");
  expect(response.headers.get("cross-origin-resource-policy")).toBe("cross-origin");
  expect((await probe(harness, "reconcile-failure")).alarm).toBeNull();
  expect((await probe(harness, "reconcile-failure")).rows).toHaveLength(1);

  expect(
    (
      await dispatch(harness, "/streams/reconcile-failure", {
        method: "POST",
        body: "repair",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(204);
  expect((await probe(harness, "reconcile-failure")).alarm).not.toBeNull();
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

test("a failed alarm turn retries without rebuilding the object scope", async () => {
  const harness = await makeHarness();
  await direct(harness, "failing-alarm", "/__probe?long-poll-timeout=25000");
  expect((await create(harness, "/streams/failing-alarm", 1)).status).toBe(201);

  const tail = (await dispatch(harness, "/streams/failing-alarm", { method: "HEAD" })).headers.get(
    "stream-next-offset",
  );

  if (tail === null) throw new Error("missing stream tail");

  const longPoll = dispatch(harness, `/streams/failing-alarm?offset=${tail}&live=long-poll`).catch(
    () => undefined,
  );

  await waitUntil(async () => (await probe(harness, "failing-alarm")).activeReads === 1);
  await direct(harness, "failing-alarm", "/__probe?fail-next-expiry=1");
  await waitUntil(async () => (await probe(harness, "failing-alarm")).alarmInfo.length > 0, 5_000);
  const failed = await probe(harness, "failing-alarm");
  expect(failed.alarmInfo[0]).toBeDefined();
  // A failed alarm turn leaves the expired row in place for the retry.
  expect(failed.rows).toHaveLength(1);
  expect(failed.layerAcquisitions).toBe(1);
  expect(failed.activeReads).toBe(1);
  await waitUntil(async () => (await probe(harness, "failing-alarm")).alarmInfo.length >= 2, 8_000);
  const retried = await probe(harness, "failing-alarm");
  expect(retried.alarmInfo.some((info) => info.isRetry && info.retryCount > 0)).toBe(true);
  expect(retried.rows).toHaveLength(0);
  expect(retried.alarm).toBeNull();
  expect(retried.layerAcquisitions).toBe(1);
  expect(retried.migrationAttempts).toBe(1);
  await longPoll;
  expect((await dispatch(harness, "/streams/failing-alarm", { method: "HEAD" })).status).toBe(404);
});

test("an abandoned workerd long-poll releases at the configured protocol bound", async () => {
  const harness = await makeHarness();
  expect((await create(harness, "/streams/cancel")).status).toBe(201);

  const tail = (await dispatch(harness, "/streams/cancel", { method: "HEAD" })).headers.get(
    "stream-next-offset",
  );

  if (tail === null) throw new Error("missing stream tail");
  const longPollAbort = new AbortController();

  const longPoll = dispatchFetch(
    harness,
    new Request(`https://streams.test/streams/cancel?offset=${tail}&live=long-poll`, {
      signal: longPollAbort.signal,
    }),
  ).catch(() => undefined);

  await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 1);
  longPollAbort.abort();
  await longPoll;
  await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 0, 3_000);
});

test.each(["fail-layer-once", "throw-layer-once"])(
  "a %s layer build returns a secured 503, is not cached, and retries",
  async (failure) => {
    const harness = await makeHarness();
    await direct(harness, "retry-layer", `/__probe?${failure}=1`);
    const first = await create(harness, "/streams/retry-layer");
    expect(first.status).toBe(503);
    expect(first.headers.get("retry-after")).toBe("1");
    expect(first.headers.get("x-content-type-options")).toBe("nosniff");
    expect(first.headers.get("cross-origin-resource-policy")).toBe("cross-origin");
    expect(await first.text()).toBe("Storage unavailable");
    expect((await create(harness, "/streams/retry-layer")).status).toBe(201);
    expect((await probe(harness, "retry-layer")).layerAcquisitions).toBe(2);
  },
);

test("a Durable Object app serves value handlers and respondable parameter errors", async () => {
  const harness = await makeHarness();
  const response = await direct(harness, "serve", "/serve-value/7");
  expect(response.status).toBe(200);
  expect(await response.text()).toBe('{"seat":7}');
  expect(response.headers.get("x-streamsy-value-contract")).toMatch(/^[0-9a-f]{8}$/);
  const invalid = await direct(harness, "serve", "/serve-value/no");
  expect(invalid.status).toBe(400);
  expect(await invalid.json()).toEqual({
    _tag: "InvalidParams",
    route: "/serve-value/:seat",
    parameter: "params",
    detail: "Invalid path parameters",
  });
  const denied = await direct(harness, "serve", "/serve-value/7", { method: "POST" });
  expect(denied.status).toBe(405);
  expect(denied.headers.get("allow")).toBe("GET, HEAD");
});
