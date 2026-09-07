/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/crypto-random-uuid, anti-slop/no-chained-type-assertions -- These tests own the real local workerd boundary and its Bun harness. */
import { afterEach, expect, test } from "bun:test";
import {
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { Layer } from "effect";
import { Memory, Protocol } from "@streamsy/core";
import { serve as serveBun } from "../src/bun.ts";
import { Miniflare } from "miniflare";
import { decodeFrames } from "../src/cloudflare/fork-frames.ts";

const retentionRoot = Bun.env.STREAMSY_STORAGE_SCRATCH;

interface Harness {
  readonly bundleRoot: string;
  readonly miniflare: Miniflare;
  readonly root: string;
  readonly namespace: TestNamespace;
}

interface TestNamespace {
  readonly idFromName: (name: string) => TestObjectId;
  readonly get: (id: TestObjectId) => {
    readonly fetch: (request: Request) => Promise<Response> | Response;
  };
}

interface TestObjectId {
  readonly name: string;
}

interface ProbeResult {
  readonly layerAcquisitions: number;
  readonly migrationAttempts: number;
  readonly alarmInvocations: number;
  readonly activeReads: number;
  readonly alarmInfo: ReadonlyArray<{
    readonly observedAt: number;
    readonly scheduledTime: number;
    readonly isRetry: boolean;
    readonly retryCount: number;
  }>;
  readonly alarm: number | null;
  readonly alarmAfterMutation: number | null;
  readonly exportRequests: number;
  readonly rows: ReadonlyArray<ReadonlyArray<unknown>>;
  readonly messages: ReadonlyArray<ReadonlyArray<unknown>>;
}

const open: Array<Harness> = [];
const openBun: Array<{ readonly stop: () => Promise<void> }> = [];
const ownedRoots = new Set<string>();
const ownedBundles = new Set<string>();

const isDescendant = (source: string, candidate: string): boolean => {
  const path = relative(source, candidate);
  return path !== "" && path !== ".." && !path.startsWith(`..${sep}`) && !isAbsolute(path);
};

const rejectSymlinkPath = (path: string): void => {
  let current = resolve(path);
  for (;;) {
    if (existsSync(current) && lstatSync(current).isSymbolicLink())
      throw new Error(`Retention path must not contain a symlink: ${current}`);
    const parent = dirname(current);
    if (parent === current) return;
    current = parent;
  }
};

const retentionDestination = (root: string): string | undefined => {
  if (retentionRoot === undefined) return undefined;
  if (!isAbsolute(retentionRoot)) throw new Error("STREAMSY_STORAGE_SCRATCH must be absolute");
  const destination = resolve(retentionRoot, basename(root));
  rejectSymlinkPath(root);
  rejectSymlinkPath(retentionRoot);
  rejectSymlinkPath(destination);
  const sourceReal = realpathSync(root);
  const destinationReal = existsSync(destination)
    ? realpathSync(destination)
    : resolve(realpathSync(dirname(destination)), basename(destination));
  if (destinationReal === sourceReal || isDescendant(sourceReal, destinationReal)) {
    throw new Error("STREAMSY_STORAGE_SCRATCH must not point inside the owned harness root");
  }
  return destination;
};
// SAFETY: JSON probe values are untrusted here; Number.isFinite is the parser for this narrow test domain.
// oxlint-disable-next-line anti-slop/no-unknown-parameters
const isFiniteNumber = (value: unknown): value is number => Number.isFinite(value);

const makeHarness = async (entry = "worker.ts", root?: string) => {
  const ownedRoot = root ?? mkdtempSync(join(tmpdir(), ".streamsy-cloudflare-batch-b-"));
  ownedRoots.add(ownedRoot);
  let bundleRoot: string | undefined;
  let miniflare: Miniflare | undefined;
  try {
    bundleRoot = mkdtempSync(".streamsy-cloudflare-batch-b-bundle-");
    ownedBundles.add(bundleRoot);
    const bundle = join(bundleRoot, `bundle-${crypto.randomUUID()}`);
    const built = await Bun.build({
      entrypoints: [join(import.meta.dir, "cloudflare", entry)],
      outdir: bundle,
      target: "browser",
      format: "esm",
      external: ["cloudflare:workers"],
    });
    expect(built.success).toBe(true);
    const output = built.outputs[0];
    if (output === undefined) throw new Error("Batch B workerd proof produced no bundle");
    miniflare = new Miniflare({
      scriptPath: output.path,
      modules: true,
      compatibilityDate: "2026-08-06",
      host: "127.0.0.1",
      port: 0,
      cf: false,
      durableObjects: { STREAMS: { className: "ProbeObject", useSQLite: true } },
      durableObjectsPersist: join(ownedRoot, "state"),
    });
    await miniflare.ready;
    // SAFETY: the fixture only uses the namespace's idFromName/get/fetch boundary.
    const namespace = (await miniflare.getDurableObjectNamespace(
      "STREAMS",
    )) as unknown as TestNamespace;
    const harness = { bundleRoot, miniflare, root: ownedRoot, namespace };
    open.push(harness);
    return harness;
  } catch (error) {
    const cleanupErrors: Array<unknown> = [];
    if (miniflare !== undefined) {
      try {
        await miniflare.dispose();
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (bundleRoot !== undefined) {
      try {
        rmSync(bundleRoot, { recursive: true, force: true });
        ownedBundles.delete(bundleRoot);
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (root === undefined) {
      try {
        rmSync(ownedRoot, { recursive: true, force: true });
        ownedRoots.delete(ownedRoot);
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (cleanupErrors.length > 0) {
      // oxlint-disable-next-line eslint(preserve-caught-error) -- AggregateError retains the primary acquisition error and every cleanup error.
      throw new AggregateError(
        [error, ...cleanupErrors],
        "Cloudflare fork harness acquisition failed",
        {
          cause: error,
        },
      );
    }
    throw error;
  }
};

const disposeHarness = async (harness: Harness, retainRoot = false) => {
  const cleanupErrors: Array<unknown> = [];
  try {
    await harness.miniflare.dispose();
  } catch (error) {
    cleanupErrors.push(error);
  }
  const index = open.indexOf(harness);
  if (index >= 0) open.splice(index, 1);
  try {
    rmSync(harness.bundleRoot, { recursive: true, force: true });
    ownedBundles.delete(harness.bundleRoot);
  } catch (error) {
    cleanupErrors.push(error);
  }
  if (!retainRoot) {
    try {
      const destination = retentionDestination(harness.root);
      if (destination !== undefined) {
        mkdirSync(resolve(destination, ".."), { recursive: true });
        cpSync(harness.root, destination, { recursive: true });
      }
    } catch (error) {
      cleanupErrors.push(error);
    }
    try {
      rmSync(harness.root, { recursive: true, force: true });
      ownedRoots.delete(harness.root);
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (cleanupErrors.length > 0)
    throw new AggregateError(cleanupErrors, "Cloudflare fork harness cleanup failed");
};

afterEach(async () => {
  const errors: Array<unknown> = [];
  for (const server of openBun.splice(0)) {
    try {
      await server.stop();
    } catch (error) {
      openBun.push(server);
      errors.push(error);
    }
  }
  for (const server of openBun.splice(0)) {
    try {
      await server.stop();
    } catch (error) {
      openBun.push(server);
      errors.push(error);
    }
  }
  for (const harness of open.splice(0)) {
    try {
      await disposeHarness(harness);
    } catch (error) {
      errors.push(error);
    }
  }
  for (const bundleRoot of ownedBundles) {
    try {
      rmSync(bundleRoot, { recursive: true, force: true });
      ownedBundles.delete(bundleRoot);
    } catch (error) {
      errors.push(error);
    }
  }
  for (const root of ownedRoots) {
    try {
      const destination = retentionDestination(root);
      if (destination !== undefined) {
        mkdirSync(resolve(destination, ".."), { recursive: true });
        cpSync(root, destination, { recursive: true });
      }
      rmSync(root, { recursive: true, force: true });
      ownedRoots.delete(root);
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length > 0)
    throw new AggregateError(errors, "Cloudflare fork afterEach cleanup failed");
});

const dispatchFetch = (
  miniflare: Miniflare,
  input: string | Request,
  init?: RequestInit,
): Promise<Response> =>
  // SAFETY: Miniflare's declaration uses the workers-types Request overload;
  // this adapter exposes the equivalent Bun Fetch signature used by the tests.
  (
    miniflare.dispatchFetch as unknown as (
      input: string | Request,
      init?: RequestInit,
    ) => Promise<Response>
  )(input, init);

const dispatch = (harness: Harness, path: string, init?: RequestInit) =>
  dispatchFetch(harness.miniflare, `https://streams.test${path}`, init);

const direct = (harness: Harness, name: string, path: string, init?: RequestInit) =>
  harness.namespace
    .get(harness.namespace.idFromName(name))
    .fetch(new Request(`https://object.test${path}`, init));

const directInternal = (harness: Harness, name: string, path: string, init?: RequestInit) =>
  harness.namespace
    .get(harness.namespace.idFromName(name))
    .fetch(new Request(`https://streamsy.internal${path}`, init));

const probe = async (harness: Harness, name: string) =>
  // SAFETY: the fixture's /__probe branch always returns this fixed JSON shape.
  (await direct(harness, name, "/__probe")).json() as unknown as ProbeResult;

const setProbe = (harness: Harness, name: string, query: string) =>
  direct(harness, name, `/__probe?${query}`);

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

const waitUntil = async (predicate: () => Promise<boolean>, timeoutMs = 6_000) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(50);
  }
  throw new Error(`Timed out after ${timeoutMs} ms`);
};

const rowFor = (observation: ProbeResult, streamId: string) =>
  observation.rows.find((row) => row[0] === streamId);

const messageRowsFor = (observation: ProbeResult, streamId: string) =>
  observation.messages.filter((row) => row[0] === streamId);

const zeroOffset = "0000000000000000_0000000000000000";
const oneOffset = "0000000000000000_0000000000000001";
const twoOffset = "0000000000000000_0000000000000002";
const largeJson = (prefix: string) =>
  JSON.stringify(Array.from({ length: 3_000 }, (_, index) => `${prefix}-${index}`));

test("B1 byStream copy preserves bodies, offsets, timestamps, and provenance", async () => {
  const harness = await makeHarness();
  expect((await put(harness, "/streams/src", "one")).status).toBe(201);
  expect((await append(harness, "/streams/src", "two")).status).toBe(204);
  expect((await append(harness, "/streams/src", "three")).status).toBe(204);
  const sourceHead = await head(harness, "/streams/src");
  const sourceTail = sourceHead.headers.get("stream-next-offset");
  if (sourceTail === null) throw new Error("B1 source tail missing");

  const fork = await put(harness, "/streams/child", "", "text/plain", {
    "stream-forked-from": "/streams/src",
  });
  expect(fork.status).toBe(201);
  expect(fork.headers.get("stream-next-offset")).toBe(sourceTail);
  expect(await dispatch(harness, "/streams/child").then((response) => response.text())).toBe(
    "onetwothree",
  );
  const sourceRead = await dispatch(harness, `/streams/src?offset=${oneOffset}`);
  const childRead = await dispatch(harness, `/streams/child?offset=${oneOffset}`);
  expect(await childRead.text()).toBe(await sourceRead.text());

  const source = await probe(harness, "src");
  const child = await probe(harness, "child");
  expect(messageRowsFor(child, "child")).toEqual(
    messageRowsFor(source, "src").map((row) => ["child", row[1], row[2], row[3]]),
  );
  expect(rowFor(child, "child")?.[2]).toBe("src");
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(2);
});

test("B2 Cloudflare fork classifications are byte-parity with Bun", async () => {
  const harness = await makeHarness();
  const bun = await serveBun({
    pathPrefix: "/streams",
    layer: Protocol.layer().pipe(Layer.provide(Memory.layer())),
    port: 0,
  });
  openBun.push(bun);
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
    expect((await put(harness, `/streams/${source}`, body, contentType)).status).toBe(201);
    expect(
      (
        await bunRequest(`/streams/${source}`, {
          method: "PUT",
          body,
          headers: { "content-type": contentType },
        })
      ).status,
    ).toBe(201);
    await compare(`/streams/${forkPath}`, {
      method: "PUT",
      headers,
    });
  };

  expect((await put(harness, "/streams/offset", "a")).status).toBe(201);
  expect((await append(harness, "/streams/offset", "b")).status).toBe(204);
  expect(
    (
      await bunRequest("/streams/offset", {
        method: "PUT",
        body: "a",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(201);
  expect(
    (
      await bunRequest("/streams/offset", {
        method: "POST",
        body: "b",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(204);
  const offsetTail = (await head(harness, "/streams/offset")).headers.get("stream-next-offset");
  if (offsetTail === null) throw new Error("B2 offset tail missing");
  await compare("/streams/explicit-offset", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/offset", "stream-fork-offset": offsetTail },
  });

  await pairedSource("json", "[1,2]", "application/json", "json-child", {
    "stream-forked-from": "/streams/json",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "1",
  });
  await pairedSource("text", "hello", "text/plain", "text-child", {
    "stream-forked-from": "/streams/text",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "3",
  });
  const largeSubOffset = "x".repeat(10_001);
  await pairedSource("large-text", largeSubOffset, "text/plain", "large-text-child", {
    "stream-forked-from": "/streams/large-text",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "10001",
  });
  await pairedSource(
    "large-binary",
    largeSubOffset,
    "application/octet-stream",
    "large-binary-child",
    {
      "stream-forked-from": "/streams/large-binary",
      "stream-fork-offset": zeroOffset,
      "stream-fork-sub-offset": "10001",
    },
  );
  await pairedSource("invalid", "body", "text/plain", "invalid-child", {
    "stream-forked-from": "/streams/invalid",
    "stream-fork-offset": "bad",
  });
  await pairedSource("beyond", "body", "text/plain", "beyond-child", {
    "stream-forked-from": "/streams/beyond",
    "stream-fork-offset": twoOffset,
  });
  await pairedSource("mismatch", "body", "text/plain", "mismatch-child", {
    "stream-forked-from": "/streams/mismatch",
    "content-type": "application/json",
  });
  await pairedSource("overshoot", "hi", "text/plain", "overshoot-child", {
    "stream-forked-from": "/streams/overshoot",
    "stream-fork-offset": zeroOffset,
    "stream-fork-sub-offset": "3",
  });
  await pairedSource("sub-without-offset", "hello", "text/plain", "sub-without-offset-child", {
    "stream-forked-from": "/streams/sub-without-offset",
    "stream-fork-sub-offset": "1",
  });
  await compare("/streams/absolute-child", {
    method: "PUT",
    headers: { "stream-forked-from": "https://streams.test/streams/offset" },
  });

  expect((await put(harness, "/streams/retry-source", "body")).status).toBe(201);
  expect(
    (
      await bunRequest("/streams/retry-source", {
        method: "PUT",
        body: "body",
        headers: { "content-type": "text/plain" },
      })
    ).status,
  ).toBe(201);
  await compare("/streams/retry-child", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/retry-source" },
  });
  await compare("/streams/retry-child", {
    method: "PUT",
    headers: { "stream-forked-from": "/streams/retry-source" },
  });
});

test("B3 byKey same-object forks chain and retain a soft-deleted source", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect((await put(harness, "/streams/t1/x", "x")).status).toBe(201);
  const fork = await put(harness, "/streams/t1/y", "", "text/plain", {
    "stream-forked-from": "/streams/t1/x",
  });
  expect(fork.status).toBe(201);
  const observation = await probe(harness, "t1");
  expect(messageRowsFor(observation, "t1/y")).toHaveLength(0);
  expect(rowFor(observation, "t1/y")?.[2]).toBe("t1/x");
  expect(observation.exportRequests).toBe(0);
  expect((await dispatch(harness, "/streams/t1/x", { method: "DELETE" })).status).toBe(204);
  expect((await head(harness, "/streams/t1/x")).status).toBe(410);
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(1);
});

test("B4 copied children are independent of source deletion and recreation", async () => {
  const harness = await makeHarness();
  expect((await put(harness, "/streams/src", "old")).status).toBe(201);
  expect(
    (
      await put(harness, "/streams/child", "", "text/plain", {
        "stream-forked-from": "/streams/src",
      })
    ).status,
  ).toBe(201);
  expect((await dispatch(harness, "/streams/src", { method: "DELETE" })).status).toBe(204);
  expect((await head(harness, "/streams/src")).status).toBe(404);
  expect(await dispatch(harness, "/streams/child").then((response) => response.text())).toBe("old");
  expect((await put(harness, "/streams/src", "new")).status).toBe(201);
  expect((await append(harness, "/streams/src", "-again")).status).toBe(204);
  expect(await dispatch(harness, "/streams/child").then((response) => response.text())).toBe("old");
  expect((await dispatch(harness, "/streams/child", { method: "DELETE" })).status).toBe(204);
  expect(messageRowsFor(await probe(harness, "child"), "child")).toHaveLength(0);
});

test("B5 retries avoid source traffic and concurrent copies commit once", async () => {
  const harness = await makeHarness();
  expect((await put(harness, "/streams/src", "a")).status).toBe(201);
  const first = await put(harness, "/streams/retry", "", "text/plain", {
    "stream-forked-from": "/streams/src",
  });
  expect(first.status).toBe(201);
  const sourceAfterFirst = await probe(harness, "src");
  const second = await put(harness, "/streams/retry", "", "text/plain", {
    "stream-forked-from": "/streams/src",
  });
  expect(second.status).toBe(200);
  expect(second.headers.get("stream-next-offset")).toBe(first.headers.get("stream-next-offset"));
  expect((await probe(harness, "src")).exportRequests).toBe(sourceAfterFirst.exportRequests);

  expect((await append(harness, "/streams/src", "b")).status).toBe(204);
  const different = await put(harness, "/streams/retry", "", "text/plain", {
    "stream-forked-from": "/streams/src",
    "stream-fork-offset": oneOffset,
  });
  expect(different.status).toBe(409);

  const concurrent = await Promise.all(
    Array.from({ length: 4 }, () =>
      put(harness, "/streams/concurrent", "", "text/plain", {
        "stream-forked-from": "/streams/src",
      }),
    ),
  );
  expect(concurrent.filter((response) => response.status === 201)).toHaveLength(1);
  expect(concurrent.filter((response) => response.status === 200)).toHaveLength(3);
  expect(messageRowsFor(await probe(harness, "concurrent"), "concurrent")).toHaveLength(2);

  const sourceBefore = (await probe(harness, "src")).messages;
  const six = await Promise.all(
    Array.from({ length: 6 }, (_, index) =>
      put(harness, `/streams/copy-${index}`, "", "text/plain", {
        "stream-forked-from": "/streams/src",
      }),
    ),
  );
  expect(six.every((response) => response.status === 201)).toBe(true);
  expect((await probe(harness, "src")).messages).toEqual(sourceBefore);
});

test("B6 enforces exact encoded-frame bounds and copies seven 1 MiB messages", async () => {
  const harness = await makeHarness();
  const exactBody = "x".repeat(4_051);
  await setProbe(harness, "exact", "copy-limit=4096");
  await setProbe(harness, "exact-child", "copy-limit=4096");
  expect((await put(harness, "/streams/exact", exactBody)).status).toBe(201);
  expect(
    (
      await put(harness, "/streams/exact-child", "", "text/plain", {
        "stream-forked-from": "/streams/exact",
      })
    ).status,
  ).toBe(201);

  await setProbe(harness, "source-capped", "copy-limit=45");
  expect((await put(harness, "/streams/source-capped", "x")).status).toBe(201);
  const sourceCap = await put(harness, "/streams/source-capped-child", "", "text/plain", {
    "stream-forked-from": "/streams/source-capped",
  });
  expect(sourceCap.status).toBe(409);
  expect(await sourceCap.text()).toBe("Fork copy exceeds copyOnForkMaxBytes");

  await setProbe(harness, "over", "copy-limit=4096");
  await setProbe(harness, "over-child", "copy-limit=4096");
  expect((await put(harness, "/streams/over", "x".repeat(4_052))).status).toBe(201);
  const over = await put(harness, "/streams/over-child", "", "text/plain", {
    "stream-forked-from": "/streams/over",
  });
  expect(over.status).toBe(409);
  expect(await over.text()).toBe("Fork copy exceeds copyOnForkMaxBytes");

  await setProbe(harness, "prefix", "copy-limit=4096");
  await setProbe(harness, "prefix-child", "copy-limit=4096");
  expect((await put(harness, "/streams/prefix", exactBody)).status).toBe(201);
  const firstOffset = (await head(harness, "/streams/prefix")).headers.get("stream-next-offset");
  if (firstOffset === null) throw new Error("B6 prefix offset missing");
  expect((await append(harness, "/streams/prefix", "y")).status).toBe(204);
  expect(
    (
      await put(harness, "/streams/prefix-child", "", "text/plain", {
        "stream-forked-from": "/streams/prefix",
        "stream-fork-offset": firstOffset,
      })
    ).status,
  ).toBe(201);

  const megabyte = "m".repeat(1_048_576);
  expect((await put(harness, "/streams/seven", megabyte)).status).toBe(201);
  for (let index = 0; index < 6; index += 1)
    expect((await append(harness, "/streams/seven", megabyte)).status).toBe(204);
  const started = Date.now();
  const seven = await put(harness, "/streams/seven-child", "", "text/plain", {
    "stream-forked-from": "/streams/seven",
  });
  const elapsed = Date.now() - started;
  console.info(`B6 seven-megabyte-copy-ms=${elapsed}`);
  expect(seven.status).toBe(201);
  expect(messageRowsFor(await probe(harness, "seven-child"), "seven-child")).toHaveLength(7);
  expect((await append(harness, "/streams/seven", megabyte)).status).toBe(204);
  const eighth = await put(harness, "/streams/eight-child", "", "text/plain", {
    "stream-forked-from": "/streams/seven",
  });
  expect(eighth.status).toBe(409);
  expect(await eighth.text()).toBe("Fork copy exceeds copyOnForkMaxBytes");
});

test.skipIf(Bun.env.STREAMSY_FORK_TIMING !== "1")(
  "B6 timing observation (opt-in because 20,000 small messages is host-duration sensitive)",
  async () => {
    const harness = await makeHarness();
    expect((await put(harness, "/streams/timing", "0123456789")).status).toBe(201);
    for (let index = 1; index < 20_000; index += 1)
      expect((await append(harness, "/streams/timing", "0123456789")).status).toBe(204);
    const started = Date.now();
    expect(
      (
        await put(harness, "/streams/timing-child", "", "text/plain", {
          "stream-forked-from": "/streams/timing",
        })
      ).status,
    ).toBe(201);
    console.info(`B6 twenty-thousand-small-copy-ms=${Date.now() - started}`);
  },
  { timeout: 60_000 },
);

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
  const deletedResponse = await put(deleted, "/streams/t2/z", "", "text/plain", {
    "stream-forked-from": "/streams/t1/x",
  });
  expect(deletedResponse.status).toBe(409);
  expect(await deletedResponse.text()).toBe("Source stream is soft-deleted: t1/x");

  const expired = await makeHarness();
  expect(
    (await put(expired, "/streams/expired", "x", "text/plain", { "stream-ttl": "1" })).status,
  ).toBe(201);
  await Bun.sleep(2_000);
  const expiredResponse = await put(expired, "/streams/expired-child", "", "text/plain", {
    "stream-forked-from": "/streams/expired",
  });
  expect(expiredResponse.status).toBe(404);
  expect(await expiredResponse.text()).toBe("Source stream not found: expired");
});

test("B8 public routes ignore markers while direct frames export is bounded and alarm-free", async () => {
  const harness = await makeHarness();
  expect((await put(harness, "/streams/src", "body")).status).toBe(201);
  const before = await probe(harness, "src");
  const publicRead = await dispatch(harness, "/streams/src", {
    headers: { "streamsy-fork-source": "1", "streamsy-frames-truncated": "1" },
  });
  expect(publicRead.status).toBe(200);
  expect(await publicRead.text()).toBe("body");
  const routedInternal = await dispatchFetch(
    harness.miniflare,
    "https://streamsy.internal/fork-source?stream=src",
  );
  expect(routedInternal.status).toBe(400);
  expect(await routedInternal.text()).toBe("Stream path required: /streams/{path}");
  const exported = await directInternal(
    harness,
    "src",
    "/fork-source?stream=src&budget=1048576&tail=0",
  );
  expect(exported.status).toBe(200);
  expect(exported.headers.get("content-type")).toBe("application/vnd.streamsy.frames");
  expect(decodeFrames(new Uint8Array(await exported.arrayBuffer()))).toHaveLength(1);
  expect((await probe(harness, "src")).alarmInvocations).toBe(before.alarmInvocations);
});

test("B9 byKey placement isolates copied families", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  expect((await put(harness, "/streams/t1/x", "x")).status).toBe(201);
  expect((await put(harness, "/streams/t2/y", "y")).status).toBe(201);
  const before = await probe(harness, "t1");
  expect(
    (
      await put(harness, "/streams/t2/z", "", "text/plain", {
        "stream-forked-from": "/streams/t1/x",
      })
    ).status,
  ).toBe(201);
  expect(await (await direct(harness, "t2", "/streams/t2/z")).text()).toBe("x");
  expect((await direct(harness, "t1", "/streams/t2/z")).status).toBe(404);
  expect((await probe(harness, "t1")).rows).toEqual(before.rows);
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(2);
});

test("B10 no namespace and mismatched placement fail safely", async () => {
  const noNamespace = await makeHarness("worker-no-namespace.ts");
  expect((await put(noNamespace, "/streams/src", "x")).status).toBe(201);
  const unsupported = await put(noNamespace, "/streams/child", "", "text/plain", {
    "stream-forked-from": "/streams/src",
  });
  expect(unsupported.status).toBe(400);
  expect(unsupported.headers.get("stream-not-supported")).toBe("fork");
  expect(await unsupported.text()).toBe("Feature not supported: fork");

  const mismatch = await makeHarness("worker-mismatch.ts");
  const notFound = await put(mismatch, "/streams/t1/y", "", "text/plain", {
    "stream-forked-from": "/streams/t1/x",
  });
  expect(notFound.status).toBe(404);
  expect(await notFound.text()).toBe("Source stream not found: t1/x");
  expect((await probe(mismatch, "t1")).rows).toHaveLength(0);
});

test("B11 copied TTL and absolute expiry inherit into the child alarm", async () => {
  const harness = await makeHarness();
  expect(
    (await put(harness, "/streams/ttl-source", "x", "text/plain", { "stream-ttl": "2" })).status,
  ).toBe(201);
  const now = Date.now();
  expect(
    (
      await put(harness, "/streams/ttl-child", "", "text/plain", {
        "stream-forked-from": "/streams/ttl-source",
      })
    ).status,
  ).toBe(201);
  const child = await probe(harness, "ttl-child");
  const expiry = child.rows[0]?.[1];
  if (!isFiniteNumber(expiry)) throw new Error("B11 inherited TTL was not persisted");
  expect(expiry - now).toBeGreaterThan(500);
  expect(expiry - now).toBeLessThan(2_500);
  await waitUntil(async () => (await probe(harness, "ttl-child")).rows.length === 0, 4_000);
  const expired = await probe(harness, "ttl-child");
  expect(expired.alarm).toBeNull();
  expect(expired.alarmInvocations).toBeGreaterThanOrEqual(1);

  expect(
    (await put(harness, "/streams/ttl-override", "x", "text/plain", { "stream-ttl": "2" })).status,
  ).toBe(201);
  expect(
    (
      await put(harness, "/streams/override-child", "", "text/plain", {
        "stream-forked-from": "/streams/ttl-override",
        "stream-ttl": "60",
      })
    ).status,
  ).toBe(201);
  const override = await probe(harness, "override-child");
  const overrideExpiry = override.rows[0]?.[1];
  if (!isFiniteNumber(overrideExpiry)) throw new Error("B11 override expiry was not persisted");
  expect(overrideExpiry - Date.now()).toBeGreaterThan(58_000);

  const absolute = new Date(Date.now() + 2_000).toISOString();
  expect(
    (
      await put(harness, "/streams/date-source", "x", "text/plain", {
        "stream-expires-at": absolute,
      })
    ).status,
  ).toBe(201);
  expect(
    (
      await put(harness, "/streams/date-child", "", "text/plain", {
        "stream-forked-from": "/streams/date-source",
      })
    ).status,
  ).toBe(201);
  const dateChild = await probe(harness, "date-child");
  const dateExpiry = dateChild.rows[0]?.[1];
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

test(
  "B14 repeated alarm failure recovers through the next mutation",
  async () => {
    const harness = await makeHarness();
    expect(
      (await put(harness, "/streams/recover", "x", "text/plain", { "stream-ttl": "1" })).status,
    ).toBe(201);
    await setProbe(harness, "recover", "fail-expiry-while=1");
    await waitUntil(async () => (await probe(harness, "recover")).alarmInfo.length >= 2, 5_000);
    const failed = await probe(harness, "recover");
    expect(failed.alarmInfo.some((info) => info.isRetry)).toBe(true);
    expect(failed.rows).toHaveLength(1);
    const [firstAlarm, retryAlarm] = failed.alarmInfo;
    const retryScheduledDeltaMs =
      firstAlarm === undefined || retryAlarm === undefined
        ? null
        : retryAlarm.scheduledTime - firstAlarm.scheduledTime;
    const retryObservedDelayMs =
      firstAlarm === undefined || retryAlarm === undefined
        ? null
        : retryAlarm.observedAt - firstAlarm.observedAt;
    console.info(
      `B14 alarm-invocations=${failed.alarmInfo.length} retry-observed-delay-ms=${retryObservedDelayMs ?? "n/a"} retry-scheduled-delta-ms=${retryScheduledDeltaMs ?? "n/a"}`,
    );
    if (retryObservedDelayMs === null) throw new Error("B14 retry observation timestamp missing");
    expect(retryObservedDelayMs).toBeGreaterThanOrEqual(0);
    await setProbe(harness, "recover", "clear-fail-expiry=1");
    expect((await append(harness, "/streams/recover", "repair")).status).toBe(404);
    await waitUntil(async () => (await probe(harness, "recover")).rows.length === 0, 4_000);
    const recovered = await probe(harness, "recover");
    expect(recovered.layerAcquisitions).toBe(1);
    expect(recovered.migrationAttempts).toBe(1);
  },
  { timeout: 12_000 },
);

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
        "stream-forked-from": "/streams/source",
      })
    ).status,
  ).toBe(201);
  const byKeyRoot = byKey.root;
  const byStreamRoot = byStream.root;
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
  const recreatedByKey = await makeHarness("worker-by-key.ts", byKeyRoot);
  const recreatedByStream = await makeHarness("worker.ts", byStreamRoot);
  await waitUntil(async () => (await probe(recreatedByKey, "t1")).rows.length === 0, 5_000);
  await waitUntil(async () => (await probe(recreatedByStream, "child")).rows.length === 0, 5_000);
  expect((await probe(recreatedByKey, "t1")).alarm).toBeNull();
  expect((await probe(recreatedByStream, "child")).alarm).toBeNull();
  expect((await probe(recreatedByKey, "t1")).layerAcquisitions).toBe(1);
  expect((await probe(recreatedByStream, "child")).layerAcquisitions).toBe(1);
});

test("B16 copied creates reconcile an inherited alarm before the PUT returns", async () => {
  const harness = await makeHarness();
  expect(
    (await put(harness, "/streams/alarm-source", "x", "text/plain", { "stream-ttl": "2" })).status,
  ).toBe(201);
  expect(
    (await direct(harness, "alarm-child", "/streams/alarm-child", { method: "HEAD" })).status,
  ).toBe(404);
  const before = await probe(harness, "alarm-child");
  expect(before.alarm).toBeNull();
  expect(
    (
      await put(harness, "/streams/alarm-child", "", "text/plain", {
        "stream-forked-from": "/streams/alarm-source",
      })
    ).status,
  ).toBe(201);
  const after = await probe(harness, "alarm-child");
  expect(after.alarm).not.toBeNull();
  expect(after.rows[0]?.[1]).toBeGreaterThan(Date.now());
});

test(
  "Batch B F1 fails closed when a low-yield export meets source deletion and reincarnation",
  async () => {
    const harness = await makeHarness("worker-low-yield.ts");
    const original = largeJson("old");
    const replacement = largeJson("new");
    expect((await put(harness, "/streams/src", original, "application/json")).status).toBe(201);

    const forkPromise = put(harness, "/streams/child", "", "application/json", {
      "stream-forked-from": "/streams/src",
    });
    await Bun.sleep(2);
    expect((await dispatch(harness, "/streams/src", { method: "DELETE" })).status).toBe(204);
    expect((await put(harness, "/streams/src", replacement, "application/json")).status).toBe(201);

    const fork = await forkPromise;
    if (fork.status === 201) {
      expect(fork.headers.get("stream-next-offset")).toBe("0000000000003000_0000000000000000");
      expect(messageRowsFor(await probe(harness, "child"), "child")).toHaveLength(3_000);
      const childBody = await dispatch(harness, "/streams/child").then((response) =>
        response.text(),
      );
      expect([original, replacement]).toContain(childBody);
    } else {
      expect(fork.status).not.toBe(201);
    }
  },
  { timeout: 20_000 },
);

test(
  "Batch B F2 keeps large same-object creates and source reincarnation bounded",
  async () => {
    const harness = await makeHarness("worker-by-key.ts");
    const first = largeJson("first");
    const second = largeJson("second");
    const concurrent = await Promise.all([
      put(harness, "/streams/t1/x", first, "application/json"),
      put(harness, "/streams/t1/y", second, "application/json"),
    ]);
    expect(concurrent.map((response) => response.status)).toEqual([201, 201]);
    const family = await probe(harness, "t1");
    expect(messageRowsFor(family, "t1/x")).toHaveLength(3_000);
    expect(messageRowsFor(family, "t1/y")).toHaveLength(3_000);
    expect(family.layerAcquisitions).toBe(1);

    const source = largeJson("source");
    const recreated = largeJson("recreated");
    expect((await put(harness, "/streams/t2/source", source, "application/json")).status).toBe(201);
    const started = Date.now();
    const forkPromise = put(harness, "/streams/t3/child", "", "application/json", {
      "stream-forked-from": "/streams/t2/source",
    });
    await Bun.sleep(2);
    expect((await dispatch(harness, "/streams/t2/source", { method: "DELETE" })).status).toBe(204);
    expect((await put(harness, "/streams/t2/source", recreated, "application/json")).status).toBe(
      201,
    );
    const fork = await forkPromise;
    const elapsed = Date.now() - started;
    expect(elapsed).toBeLessThan(10_000);
    expect([201, 404, 500]).toContain(fork.status);
    if (fork.status === 201) {
      expect(messageRowsFor(await probe(harness, "t3"), "t3/child")).toHaveLength(3_000);
    }
    const sourceAfter = await probe(harness, "t2");
    expect(messageRowsFor(sourceAfter, "t2/source")).toHaveLength(3_000);
    expect(sourceAfter.layerAcquisitions).toBe(1);
  },
  { timeout: 35_000 },
);
