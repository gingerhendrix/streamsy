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
import { Effect, Layer } from "effect";
import { Memory, Protocol } from "@streamsy/core";
import { testHost, type TestHost, type TestHostOptions } from "./support/scoped-host.ts";
import { Miniflare } from "miniflare";

const retentionRoot = Bun.env.STREAMSY_STORAGE_SCRATCH;

interface Harness {
  readonly bundleRoot: string;
  readonly miniflare: HarnessProcess;
  readonly ownership: OwnedHarness;
  readonly root: string;
  readonly namespace: TestNamespace;
}

interface OwnedHarness {
  readonly bundleRoot: string;
  readonly miniflare: HarnessProcess;
  readonly root: string;
  readonly cleanup: HarnessCleanup;
  disposed: boolean;
}

interface HarnessProcess {
  readonly ready: Promise<void>;
  readonly dispose: () => Promise<void>;
  readonly getDurableObjectNamespace: (binding: string) => Promise<TestNamespace>;
  readonly listDurableObjectIds: (binding: string) => Promise<ReadonlyArray<string>>;
  readonly dispatchFetch: (input: string | Request, init?: RequestInit) => Promise<Response>;
}

interface HarnessCleanup {
  readonly remove: (path: string) => void;
  readonly copy: (source: string, destination: string) => void;
}

interface HarnessDependencies extends HarnessCleanup {
  readonly build: (entry: string, outdir: string) => Promise<string>;
  readonly create: (scriptPath: string, root: string) => HarnessProcess;
  readonly namespace: (process: HarnessProcess) => Promise<TestNamespace>;
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

const realHarnessDependencies: HarnessDependencies = {
  build: async (entry, outdir) => {
    const built = await Bun.build({
      entrypoints: [join(import.meta.dir, "support/cloudflare", entry)],
      outdir,
      target: "browser",
      format: "esm",
      external: ["cloudflare:workers"],
    });
    if (!built.success) throw new Error("Batch B workerd proof build failed");
    const output = built.outputs[0];
    if (output === undefined) throw new Error("Batch B workerd proof produced no bundle");
    return output.path;
  },
  create: (scriptPath, root) => {
    const miniflare = new Miniflare({
      scriptPath,
      modules: true,
      compatibilityDate: "2026-08-06",
      host: "127.0.0.1",
      port: 0,
      cf: false,
      durableObjects: { STREAMS: { className: "ProbeObject", useSQLite: true } },
      durableObjectsPersist: join(root, "state"),
    });
    return {
      ready: miniflare.ready.then(() => undefined),
      dispose: () => miniflare.dispose(),
      getDurableObjectNamespace: async (binding) => {
        const namespace = await miniflare.getDurableObjectNamespace(binding);
        // SAFETY: the local fixture exposes only idFromName/get/fetch to this harness.
        return namespace as unknown as TestNamespace;
      },
      listDurableObjectIds: (binding) => miniflare.listDurableObjectIds(binding),
      dispatchFetch: (input, init) =>
        // SAFETY: Miniflare's local dispatch is adapted to the Bun Fetch signature used here.
        (
          miniflare.dispatchFetch as unknown as (
            input: string | Request,
            init?: RequestInit,
          ) => Promise<Response>
        )(input, init),
    };
  },
  namespace: (process) => process.getDurableObjectNamespace("STREAMS"),
  remove: (path) => rmSync(path, { recursive: true, force: true }),
  copy: (source, destination) => cpSync(source, destination, { recursive: true }),
};

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
  readonly rows: ReadonlyArray<ReadonlyArray<unknown>>;
  readonly messages: ReadonlyArray<ReadonlyArray<unknown>>;
}

const open: Array<Harness> = [];
const pending = new Set<OwnedHarness>();
const openBun: Array<{ readonly stop: () => Promise<void> }> = [];

/** Start one Bun host for this suite and expose a Promise-shaped stop. */
const startBun = (options: TestHostOptions): Promise<TestHost> =>
  Effect.runPromise(testHost(options));
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

const retentionDestination = (root: string, configured = retentionRoot): string | undefined => {
  if (configured === undefined) return undefined;
  if (!isAbsolute(configured)) throw new Error("STREAMSY_STORAGE_SCRATCH must be absolute");
  rejectSymlinkPath(root);
  const sourceReal = realpathSync(root);
  const retention = resolve(configured);
  rejectSymlinkPath(retention);
  if (retention === sourceReal || isDescendant(sourceReal, retention)) {
    throw new Error("STREAMSY_STORAGE_SCRATCH must not point inside the owned harness root");
  }
  mkdirSync(retention, { recursive: true });
  rejectSymlinkPath(retention);
  const destination = resolve(retention, basename(root));
  rejectSymlinkPath(destination);
  const destinationReal = existsSync(destination)
    ? realpathSync(destination)
    : resolve(realpathSync(retention), basename(destination));
  if (destinationReal === sourceReal || isDescendant(sourceReal, destinationReal)) {
    throw new Error("STREAMSY_STORAGE_SCRATCH must not point inside the owned harness root");
  }
  return destination;
};

const reclaimOwnedRoot = (
  root: string,
  errors: Array<unknown>,
  resolveDestination: () => string | undefined = () => retentionDestination(root),
  copy: (source: string, destination: string) => void = (source, destination) => {
    mkdirSync(resolve(destination, ".."), { recursive: true });
    cpSync(source, destination, { recursive: true });
  },
  remove: (path: string) => void = (path) => rmSync(path, { recursive: true, force: true }),
): void => {
  try {
    const destination = resolveDestination();
    if (destination !== undefined) copy(root, destination);
  } catch (error) {
    errors.push(error);
  }
  try {
    remove(root);
    ownedRoots.delete(root);
  } catch (error) {
    errors.push(error);
  }
};
// SAFETY: JSON probe values are untrusted here; Number.isFinite is the parser for this narrow test domain.
// oxlint-disable-next-line anti-slop/no-unknown-parameters
const isFiniteNumber = (value: unknown): value is number => Number.isFinite(value);

const makeHarness = async (
  entry = "worker.ts",
  root?: string,
  dependencies: HarnessDependencies = realHarnessDependencies,
) => {
  const ownedRoot = root ?? mkdtempSync(join(tmpdir(), ".streamsy-cloudflare-batch-b-"));
  ownedRoots.add(ownedRoot);
  let bundleRoot: string | undefined;
  let ownership: OwnedHarness | undefined;
  try {
    bundleRoot = mkdtempSync(".streamsy-cloudflare-batch-b-bundle-");
    ownedBundles.add(bundleRoot);
    const bundle = join(bundleRoot, `bundle-${crypto.randomUUID()}`);
    const scriptPath = await dependencies.build(entry, bundle);
    const miniflare = dependencies.create(scriptPath, ownedRoot);
    ownership = {
      bundleRoot,
      miniflare,
      root: ownedRoot,
      cleanup: dependencies,
      disposed: false,
    };
    pending.add(ownership);
    await miniflare.ready;
    const namespace = await dependencies.namespace(miniflare);
    const harness = { bundleRoot, miniflare, ownership, root: ownedRoot, namespace };
    open.push(harness);
    return harness;
  } catch (error) {
    const cleanupErrors: Array<unknown> = [];
    if (ownership !== undefined) {
      try {
        await ownership.miniflare.dispose();
        ownership.disposed = true;
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (ownership?.disposed === true && bundleRoot !== undefined) {
      try {
        dependencies.remove(bundleRoot);
        ownedBundles.delete(bundleRoot);
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (ownership?.disposed === true && root === undefined) {
      try {
        dependencies.remove(ownedRoot);
        ownedRoots.delete(ownedRoot);
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (ownership?.disposed === true && cleanupErrors.length === 0) pending.delete(ownership);
    if (cleanupErrors.length > 0 || ownership?.disposed !== true) {
      // oxlint-disable-next-line preserve-caught-error -- AggregateError retains the primary acquisition error and every cleanup error.
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

type CleanupHarness = Pick<Harness, "bundleRoot" | "miniflare" | "ownership" | "root">;

const disposeHarness = async (harness: CleanupHarness, retainRoot = false) => {
  const cleanupErrors: Array<unknown> = [];
  if (!harness.ownership.disposed) {
    try {
      await harness.miniflare.dispose();
      harness.ownership.disposed = true;
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (harness.ownership.disposed) {
    const index = open.findIndex((item) => item.ownership === harness.ownership);
    if (index >= 0) open.splice(index, 1);
  }
  if (harness.ownership.disposed && ownedBundles.has(harness.bundleRoot)) {
    try {
      harness.ownership.cleanup.remove(harness.bundleRoot);
      ownedBundles.delete(harness.bundleRoot);
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (harness.ownership.disposed && !retainRoot && ownedRoots.has(harness.root)) {
    try {
      const destination = retentionDestination(harness.root);
      if (destination !== undefined) {
        mkdirSync(resolve(destination, ".."), { recursive: true });
        harness.ownership.cleanup.copy(harness.root, destination);
      }
    } catch (error) {
      cleanupErrors.push(error);
    }
    try {
      harness.ownership.cleanup.remove(harness.root);
      ownedRoots.delete(harness.root);
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (
    harness.ownership.disposed &&
    !ownedBundles.has(harness.bundleRoot) &&
    (retainRoot || !ownedRoots.has(harness.root))
  )
    pending.delete(harness.ownership);
  if (cleanupErrors.length > 0 || !harness.ownership.disposed)
    throw new AggregateError(
      cleanupErrors.length > 0
        ? cleanupErrors
        : [new Error("Miniflare disposal remains unresolved")],
      "Cloudflare fork harness cleanup failed",
    );
};

const cleanupOwnedHarnesses = async (): Promise<void> => {
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
  for (const harness of open.slice()) {
    try {
      await disposeHarness(harness);
    } catch (error) {
      errors.push(error);
    }
  }
  for (const harness of open.slice()) {
    try {
      await disposeHarness(harness);
    } catch (error) {
      errors.push(error);
    }
  }
  for (let attempt = 0; attempt < 2; attempt += 1) {
    for (const ownership of Array.from(pending)) {
      if (open.some((harness) => harness.ownership === ownership)) continue;
      try {
        const harness: CleanupHarness = {
          bundleRoot: ownership.bundleRoot,
          miniflare: ownership.miniflare,
          ownership,
          root: ownership.root,
        };
        await disposeHarness(harness);
      } catch (error) {
        errors.push(error);
      }
    }
  }
  const pendingRoots = new Set(Array.from(pending, (ownership) => ownership.root));
  const pendingBundles = new Set(Array.from(pending, (ownership) => ownership.bundleRoot));
  for (const bundleRoot of ownedBundles) {
    if (pendingBundles.has(bundleRoot)) continue;
    try {
      rmSync(bundleRoot, { recursive: true, force: true });
      ownedBundles.delete(bundleRoot);
    } catch (error) {
      errors.push(error);
    }
  }
  for (const root of ownedRoots) {
    if (pendingRoots.has(root)) continue;
    reclaimOwnedRoot(root, errors);
  }
  if (errors.length > 0)
    throw new AggregateError(errors, "Cloudflare fork afterEach cleanup failed");
};

afterEach(cleanupOwnedHarnesses);

test("owned-root reclamation removes a root after retention copy failure", () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-fork-reclaim-"));
  const errors: Array<unknown> = [];
  let removed = false;
  reclaimOwnedRoot(
    root,
    errors,
    () => "/retained/root",
    () => {
      throw new Error("copy failed");
    },
    () => {
      removed = true;
      rmSync(root, { recursive: true, force: true });
    },
  );
  expect(removed).toBe(true);
  expect(errors).toHaveLength(1);
});

test("retention paths reject descendants and support a fresh destination", () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-fork-retention-"));
  const retention = mkdtempSync(join(tmpdir(), ".streamsy-fork-retention-target-"));
  const fresh = join(retention, "fresh");
  try {
    expect(retentionDestination(root, fresh)).toBe(join(fresh, basename(root)));
    expect(() => retentionDestination(root, root)).toThrow();
  } finally {
    rmSync(root, { recursive: true, force: true });
    rmSync(retention, { recursive: true, force: true });
  }
});

const fakeHarnessDependencies = (
  options: {
    readonly readyFailure?: boolean;
    readonly namespaceFailure?: boolean;
    readonly disposeFailures?: number;
    readonly removeFailures?: number;
  } = {},
): HarnessDependencies => {
  let disposeAttempts = 0;
  let removeAttempts = 0;
  const process: HarnessProcess = {
    ready: options.readyFailure ? Promise.reject(new Error("ready failed")) : Promise.resolve(),
    dispose: async () => {
      disposeAttempts += 1;
      if (disposeAttempts <= (options.disposeFailures ?? 0)) throw new Error("dispose failed");
    },
    getDurableObjectNamespace: async () => ({
      idFromName: (name: string) => ({ name }),
      get: () => ({ fetch: () => new Response() }),
    }),
    listDurableObjectIds: async () => [],
    dispatchFetch: async () => new Response(),
  };
  return {
    build: async (_entry, outdir) => join(outdir, "worker.js"),
    create: () => process,
    namespace: async () => {
      if (options.namespaceFailure) throw new Error("namespace failed");
      return {
        idFromName: (name) => ({ name }),
        get: () => ({ fetch: () => new Response() }),
      };
    },
    remove: (path) => {
      removeAttempts += 1;
      if (removeAttempts <= (options.removeFailures ?? 0)) throw new Error("remove failed");
      rmSync(path, { recursive: true, force: true });
    },
    copy: (source, destination) => cpSync(source, destination, { recursive: true }),
  };
};

const expectRejected = async (operation: Promise<unknown>, message: string): Promise<void> => {
  let caught: unknown;
  try {
    await operation;
  } catch (error) {
    caught = error;
  }
  if (!(caught instanceof Error)) throw new Error(`Expected rejection containing ${message}`);
  expect(caught.message).toContain(message);
};

test("real acquisition and afterEach retry retain a failed ready owner", async () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-fork-acquire-state-"));
  const dependencies = fakeHarnessDependencies({ readyFailure: true, disposeFailures: 1 });
  try {
    await expectRejected(
      makeHarness("worker.ts", root, dependencies),
      "harness acquisition failed",
    );
    expect(pending.size).toBe(1);
    expect(ownedRoots.has(root)).toBe(true);
    await cleanupOwnedHarnesses().catch(() => undefined);
    expect(pending.size).toBe(0);
    expect(ownedRoots.has(root)).toBe(false);
  } finally {
    await cleanupOwnedHarnesses().catch(() => undefined);
    rmSync(root, { recursive: true, force: true });
  }
});

test("namespace acquisition and persistent disposal retain ownership until recovery", async () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-fork-namespace-state-"));
  const dependencies = fakeHarnessDependencies({ namespaceFailure: true, disposeFailures: 4 });
  try {
    await expectRejected(
      makeHarness("worker.ts", root, dependencies),
      "harness acquisition failed",
    );
    expect(pending.size).toBe(1);
    await cleanupOwnedHarnesses().catch(() => undefined);
    expect(pending.size).toBe(1);
    await cleanupOwnedHarnesses().catch(() => undefined);
    expect(pending.size).toBe(0);
  } finally {
    await cleanupOwnedHarnesses().catch(() => undefined);
    rmSync(root, { recursive: true, force: true });
  }
});

test("actual hook retries root removal after acquisition cleanup", async () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-fork-remove-state-"));
  const dependencies = fakeHarnessDependencies({ removeFailures: 6 });
  try {
    const acquired = await makeHarness("worker.ts", root, dependencies);
    expect(acquired).toBeDefined();
    await cleanupOwnedHarnesses().catch(() => undefined);
    expect(ownedRoots.has(root)).toBe(true);
    await cleanupOwnedHarnesses();
    expect(ownedRoots.has(root)).toBe(false);
  } finally {
    await cleanupOwnedHarnesses().catch(() => undefined);
    rmSync(root, { recursive: true, force: true });
  }
});

test("B15 recreation failure reclaims the retained owner through the hook", async () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-fork-recreate-state-"));
  const firstDependencies = fakeHarnessDependencies();
  const secondDependencies = fakeHarnessDependencies({ readyFailure: true });
  try {
    const first = await makeHarness("worker.ts", root, firstDependencies);
    await disposeHarness(first, true);
    expect(ownedRoots.has(root)).toBe(true);
    await expectRejected(makeHarness("worker.ts", root, secondDependencies), "ready failed");
    expect(ownedRoots.has(root)).toBe(true);
    await cleanupOwnedHarnesses().catch(() => undefined);
    expect(ownedRoots.has(root)).toBe(false);
  } finally {
    await cleanupOwnedHarnesses().catch(() => undefined);
    rmSync(root, { recursive: true, force: true });
  }
});

const dispatchFetch = (
  miniflare: HarnessProcess,
  input: string | Request,
  init?: RequestInit,
): Promise<Response> =>
  // SAFETY: Miniflare's declaration uses the workers-types Request overload;
  // this adapter exposes the equivalent Bun Fetch signature used by the tests.
  miniflare.dispatchFetch(input, init);

const dispatch = (harness: Harness, path: string, init?: RequestInit) =>
  dispatchFetch(harness.miniflare, `https://streams.test${path}`, init);

const direct = (harness: Harness, name: string, path: string, init?: RequestInit) =>
  harness.namespace
    .get(harness.namespace.idFromName(name))
    .fetch(new Request(`https://object.test${path}`, init));

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
const twoOffset = "0000000000000000_0000000000000002";
const largeJson = (prefix: string) =>
  JSON.stringify(Array.from({ length: 3_000 }, (_, index) => `${prefix}-${index}`));

test("B2 Cloudflare fork classifications are byte-parity with Bun", async () => {
  const harness = await makeHarness("worker-by-key.ts");
  const bun = await startBun({
    pathPrefix: "/streams",
    layer: Protocol.layer().pipe(Layer.provide(Memory.layer())),
    port: 0,
  });
  openBun.push({ stop: () => Effect.runPromise(bun.stop) });
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

test("B9 byKey rejects cross-family forks without changing the source", async () => {
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

const recreateB15Harnesses = async (
  byKey: Harness,
  byStream: Harness,
  acquire: (entry: string, root: string) => Promise<Harness> = (entry, root) =>
    makeHarness(entry, root),
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
    byKey: await acquire("worker-by-key.ts", byKey.root),
    byStream: await acquire("worker.ts", byStream.root),
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

const runB15ReacquisitionFailure = async (failureAt: 1 | 2): Promise<void> => {
  const byKeyRoot = mkdtempSync(join(tmpdir(), ".streamsy-fork-b15-key-fault-"));
  const byStreamRoot = mkdtempSync(join(tmpdir(), ".streamsy-fork-b15-stream-fault-"));
  try {
    const byKey = await makeHarness("worker-by-key.ts", byKeyRoot, fakeHarnessDependencies());
    const byStream = await makeHarness("worker.ts", byStreamRoot, fakeHarnessDependencies());
    let acquisition = 0;
    await expectRejected(
      recreateB15Harnesses(byKey, byStream, (entry, root) => {
        acquisition += 1;
        return makeHarness(
          entry,
          root,
          fakeHarnessDependencies({ readyFailure: acquisition === failureAt }),
        );
      }),
      "ready failed",
    );
    expect(acquisition).toBe(failureAt);
    expect(byKey.ownership.disposed).toBe(true);
    expect(byStream.ownership.disposed).toBe(true);
    expect(ownedRoots.has(byKeyRoot)).toBe(true);
    expect(ownedRoots.has(byStreamRoot)).toBe(true);
    await cleanupOwnedHarnesses();
    expect(ownedRoots.has(byKeyRoot)).toBe(false);
    expect(ownedRoots.has(byStreamRoot)).toBe(false);
  } finally {
    await cleanupOwnedHarnesses().catch(() => undefined);
    rmSync(byKeyRoot, { recursive: true, force: true });
    rmSync(byStreamRoot, { recursive: true, force: true });
  }
};

test("B15 first reacquisition failure retains and reclaims both roots", async () => {
  await runB15ReacquisitionFailure(1);
});

test("B15 second reacquisition failure retains and reclaims both roots", async () => {
  await runB15ReacquisitionFailure(2);
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
    const family = await probe(harness, "t1");
    expect(messageRowsFor(family, "t1/x")).toHaveLength(3_000);
    expect(messageRowsFor(family, "t1/y")).toHaveLength(3_000);
    expect(family.layerAcquisitions).toBe(1);
  },
  { timeout: 35_000 },
);
