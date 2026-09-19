/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, anti-slop/no-chained-type-assertions -- This is the real local workerd boundary test; Miniflare exposes a workers-types Fetch overload while the Bun test uses Bun Fetch values. */
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
      entrypoints: [join(import.meta.dir, "cloudflare", entry)],
      outdir,
      target: "browser",
      format: "esm",
      external: ["cloudflare:workers"],
    });
    if (!built.success) throw new Error("Cloudflare host proof build failed");
    const output = built.outputs[0];
    if (output === undefined) throw new Error("Cloudflare host proof produced no bundle");
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
  readonly alarmInfo: ReadonlyArray<{ readonly isRetry: boolean; readonly retryCount: number }>;
  readonly alarm: number | null;
  readonly rows: ReadonlyArray<ReadonlyArray<unknown>>;
}

const open: Array<Harness> = [];
const pending = new Set<OwnedHarness>();
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

const makeHarness = async (
  entry = "worker.ts",
  root?: string,
  dependencies: HarnessDependencies = realHarnessDependencies,
) => {
  const ownedRoot = root ?? mkdtempSync(join(tmpdir(), ".streamsy-cloudflare-workerd-"));
  ownedRoots.add(ownedRoot);
  let bundleRoot: string | undefined;
  let ownership: OwnedHarness | undefined;
  try {
    bundleRoot = mkdtempSync(".streamsy-cloudflare-workerd-bundle-");
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
        "Cloudflare host harness acquisition failed",
        {
          cause: error,
        },
      );
    }
    throw error;
  }
};

type CleanupHarness = Pick<Harness, "bundleRoot" | "miniflare" | "ownership" | "root">;

const disposeHarness = async (harness: CleanupHarness, retainRoot = false): Promise<void> => {
  const errors: Array<unknown> = [];
  if (!harness.ownership.disposed) {
    try {
      await harness.miniflare.dispose();
      harness.ownership.disposed = true;
    } catch (error) {
      errors.push(error);
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
      errors.push(error);
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
      errors.push(error);
    }
    try {
      harness.ownership.cleanup.remove(harness.root);
      ownedRoots.delete(harness.root);
    } catch (error) {
      errors.push(error);
    }
  }
  if (
    harness.ownership.disposed &&
    !ownedBundles.has(harness.bundleRoot) &&
    (retainRoot || !ownedRoots.has(harness.root))
  )
    pending.delete(harness.ownership);
  if (errors.length > 0 || !harness.ownership.disposed)
    throw new AggregateError(
      errors.length > 0 ? errors : [new Error("Miniflare disposal remains unresolved")],
      "Cloudflare host harness cleanup failed",
    );
};

const cleanupOwnedHarnesses = async (): Promise<void> => {
  const errors: Array<unknown> = [];
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
    throw new AggregateError(errors, "Cloudflare host afterEach cleanup failed");
};

afterEach(cleanupOwnedHarnesses);

test("owned-root reclamation removes a root after retention copy failure", () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-host-reclaim-"));
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
  const root = mkdtempSync(join(tmpdir(), ".streamsy-host-retention-"));
  const retention = mkdtempSync(join(tmpdir(), ".streamsy-host-retention-target-"));
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
  const root = mkdtempSync(join(tmpdir(), ".streamsy-host-acquire-state-"));
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
  const root = mkdtempSync(join(tmpdir(), ".streamsy-host-namespace-state-"));
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
  const root = mkdtempSync(join(tmpdir(), ".streamsy-host-remove-state-"));
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

test("host recreation failure reclaims the retained owner through the hook", async () => {
  const root = mkdtempSync(join(tmpdir(), ".streamsy-host-recreate-state-"));
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
  // SAFETY: Miniflare dispatches the same local workerd boundary, while its
  // declaration uses the workers-types Request overload instead of Bun's.
  miniflare.dispatchFetch(input, init);

const dispatch = (harness: Harness, path: string, init?: RequestInit) =>
  dispatchFetch(harness.miniflare, `https://streams.test${path}`, init);

const direct = (harness: Harness, name: string, path: string, init?: RequestInit) =>
  harness.namespace
    .get(harness.namespace.idFromName(name))
    .fetch(new Request(`https://object.test${path}`, init));

const probe = async (harness: Harness, name: string) =>
  // SAFETY: The fixture's `/__probe` branch always returns this fixed JSON shape.
  (await direct(harness, name, "/__probe")).json() as unknown as ProbeResult;

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

test("byKey co-locates same-family forks and returns 404 for cross-family forks", async () => {
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
  expect(cross.status).toBe(404);
  expect(await cross.text()).toBe("Source stream not found: t1/x");
  expect((await dispatch(harness, "/streams/t2/z")).status).toBe(404);
  expect(await harness.miniflare.listDurableObjectIds("STREAMS")).toHaveLength(2);
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
  for (let index = 0; index < 20; index += 1) {
    const response = await dispatch(
      harness,
      "/streams/reuse",
      index % 3 === 0 ? { method: "HEAD" } : index % 3 === 1 ? undefined : { method: "OPTIONS" },
    );
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

test("a recreated Miniflare instance receives the persisted alarm", async () => {
  const first = await makeHarness();
  expect((await create(first, "/streams/recreated", 1)).status).toBe(201);
  const root = first.root;
  await disposeHarness(first, true);
  const second = await makeHarness("worker.ts", root);
  await waitUntil(async () => (await probe(second, "recreated")).rows.length === 0, 5_000);
  const observation = await probe(second, "recreated");
  expect(observation.alarm).toBeNull();
  expect(observation.layerAcquisitions).toBe(1);
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
    harness.miniflare,
    new Request(`https://streams.test/streams/cancel?offset=${tail}&live=long-poll`, {
      signal: longPollAbort.signal,
    }),
  ).catch(() => undefined);
  await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 1);
  longPollAbort.abort();
  await longPoll;
  await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 0, 3_000);
});

const workerdCancellationTest = test.skipIf(Bun.env.STREAMSY_WORKERD_CANCELLATION !== "1");

workerdCancellationTest(
  "opt-in workerd cancellation propagation interrupts long-poll and SSE reads",
  async () => {
    const harness = await makeHarness();
    await direct(harness, "cancel", "/__probe?long-poll-timeout=25000");
    expect((await create(harness, "/streams/cancel")).status).toBe(201);
    const tail = (await dispatch(harness, "/streams/cancel", { method: "HEAD" })).headers.get(
      "stream-next-offset",
    );
    if (tail === null) throw new Error("missing stream tail");
    const failures: Array<string> = [];

    const longPollAbort = new AbortController();
    const longPoll = dispatchFetch(
      harness.miniflare,
      new Request(`https://streams.test/streams/cancel?offset=${tail}&live=long-poll`, {
        signal: longPollAbort.signal,
      }),
    ).catch(() => undefined);
    await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 1);
    longPollAbort.abort();
    await longPoll;
    try {
      await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 0, 2_000);
    } catch {
      failures.push("long-poll active read remained after abort");
    }

    const sseAbort = new AbortController();
    const sseResponse = await dispatchFetch(
      harness.miniflare,
      new Request(`https://streams.test/streams/cancel?offset=${tail}&live=sse`, {
        signal: sseAbort.signal,
      }),
    );
    const reader = sseResponse.body?.getReader();
    if (reader === undefined) throw new Error("SSE response has no body");
    await reader.read();
    await waitUntil(
      async () => (await probe(harness, "cancel")).activeReads >= (failures.length === 0 ? 1 : 2),
    );
    sseAbort.abort();
    try {
      await reader.cancel();
    } catch {
      // The boundary may reject the client-side body cancellation.
    }
    try {
      await waitUntil(async () => (await probe(harness, "cancel")).activeReads === 0, 2_000);
    } catch {
      failures.push("SSE active read remained after abort");
    }
    expect(failures).toEqual([]);
  },
);

const sseBoundTest = test.skipIf(Bun.env.STREAMSY_SSE_BOUND !== "1");

sseBoundTest(
  "opt-in SSE proof releases at core's 60 second bound",
  async () => {
    const harness = await makeHarness();
    expect((await create(harness, "/streams/sse-bound")).status).toBe(201);
    const tail = (await dispatch(harness, "/streams/sse-bound", { method: "HEAD" })).headers.get(
      "stream-next-offset",
    );
    if (tail === null) throw new Error("missing stream tail");
    const response = await dispatch(harness, `/streams/sse-bound?offset=${tail}&live=sse`);
    const reader = response.body?.getReader();
    if (reader === undefined) throw new Error("SSE response has no body");
    await reader.read();
    await waitUntil(async () => (await probe(harness, "sse-bound")).activeReads === 0, 65_000);
  },
  { timeout: 70_000 },
);

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
