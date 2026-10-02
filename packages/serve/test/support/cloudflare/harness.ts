/* oxlint-disable effecttsgo/async-function, effecttsgo/node-builtin-import, effecttsgo/crypto-random-uuid, anti-slop/no-chained-type-assertions -- This harness owns the real local workerd boundary for the Bun tests; Miniflare exposes a workers-types Fetch overload while the Bun tests use Bun Fetch values. */
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

/**
 * The Miniflare harness the Cloudflare host and lifecycle suites share. Each
 * harness builds one fixture worker from this folder, runs it in a local
 * workerd with persisted Durable Object state under an owned temporary root,
 * and is disposed by `cleanupHarnesses` after each test. Set
 * `STREAMSY_STORAGE_SCRATCH` to an absolute path to keep a copy of each root.
 */

const retentionRoot = Bun.env.STREAMSY_STORAGE_SCRATCH;

export interface Harness {
  readonly bundleRoot: string;
  readonly miniflare: Miniflare;
  readonly ownership: OwnedHarness;
  readonly root: string;
  readonly namespace: TestNamespace;
}

interface OwnedHarness {
  readonly bundleRoot: string;
  readonly miniflare: Miniflare;
  readonly root: string;
  disposed: boolean;
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

/** The fixed JSON shape of the fixture's `/__probe` branch. */
export interface ProbeResult {
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
const ownedRoots = new Set<string>();
const ownedBundles = new Set<string>();
const closers: Array<() => Promise<void>> = [];

/** Stop another test resource, such as a Bun host, before the harnesses at the end of the test. */
export const closeAfterTest = (close: () => Promise<void>): void => {
  closers.push(close);
};

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
  rejectSymlinkPath(root);
  const sourceReal = realpathSync(root);
  const retention = resolve(retentionRoot);
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

const removePath = (path: string): void => rmSync(path, { recursive: true, force: true });

/** Copy an owned root to the retention folder when one is set, then remove it. */
const reclaimOwnedRoot = (root: string, errors: Array<unknown>): void => {
  try {
    const destination = retentionDestination(root);
    if (destination !== undefined) {
      mkdirSync(resolve(destination, ".."), { recursive: true });
      cpSync(root, destination, { recursive: true });
    }
  } catch (error) {
    errors.push(error);
  }
  try {
    removePath(root);
    ownedRoots.delete(root);
  } catch (error) {
    errors.push(error);
  }
};

const buildWorker = async (entry: string, outdir: string): Promise<string> => {
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, entry)],
    outdir,
    target: "browser",
    format: "esm",
    external: ["cloudflare:workers"],
  });
  if (!built.success) throw new Error("Cloudflare fixture worker build failed");
  const output = built.outputs[0];
  if (output === undefined) throw new Error("Cloudflare fixture worker produced no bundle");
  return output.path;
};

/**
 * Start one fixture worker. Pass the `root` of a disposed harness to start a new
 * Miniflare instance over the same persisted Durable Object state.
 */
export const makeHarness = async (entry = "worker.ts", root?: string): Promise<Harness> => {
  const ownedRoot = root ?? mkdtempSync(join(tmpdir(), ".streamsy-cloudflare-workerd-"));
  ownedRoots.add(ownedRoot);
  let bundleRoot: string | undefined;
  let ownership: OwnedHarness | undefined;
  try {
    bundleRoot = mkdtempSync(".streamsy-cloudflare-workerd-bundle-");
    ownedBundles.add(bundleRoot);
    const scriptPath = await buildWorker(entry, join(bundleRoot, `bundle-${crypto.randomUUID()}`));
    const miniflare = new Miniflare({
      scriptPath,
      modules: true,
      compatibilityDate: "2026-08-06",
      host: "127.0.0.1",
      port: 0,
      cf: false,
      durableObjects: { STREAMS: { className: "ProbeObject", useSQLite: true } },
      durableObjectsPersist: join(ownedRoot, "state"),
    });
    ownership = { bundleRoot, miniflare, root: ownedRoot, disposed: false };
    pending.add(ownership);
    await miniflare.ready;
    // SAFETY: the local fixture exposes only idFromName/get/fetch to this harness.
    const namespace = (await miniflare.getDurableObjectNamespace(
      "STREAMS",
    )) as unknown as TestNamespace;
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
        removePath(bundleRoot);
        ownedBundles.delete(bundleRoot);
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (ownership?.disposed === true && root === undefined) {
      try {
        removePath(ownedRoot);
        ownedRoots.delete(ownedRoot);
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
    }
    if (ownership?.disposed === true && cleanupErrors.length === 0) pending.delete(ownership);
    if (cleanupErrors.length > 0 || ownership?.disposed !== true) {
      // oxlint-disable-next-line preserve-caught-error -- AggregateError retains the primary acquisition error and every cleanup error.
      throw new AggregateError([error, ...cleanupErrors], "Cloudflare harness acquisition failed", {
        cause: error,
      });
    }
    throw error;
  }
};

type CleanupHarness = Pick<Harness, "bundleRoot" | "miniflare" | "ownership" | "root">;

/** Dispose one harness. With `retainRoot`, keep its root for a later `makeHarness`. */
export const disposeHarness = async (
  harness: CleanupHarness,
  retainRoot = false,
): Promise<void> => {
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
      removePath(harness.bundleRoot);
      ownedBundles.delete(harness.bundleRoot);
    } catch (error) {
      errors.push(error);
    }
  }
  if (harness.ownership.disposed && !retainRoot && ownedRoots.has(harness.root))
    reclaimOwnedRoot(harness.root, errors);
  if (
    harness.ownership.disposed &&
    !ownedBundles.has(harness.bundleRoot) &&
    (retainRoot || !ownedRoots.has(harness.root))
  )
    pending.delete(harness.ownership);
  if (errors.length > 0 || !harness.ownership.disposed)
    throw new AggregateError(
      errors.length > 0 ? errors : [new Error("Miniflare disposal remains unresolved")],
      "Cloudflare harness cleanup failed",
    );
};

/** Stop every resource a test opened. Register it with `afterEach`. */
export const cleanupHarnesses = async (): Promise<void> => {
  const errors: Array<unknown> = [];
  for (let attempt = 0; attempt < 2; attempt += 1) {
    for (const close of closers.splice(0)) {
      try {
        await close();
      } catch (error) {
        closers.push(close);
        errors.push(error);
      }
    }
  }
  for (let attempt = 0; attempt < 2; attempt += 1) {
    for (const harness of open.slice()) {
      try {
        await disposeHarness(harness);
      } catch (error) {
        errors.push(error);
      }
    }
  }
  for (let attempt = 0; attempt < 2; attempt += 1) {
    for (const ownership of Array.from(pending)) {
      if (open.some((harness) => harness.ownership === ownership)) continue;
      try {
        await disposeHarness({
          bundleRoot: ownership.bundleRoot,
          miniflare: ownership.miniflare,
          ownership,
          root: ownership.root,
        });
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
      removePath(bundleRoot);
      ownedBundles.delete(bundleRoot);
    } catch (error) {
      errors.push(error);
    }
  }
  for (const root of ownedRoots) {
    if (pendingRoots.has(root)) continue;
    reclaimOwnedRoot(root, errors);
  }
  if (errors.length > 0) throw new AggregateError(errors, "Cloudflare afterEach cleanup failed");
};

/** Dispatch through the worker's fetch handler. */
export const dispatchFetch = (
  harness: Harness,
  input: string | Request,
  init?: RequestInit,
): Promise<Response> =>
  // SAFETY: Miniflare dispatches the same local workerd boundary, while its
  // declaration uses the workers-types Request overload instead of Bun's.
  (
    harness.miniflare.dispatchFetch as unknown as (
      input: string | Request,
      init?: RequestInit,
    ) => Promise<Response>
  )(input, init);

export const dispatch = (harness: Harness, path: string, init?: RequestInit) =>
  dispatchFetch(harness, `https://streams.test${path}`, init);

/** Fetch one Durable Object directly through its stub, bypassing the worker router. */
export const direct = (harness: Harness, name: string, path: string, init?: RequestInit) =>
  harness.namespace
    .get(harness.namespace.idFromName(name))
    .fetch(new Request(`https://object.test${path}`, init));

export const probe = async (harness: Harness, name: string) =>
  // SAFETY: the fixture's `/__probe` branch always returns this fixed JSON shape.
  (await direct(harness, name, "/__probe")).json() as unknown as ProbeResult;

export const waitUntil = async (predicate: () => Promise<boolean>, timeoutMs = 6_000) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(50);
  }
  throw new Error(`Timed out after ${timeoutMs} ms`);
};
